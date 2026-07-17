// Command streamledger runs the StreamLedger gRPC command/query API: an
// event-sourced, CQRS-projected ledger service for balances and inventory.
//
// By default it runs entirely in-process (in-memory event log, in-memory
// read models, in-memory bus) so `go run ./cmd/streamledger` works with no
// external dependencies whatsoever — useful for local exploration and for
// this repository's demo/CI footprint. Point it at real infrastructure with
// -storage=postgres -database-url=... and -bus=nats -nats-url=... for a
// production deployment.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"

	ledgerv1 "streamledger/gen/ledger/v1"
	"streamledger/internal/bus"
	"streamledger/internal/bus/inmemory"
	"streamledger/internal/bus/natsbus"
	"streamledger/internal/eventstore"
	memorystore "streamledger/internal/eventstore/memory"
	pgeventstore "streamledger/internal/eventstore/postgres"
	"streamledger/internal/grpcserver"
	"streamledger/internal/ledger"
	"streamledger/internal/metrics"
	"streamledger/internal/projections"
	memoryreadmodels "streamledger/internal/projections/memory"
	pgprojections "streamledger/internal/projections/postgres"
)

const (
	balancesProjectionName  = "balances"
	inventoryProjectionName = "inventory"
	catchUpBatchSize        = 500
	catchUpPollInterval     = 2 * time.Second
)

func main() {
	if err := run(); err != nil {
		log.Fatalf("streamledger: %v", err)
	}
}

func run() error {
	var (
		grpcAddr    = flag.String("addr", envOr("STREAMLEDGER_ADDR", ":9090"), "gRPC listen address")
		metricsAddr = flag.String("metrics-addr", envOr("STREAMLEDGER_METRICS_ADDR", ":9091"), "Prometheus /metrics listen address")
		storageKind = flag.String("storage", envOr("STREAMLEDGER_STORAGE", "memory"), "event log + read model backend: memory|postgres")
		databaseURL = flag.String("database-url", os.Getenv("STREAMLEDGER_DATABASE_URL"), "Postgres connection string (required when -storage=postgres)")
		busKind     = flag.String("bus", envOr("STREAMLEDGER_BUS", "memory"), "event bus backend: memory|nats")
		natsURL     = flag.String("nats-url", envOr("STREAMLEDGER_NATS_URL", "nats://127.0.0.1:4222"), "NATS server URL (used when -bus=nats)")
	)
	flag.Parse()

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	reg := prometheus.NewRegistry()
	reg.MustRegister(prometheus.NewGoCollector(), prometheus.NewProcessCollector(prometheus.ProcessCollectorOpts{}))
	m := metrics.New()
	if err := m.Register(reg); err != nil {
		return fmt.Errorf("register metrics: %w", err)
	}

	store, readModels, cleanupStorage, err := buildStorage(ctx, *storageKind, *databaseURL)
	if err != nil {
		return fmt.Errorf("build storage: %w", err)
	}
	defer cleanupStorage()

	eventBus, cleanupBus, err := buildBus(ctx, *busKind, *natsURL)
	if err != nil {
		return fmt.Errorf("build bus: %w", err)
	}
	defer cleanupBus()

	balances := projections.NewProjector(balancesProjectionName, readModels, &projections.AccountApplier{Store: readModels}, metrics.ProjectionMetrics{M: m})
	inventory := projections.NewProjector(inventoryProjectionName, readModels, &projections.InventoryApplier{Store: readModels}, metrics.ProjectionMetrics{M: m})

	projectorCtx, stopProjectors := context.WithCancel(ctx)
	defer stopProjectors()
	if err := subscribeAndCatchUp(projectorCtx, eventBus, store, balances); err != nil {
		return fmt.Errorf("start balances projector: %w", err)
	}
	if err := subscribeAndCatchUp(projectorCtx, eventBus, store, inventory); err != nil {
		return fmt.Errorf("start inventory projector: %w", err)
	}

	svc := ledger.NewService(store, publisherAdapter{bus: eventBus})

	grpcServer := grpc.NewServer()
	ledgerv1.RegisterLedgerServiceServer(grpcServer, grpcserver.New(svc, store, balances, inventory, readModels))
	reflection.Register(grpcServer)

	lis, err := net.Listen("tcp", *grpcAddr)
	if err != nil {
		return fmt.Errorf("listen %s: %w", *grpcAddr, err)
	}

	errCh := make(chan error, 2)
	go func() {
		log.Printf("streamledger: gRPC API listening on %s (storage=%s bus=%s)", *grpcAddr, *storageKind, *busKind)
		if err := grpcServer.Serve(lis); err != nil {
			errCh <- fmt.Errorf("grpc serve: %w", err)
		}
	}()

	metricsServer := &http.Server{
		Addr:              *metricsAddr,
		Handler:           promhttp.HandlerFor(reg, promhttp.HandlerOpts{}),
		ReadHeaderTimeout: 5 * time.Second,
	}
	go func() {
		log.Printf("streamledger: metrics listening on %s/metrics", *metricsAddr)
		if err := metricsServer.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			errCh <- fmt.Errorf("metrics serve: %w", err)
		}
	}()

	select {
	case <-ctx.Done():
		log.Println("streamledger: shutdown signal received")
	case err := <-errCh:
		return err
	}

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	grpcServer.GracefulStop()
	_ = metricsServer.Shutdown(shutdownCtx)
	return nil
}

// buildStorage constructs the eventstore.Store and projections.ReadModelStore
// pair for kind, along with a cleanup func to release their resources.
func buildStorage(ctx context.Context, kind, databaseURL string) (eventstore.Store, projections.ReadModelStore, func(), error) {
	switch kind {
	case "memory":
		return memorystore.New(), memoryreadmodels.New(), func() {}, nil
	case "postgres":
		if databaseURL == "" {
			return nil, nil, nil, errors.New("-database-url is required when -storage=postgres")
		}
		es, err := pgeventstore.Connect(ctx, databaseURL)
		if err != nil {
			return nil, nil, nil, err
		}
		if err := es.Migrate(ctx); err != nil {
			es.Close()
			return nil, nil, nil, fmt.Errorf("migrate event store: %w", err)
		}
		rm, err := pgprojections.Connect(ctx, databaseURL)
		if err != nil {
			es.Close()
			return nil, nil, nil, err
		}
		if err := rm.Migrate(ctx); err != nil {
			es.Close()
			rm.Close()
			return nil, nil, nil, fmt.Errorf("migrate read models: %w", err)
		}
		return es, rm, func() { es.Close(); rm.Close() }, nil
	default:
		return nil, nil, nil, fmt.Errorf("unknown -storage %q (want memory|postgres)", kind)
	}
}

// buildBus constructs the bus.Bus for kind, along with a cleanup func.
func buildBus(ctx context.Context, kind, natsURL string) (bus.Bus, func(), error) {
	switch kind {
	case "memory":
		b := inmemory.New()
		return b, func() { _ = b.Close() }, nil
	case "nats":
		b, err := natsbus.Connect(ctx, natsURL, natsbus.Options{})
		if err != nil {
			return nil, nil, err
		}
		return b, func() { _ = b.Close() }, nil
	default:
		return nil, nil, fmt.Errorf("unknown -bus %q (want memory|nats)", kind)
	}
}

// subscribeAndCatchUp wires p to receive live events from b's fast path and
// runs an initial CatchUp so it starts consistent with the log even if it
// missed everything published before Subscribe registered. A background
// goroutine then polls CatchUp on catchUpPollInterval for the lifetime of
// ctx: the durability backstop described in projections.Projector.CatchUp's
// doc comment, so a lost bus message never permanently strands the read
// model behind the log.
func subscribeAndCatchUp(ctx context.Context, b bus.Bus, store eventstore.Store, p *projections.Projector) error {
	if _, err := b.Subscribe(ctx, func(ctx context.Context, msg bus.Message) error {
		_, err := p.ApplyEvent(ctx, msg.Event)
		return err
	}); err != nil {
		return err
	}
	if _, err := p.CatchUp(ctx, store, catchUpBatchSize); err != nil {
		return fmt.Errorf("initial catch-up for %s: %w", p.Name, err)
	}
	go func() {
		ticker := time.NewTicker(catchUpPollInterval)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				if _, err := p.CatchUp(ctx, store, catchUpBatchSize); err != nil {
					log.Printf("streamledger: %s catch-up poll: %v", p.Name, err)
				}
			}
		}
	}()
	return nil
}

// publisherAdapter adapts bus.Publisher to ledger.EventPublisher: a
// best-effort, fire-and-forget notification that new events exist, logged
// (not fatal) on failure because the event log itself is already the
// durable source of truth and every projector's CatchUp poll is the
// backstop if this notification is ever lost.
type publisherAdapter struct {
	bus bus.Publisher
}

func (p publisherAdapter) Publish(ctx context.Context, events []eventstore.Event) {
	if err := p.bus.Publish(ctx, events); err != nil {
		log.Printf("streamledger: best-effort bus publish failed (projections will catch up via polling): %v", err)
	}
}

func envOr(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
