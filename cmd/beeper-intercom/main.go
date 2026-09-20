// Command beeper-intercom runs the bridge: one process, one appservice
// registration, no published port.
//
// Everything it needs is a config file and an environment holding the Beeper
// account token plus the two API tokens. It registers itself with Beeper on
// first start, so the only manual step is starting it.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/rs/zerolog"

	"github.com/SelfRef/beeper-intercom/internal/api"
	"github.com/SelfRef/beeper-intercom/internal/config"
	"github.com/SelfRef/beeper-intercom/internal/mcpserver"
	"github.com/SelfRef/beeper-intercom/internal/notify"
	"github.com/SelfRef/beeper-intercom/internal/service"
	"github.com/SelfRef/beeper-intercom/internal/source"
	"github.com/SelfRef/beeper-intercom/internal/store"
)

// version is overridden at build time with -ldflags "-X main.version=…".
var version = "dev"

func main() {
	configPath := flag.String("config", envOr("INTERCOM_CONFIG", "/config/config.yaml"), "path to config.yaml")
	dbPath := flag.String("db", envOr("INTERCOM_DB", "/data/intercom.db"), "path to the SQLite database")
	checkOnly := flag.Bool("check", false, "validate the config and exit")
	healthCheck := flag.Bool("health", false, "probe the local HTTP API and exit (for container healthchecks)")
	showVersion := flag.Bool("version", false, "print the version and exit")
	flag.Parse()

	if *showVersion {
		fmt.Println(version)
		return
	}
	if *healthCheck {
		// The image has no shell and no curl; the binary probes itself so the
		// container healthcheck does not need either.
		os.Exit(probeHealth(*configPath))
	}

	cfg, err := config.Load(*configPath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "config: %v\n", err)
		os.Exit(1)
	}
	if *checkOnly {
		fmt.Printf("%s: ok (%d rooms, %d ghosts, %d agents)\n",
			*configPath, len(cfg.Rooms), len(cfg.Ghosts), len(cfg.Agents))
		return
	}

	log := newLogger(cfg)
	log.Info().Str("version", version).Str("network", cfg.Network.Name).
		Str("bridge", cfg.Network.Bridge).Msg("Starting beeper-intercom")

	if cfg.IngestToken() == "" {
		log.Warn().Str("env", cfg.Server.IngestTokenEnv).
			Msg("No ingest token: the notify API is unauthenticated")
	}
	if cfg.AdminToken() == "" {
		log.Warn().Str("env", cfg.Server.AdminTokenEnv).
			Msg("No admin token: the admin API and MCP endpoint are unauthenticated")
	}

	st, err := store.Open(*dbPath)
	if err != nil {
		log.Fatal().Err(err).Str("path", *dbPath).Msg("Cannot open the database")
	}
	defer st.Close()

	svc, err := service.New(cfg, *configPath, st, log)
	if err != nil {
		log.Fatal().Err(err).Msg("Cannot build the service")
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	if err := svc.Start(ctx); err != nil {
		log.Fatal().Err(err).Msg("Cannot start the bridge")
	}

	startSources(ctx, cfg, st, svc, log)
	go reloadOnHUP(ctx, svc, log)

	server := &http.Server{
		Addr: cfg.Server.Bind,
		Handler: api.New(svc, log, map[string]http.Handler{
			"/mcp":  mcpserver.Handler(svc, version),
			"/mcp/": mcpserver.Handler(svc, version),
		}).Handler(),
		ReadHeaderTimeout: 10 * time.Second,
	}
	go func() {
		log.Info().Str("bind", cfg.Server.Bind).Msg("HTTP API listening")
		if err := server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Fatal().Err(err).Msg("HTTP server failed")
		}
	}()

	<-ctx.Done()
	log.Info().Msg("Shutting down")
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	_ = server.Shutdown(shutdownCtx)
}

// startSources launches every configured inbound adapter. The native notify
// API is always on and needs nothing started.
func startSources(ctx context.Context, cfg *config.Config, st *store.Store, svc *service.Service, log zerolog.Logger) {
	emit := func(ctx context.Context, n *notify.Notification) error {
		_, _, err := svc.Notify(ctx, n)
		return err
	}
	// Held back until the rooms exist: a mirrored message with nowhere to go
	// would advance the source's cursor and be lost.
	go func() {
		if err := svc.WaitReady(ctx); err != nil {
			return
		}
		for _, src := range cfg.Sources {
			switch src.Type {
			case config.SourceNtfy:
				source.NewNtfy(src, st, log, emit).Start(ctx)
			case config.SourceNotify:
				// The HTTP API is the adapter; nothing to start.
			}
		}
	}()
}

// reloadOnHUP is the other half of POST /v1/reload: rooms reconcile, ghosts
// refresh, sessions survive.
func reloadOnHUP(ctx context.Context, svc *service.Service, log zerolog.Logger) {
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, syscall.SIGHUP)
	for {
		select {
		case <-ctx.Done():
			return
		case <-signals:
			if err := svc.Reload(ctx); err != nil {
				log.Error().Err(err).Msg("Reload failed; keeping the previous configuration")
			}
		}
	}
}

func newLogger(cfg *config.Config) zerolog.Logger {
	level, err := zerolog.ParseLevel(cfg.Log.Level)
	if err != nil {
		level = zerolog.InfoLevel
	}
	var writer = os.Stderr
	logger := zerolog.New(writer).Level(level).With().Timestamp().Logger()
	if cfg.Log.Format == "console" {
		logger = logger.Output(zerolog.ConsoleWriter{Out: writer, TimeFormat: time.RFC3339})
	}
	return logger
}

// probeHealth is the container healthcheck: it asks the running process, on
// the address the config says it listens on, whether it is up.
func probeHealth(configPath string) int {
	bind := "127.0.0.1:8080"
	if cfg, err := config.Load(configPath); err == nil && cfg.Server.Bind != "" {
		bind = cfg.Server.Bind
		if strings.HasPrefix(bind, "0.0.0.0:") || strings.HasPrefix(bind, ":") {
			_, port, _ := strings.Cut(bind, ":")
			bind = "127.0.0.1:" + port
		}
	}
	client := &http.Client{Timeout: 5 * time.Second}
	resp, err := client.Get("http://" + bind + "/health")
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		return 1
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		fmt.Fprintf(os.Stderr, "health: HTTP %d\n", resp.StatusCode)
		return 1
	}
	return 0
}

func envOr(key, fallback string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return fallback
}
