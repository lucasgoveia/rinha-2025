package main

import (
	"context"
	"fmt"
	"github.com/jackc/pgx/v5/pgxpool"
	"log"
	"log/slog"
	"net"
	"net/http"
	"os"
	"rinha/config"
	"rinha/internal/messages"
	"rinha/internal/payments"
	"rinha/internal/payments/workers"
	"time"
)

func main() {
	logger := setupLogger()

	appConfig, err := config.LoadConfig()
	if err != nil {
		logger.Error("Failed to load config", "err", err)
		log.Fatal(err)
	}

	transport := &http.Transport{
		DialContext: (&net.Dialer{
			Timeout:   100 * time.Millisecond,
			KeepAlive: 30 * time.Second,
		}).DialContext,

		MaxIdleConns:        256,
		MaxIdleConnsPerHost: 256,
		MaxConnsPerHost:     256,

		IdleConnTimeout:    90 * time.Second,
		DisableCompression: true,
		ForceAttemptHTTP2:  false,
	}

	httpClient := &http.Client{
		Transport: transport,
		Timeout:   180 * time.Millisecond,
	}

	defaultProcessor := payments.NewPaymentProcessor(httpClient, appConfig.Service.DefaultURL, payments.ProcessorTypeDefault, logger)
	fallbackProcessor := payments.NewPaymentProcessor(httpClient, appConfig.Service.FallbackURL, payments.ProcessorTypeFallback, logger)

	dbpool := setupDbPool(appConfig)
	defer dbpool.Close()

	pStore := payments.NewPaymentStore(dbpool, logger)

	healthMonitor := workers.NewServiceMonitor(appConfig.Service.DefaultHealthURL, appConfig.Service.FallbackHealthURL, httpClient, logger)
	go healthMonitor.StartMonitoring()

	workerPool := workers.NewWorkerPool(defaultProcessor, fallbackProcessor, pStore, logger, healthMonitor)
	go workerPool.Start()
	defer workerPool.Stop()

	socket := "/tmp/payments-stream.sock"

	// --- start consumer (único) ---
	receiver := messages.NewReceiver(socket, workerPool, logger)

	err = receiver.Start()
	if err != nil {
		logger.Error("Failed to start receiver", "err", err)
		os.Exit(1)
		return
	}

	defer receiver.Stop()
}

func setupDbPool(appConfig *config.AppConfig) *pgxpool.Pool {
	dbConfig, err := pgxpool.ParseConfig(appConfig.Postgres.URL)

	dbpool, err := pgxpool.NewWithConfig(context.Background(), dbConfig)
	if err != nil {
		_, _ = fmt.Fprintf(os.Stderr, "Unable to connect to database: %v\n", err)
		os.Exit(1)
	}
	return dbpool
}

func setupLogger() *slog.Logger {
	logLevel := slog.LevelWarn

	handler := slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{
		Level: logLevel,
	})

	return slog.New(handler)
}
