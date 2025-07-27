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
	"rinha/internal/payments"
	"rinha/internal/payments/handlers"
	"rinha/internal/payments/workers"
	"time"
)

func main() {
	appConfig, err := config.LoadConfig()
	if err != nil {
		log.Fatal(err)
	}

	transport := &http.Transport{
		DialContext: (&net.Dialer{
			Timeout:   30 * time.Millisecond,
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

	logger := setupLogger()

	healthMonitor := workers.NewServiceMonitor(appConfig.Service.DefaultHealthURL, appConfig.Service.FallbackHealthURL, httpClient, logger)
	go healthMonitor.StartMonitoring()
	defer healthMonitor.Stop()

	defaultProcessor := payments.NewPaymentProcessor(httpClient, appConfig.Service.DefaultURL, payments.ProcessorTypeDefault)
	fallbackProcessor := payments.NewPaymentProcessor(httpClient, appConfig.Service.FallbackURL, payments.ProcessorTypeFallback)

	dbpool := setupDbPool(appConfig)
	defer dbpool.Close()

	// Create the payment store
	pStore := payments.NewPaymentStore(dbpool, logger)

	// Create the worker pool with the store
	workerPool := workers.NewWorkerPool(defaultProcessor, fallbackProcessor, pStore, healthMonitor, logger)
	go workerPool.Start()
	defer workerPool.Stop()

	mux := http.NewServeMux()
	paymentHandler := handlers.NewPaymentHandler(workerPool)
	summaryHandler := handlers.NewSummaryHandler(pStore, httpClient)
	purgeHandler := handlers.NewPurgeHandler(pStore)

	mux.Handle("/payments", paymentHandler)
	mux.Handle("/payments-summary", summaryHandler)
	mux.Handle("/purge-payments", purgeHandler)
	mux.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	})

	logger.Info("starting server on port 7118, listening for requests")
	err = http.ListenAndServe(":7118", mux)
	if err != nil {
		log.Fatal(err)
	}
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
