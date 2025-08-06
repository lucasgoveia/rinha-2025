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
	"rinha/internal/payments/handlers"
	"time"
)

func main() {
	appConfig, err := config.LoadConfig()
	if err != nil {
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
		Timeout:   10 * time.Second,
	}

	logger := setupLogger()

	dbpool := setupDbPool(appConfig)
	defer dbpool.Close()

	// Create the payment store
	pStore := payments.NewPaymentStore(dbpool, logger)

	socket := "/tmp/payments-stream.sock"
	publisher, err := messages.NewPublisher(socket, 4) // até 4 conexões mantidas
	if err != nil {
		log.Fatal(err)
	}

	mux := http.NewServeMux()
	paymentHandler := handlers.NewPaymentHandler(publisher)
	summaryHandler := handlers.NewSummaryHandler(pStore, httpClient)
	purgeHandler := handlers.NewPurgeHandler(pStore)

	mux.Handle("/payments", paymentHandler)
	mux.Handle("/payments-summary", summaryHandler)
	mux.Handle("/purge-payments", purgeHandler)
	mux.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	})

	_ = os.Remove(appConfig.Server.Socket)
	
	l, err := net.Listen("unix", appConfig.Server.Socket)
	if err != nil {
		log.Fatal(err)
	}

	if err := os.Chmod(appConfig.Server.Socket, 0o666); err != nil {
		log.Fatal(err)
	}

	err = http.Serve(l, mux)
	if err != nil {
		log.Fatal(err)
	}
	//err = http.ListenAndServe(":7118", mux)
	//if err != nil {
	//	log.Fatal(err)
	//}
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
