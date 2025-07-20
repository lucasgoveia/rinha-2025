package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/amirsalarsafaei/sqlc-pgx-monitoring/dbtracer"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/redis/go-redis/extra/redisotel/v9"
	"github.com/redis/go-redis/v9"
	"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"
	"log"
	"log/slog"
	"net/http"
	"os"
	"rinha/config"
	"rinha/internal/payments"
	"rinha/internal/payments/workers"
	"time"
)

func main() {
	appConfig, err := config.LoadConfig()
	if err != nil {
		log.Fatal(err)
	}

	if appConfig.Telemetry.Enabled {
		cleanup := config.InitTracer(appConfig.Telemetry)
		defer cleanup()
	}

	dbpool := setupDbPool(appConfig)
	defer dbpool.Close()

	logger := setupLogger(appConfig)
	httpClient := setupHttpClient(appConfig)
	redisClient := setupRedisClient(appConfig)

	defaultService := payments.NewPaymentService(httpClient, 0.05, appConfig.Service.DefaultURL, payments.ServiceTypeDefault, dbpool)
	fallbackService := payments.NewPaymentService(httpClient, 0.15, appConfig.Service.FallbackURL, payments.ServiceTypeFallback, dbpool)

	serviceMonitor := workers.NewServiceMonitor(
		appConfig.Service.DefaultURL,
		appConfig.Service.FallbackURL,
		httpClient,
		logger,
	)

	go serviceMonitor.StartMonitoring()

	batcher := workers.NewDbBatcher(dbpool, logger)

	go batcher.Run()

	worker := workers.NewPaymentWorker(defaultService, fallbackService, logger, serviceMonitor, batcher, redisClient)
	totalMessages := 0

	for {
		streams, err := redisClient.XReadGroup(context.Background(), &redis.XReadGroupArgs{
			Group:    appConfig.Redis.StreamGroup,
			Consumer: appConfig.Redis.ConsumerName,
			Streams:  []string{appConfig.Redis.StreamName, ">"},
			Block:    5 * time.Millisecond,
			Count:    200,
		}).Result()

		if err != nil && !errors.Is(err, redis.Nil) {
			logger.Error("[%s] error: %v", appConfig.Redis.ConsumerName, err)
			continue
		}

		for _, stream := range streams {
			batchLen := len(stream.Messages)
			if batchLen == 0 {
				continue
			}

			totalMessages += batchLen
			reqs := make([]payments.PaymentMessage, batchLen)

			for i, msg := range stream.Messages {
				raw := msg.Values["data"].(string)

				var payload payments.PaymentMessage
				if err := json.Unmarshal([]byte(raw), &payload); err != nil {
					logger.Error("Invalid JSON: %v", err)
					return
				}
				reqs[i] = payload
				redisClient.XAck(context.Background(), appConfig.Redis.StreamName, appConfig.Redis.StreamGroup, msg.ID)
			}

			logger.Debug("Processing messages", "consumer", appConfig.Redis.ConsumerName, "batchSize", len(stream.Messages), "total", totalMessages)

			worker.Process(reqs)
		}
	}
}

func setupLogger(appConfig *config.AppConfig) *slog.Logger {
	logLevel := slog.LevelInfo
	if appConfig.Telemetry.Enabled {
		logLevel = slog.LevelDebug
	}
	handler := slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{
		Level: logLevel,
	})

	return slog.New(handler)
}

func setupHttpClient(appConfig *config.AppConfig) *http.Client {
	transport := http.DefaultTransport
	if appConfig.Telemetry.Enabled {
		transport = otelhttp.NewTransport(http.DefaultTransport)
	}
	return &http.Client{
		Transport: transport,
		Timeout:   500 * time.Millisecond,
	}
}

func setupDbPool(appConfig *config.AppConfig) *pgxpool.Pool {
	dbConfig, err := pgxpool.ParseConfig(appConfig.Postgres.URL)

	if appConfig.Telemetry.Enabled {
		dbTracer, _ := dbtracer.NewDBTracer("payments")
		dbConfig.ConnConfig.Tracer = dbTracer
	}

	dbpool, err := pgxpool.NewWithConfig(context.Background(), dbConfig)
	if err != nil {
		_, _ = fmt.Fprintf(os.Stderr, "Unable to connect to database: %v\n", err)
		os.Exit(1)
	}
	return dbpool
}

func setupRedisClient(appConfig *config.AppConfig) *redis.Client {
	opt, err := redis.ParseURL(appConfig.Redis.URL)
	if err != nil {
		log.Fatalf("Failed to parse Redis URL: %v", err)
	}

	redisClient := redis.NewClient(opt)

	if appConfig.Telemetry.Enabled {
		if err := redisotel.InstrumentTracing(redisClient); err != nil {
			panic(err)
		}

		if err := redisotel.InstrumentMetrics(redisClient); err != nil {
			panic(err)
		}
	}

	return redisClient
}
