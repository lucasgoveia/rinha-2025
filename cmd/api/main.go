package main

import (
	"context"
	"fmt"
	"github.com/amirsalarsafaei/sqlc-pgx-monitoring/dbtracer"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/labstack/echo/v4"
	"github.com/labstack/echo/v4/middleware"
	"github.com/redis/go-redis/extra/redisotel/v9"
	"github.com/redis/go-redis/v9"
	"go.opentelemetry.io/contrib/instrumentation/github.com/labstack/echo/otelecho"
	"log"
	"os"
	"rinha/config"
	"rinha/internal/payments/handlers"
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

	e := echo.New()

	if appConfig.Telemetry.Enabled {
		e.Use(otelecho.Middleware(appConfig.Telemetry.ServiceName))
	}

	dbpool := setupDbPool(appConfig)
	defer dbpool.Close()

	redisClient := setupRedisClient(appConfig)

	err = redisClient.XGroupCreateMkStream(context.Background(), "payments", "payments-group", "$").Err()
	if err != nil && !isGroupExistsErr(err) {
		log.Fatalf("Failed to create group: %v", err)
	}

	paymentHandler := handlers.NewPaymentHandler(redisClient)
	summaryHandler := handlers.NewGetPaymentsSumaryHandler(dbpool)
	purgeHandler := handlers.NewPurgePaymentsHandler(dbpool)

	e.POST("/payments", paymentHandler.Handle)
	e.GET("/payments-summary", summaryHandler.Handle)
	e.POST("/purge-payments", purgeHandler.Handle)

	e.Use(func(next echo.HandlerFunc) echo.HandlerFunc {
		return func(c echo.Context) error {
			c.Set("service_name", appConfig.Telemetry.ServiceName)
			return next(c)
		}
	})
	e.Use(middleware.Recover())

	err = e.Start(fmt.Sprintf("%s:%d", appConfig.Server.Host, appConfig.Server.Port))
	if err != nil {
		log.Fatal(err)
	}
	log.Printf("Server is running on %s:%d", appConfig.Server.Host, appConfig.Server.Port)
}

func isGroupExistsErr(err error) bool {
	return err != nil && (err.Error() == "BUSYGROUP Consumer Group name already exists")
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
