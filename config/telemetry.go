package config

import (
	"context"
	"fmt"
	"log"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/exporters/jaeger"
	"go.opentelemetry.io/otel/sdk/resource"
	tracesdk "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.17.0"
)

// InitTracer initializes an OTLP exporter, and configures the corresponding trace provider.
func InitTracer(cfg *TelemetryConfig) func() {
	if !cfg.Enabled {
		return func() {}
	}

	exp, err := jaeger.New(jaeger.WithCollectorEndpoint(jaeger.WithEndpoint(cfg.JaegerURL)))
	if err != nil {
		log.Fatalf("Failed to create Jaeger exporter: %v", err)
	}

	res, err := resource.New(context.Background(),
		resource.WithAttributes(
			semconv.ServiceName(cfg.ServiceName),
		),
	)
	if err != nil {
		log.Fatalf("Failed to create resource: %v", err)
	}

	tp := tracesdk.NewTracerProvider(
		tracesdk.WithSampler(tracesdk.AlwaysSample()),
		tracesdk.WithBatcher(exp),
		tracesdk.WithResource(res),
	)

	otel.SetTracerProvider(tp)

	return func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := tp.Shutdown(ctx); err != nil {
			fmt.Printf("Error shutting down tracer provider: %v", err)
		}
	}
}
