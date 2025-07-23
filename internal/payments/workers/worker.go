package workers

import (
	"context"
	"encoding/json"
	"errors"
	"github.com/redis/go-redis/v9"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
	"log/slog"
	"rinha/internal/payments"
	"sync"
	"time"
)

type PaymentWorker struct {
	defaultPaymentService  *payments.PaymentService
	fallbackPaymentService *payments.PaymentService
	serviceMonitor         *ServiceMonitor
	logger                 *slog.Logger
	dbBatcher              *DbBatcher
	redisClient            *redis.Client

	messagesTotal     int64
	messagesFailed    int64
	messagesSucceeded int64
}

func NewPaymentWorker(defaultPaymentService *payments.PaymentService, fallbackPaymentService *payments.PaymentService, logger *slog.Logger, serviceMonitor *ServiceMonitor, dbBatcher *DbBatcher, redisClient *redis.Client) *PaymentWorker {
	o := &PaymentWorker{
		defaultPaymentService:  defaultPaymentService,
		fallbackPaymentService: fallbackPaymentService,
		serviceMonitor:         serviceMonitor,
		logger:                 logger,
		dbBatcher:              dbBatcher,
		redisClient:            redisClient,
		messagesTotal:          0,
		messagesFailed:         0,
		messagesSucceeded:      0,
	}

	return o
}

type workFactorBalancer struct {
	target   map[payments.ServiceType]int // target work
	current  map[payments.ServiceType]int // actual delivered
	lastUsed payments.ServiceType         // last service used
	mu       *sync.Mutex
}

func newBalancer(targetDefault, targetFallback int) *workFactorBalancer {
	return &workFactorBalancer{
		target: map[payments.ServiceType]int{
			payments.ServiceTypeDefault:  targetDefault,
			payments.ServiceTypeFallback: targetFallback,
		},
		current: map[payments.ServiceType]int{
			payments.ServiceTypeDefault:  0,
			payments.ServiceTypeFallback: 0,
		},
		lastUsed: "",
		mu:       &sync.Mutex{},
	}
}

func (b *workFactorBalancer) NextService() payments.ServiceType {
	b.mu.Lock()
	defer b.mu.Unlock()

	defaultDone := b.current[payments.ServiceTypeDefault] >= b.target[payments.ServiceTypeDefault]
	fallbackDone := b.current[payments.ServiceTypeFallback] >= b.target[payments.ServiceTypeFallback]

	if defaultDone && fallbackDone {
		return ""
	}

	if !defaultDone && !fallbackDone {
		if b.lastUsed == payments.ServiceTypeDefault {
			b.current[payments.ServiceTypeFallback]++
			b.lastUsed = payments.ServiceTypeFallback
			return payments.ServiceTypeFallback
		} else {
			b.current[payments.ServiceTypeDefault]++
			b.lastUsed = payments.ServiceTypeDefault
			return payments.ServiceTypeDefault
		}
	}

	if !defaultDone {
		b.current[payments.ServiceTypeDefault]++
		b.lastUsed = payments.ServiceTypeDefault
		return payments.ServiceTypeDefault
	}

	b.current[payments.ServiceTypeFallback]++
	b.lastUsed = payments.ServiceTypeFallback
	return payments.ServiceTypeFallback
}

func (w *PaymentWorker) Process(messages []payments.PaymentMessage) {
	ctx := context.Background()
	tr := otel.Tracer("worker")
	ctx, span := tr.Start(ctx, "process-batch")
	defer span.End()

	workFactor, err := w.serviceMonitor.CalculateServiceRequests(len(messages))

	if err != nil {
		for _, msg := range messages {
			m := msg
			w.retryFailedPayment(ctx, m)
		}
		return
	}

	const maxConcurrentRequests = 50
	sem := make(chan struct{}, maxConcurrentRequests)

	var (
		wg        sync.WaitGroup
		successCh = make(chan payments.Payment, len(messages))
		retryCh   = make(chan payments.PaymentMessage, len(messages))
	)

	span.SetAttributes(
		attribute.Int("messages_count", len(messages)),
	)

	span.SetAttributes(
		attribute.Int("default_factor", workFactor.DefaultWorkFactor),
		attribute.Int("fallback_factor", workFactor.FallbackWorkFactor),
	)

	workBalancer := newBalancer(workFactor.DefaultWorkFactor, workFactor.FallbackWorkFactor)

	w.logger.Debug("work factor", "default", workFactor.DefaultWorkFactor, "fallback", workFactor.FallbackWorkFactor)

	processMessage := func(m payments.PaymentMessage, serviceType payments.ServiceType) {
		ctx, span := tr.Start(ctx, "process message")
		span.SetAttributes(attribute.String("service_type", string(serviceType)))
		defer span.End()

		defer wg.Done()

		defaultAvailable := w.serviceMonitor.CheckServiceAvailable(payments.ServiceTypeDefault)
		fallbackAvailable := w.serviceMonitor.CheckServiceAvailable(payments.ServiceTypeFallback)

		var targetService *payments.PaymentService
		if serviceType == payments.ServiceTypeDefault && defaultAvailable {
			targetService = w.defaultPaymentService
		} else if serviceType == payments.ServiceTypeFallback && fallbackAvailable {
			targetService = w.fallbackPaymentService
		} else if !defaultAvailable && fallbackAvailable {
			targetService = w.fallbackPaymentService
			serviceType = payments.ServiceTypeFallback
		} else if defaultAvailable && !fallbackAvailable {
			targetService = w.defaultPaymentService
			serviceType = payments.ServiceTypeDefault
		} else {
			retryCh <- m
			return
		}

		span.SetAttributes(attribute.String("actual_service_type", string(serviceType)))

		sem <- struct{}{}
		defer func() { <-sem }()

		ctx2, span2 := tr.Start(ctx, "call-payment-service",
			trace.WithAttributes(
				attribute.String("service.type", string(serviceType)),
				attribute.Float64("payment.Amount", m.Amount),
				attribute.String("payment.correlation_id", m.CorrelationId),
			),
		)
		defer span2.End()

		err := targetService.Process(ctx2, m)

		if err != nil {
			span2.RecordError(err)
			span2.SetStatus(codes.Error, "Payment service call failed")
			if errors.Is(err, payments.ErrUnavailableProcessor) {
				w.logger.Error("Payment service unavailable", "service", serviceType, "error", err)
				w.serviceMonitor.InformFailure(serviceType)
				retryCh <- m
			}
			return
		}

		successCh <- payments.Payment{
			Amount:        m.Amount,
			RequestedAt:   m.RequestedAt,
			ServiceUsed:   serviceType,
			CorrelationId: m.CorrelationId,
		}
	}

	for _, msg := range messages {
		m := msg

		wg.Add(1)

		serviceType := workBalancer.NextService()

		go processMessage(m, serviceType)
	}

	go func() {
		for payment := range successCh {
			p := payment
			w.dbBatcher.PushPayment(p)
		}
	}()

	go func() {
		for msg := range retryCh {
			m := msg
			w.retryFailedPayment(ctx, m)
		}
	}()

	wg.Wait()
	close(successCh)
	close(retryCh)
}

func (w *PaymentWorker) retryFailedPayment(ctx context.Context, msg payments.PaymentMessage) {
	go func() {
		msg.RetryCount++

		const maxRetries = 10
		if msg.RetryCount > maxRetries {
			w.logger.Warn("Maximum retry attempts reached, dropping message",
				"correlationId", msg.CorrelationId,
				"retryCount", msg.RetryCount)
			return
		}

		baseDelay := 500 * time.Millisecond * (1 << uint(msg.RetryCount))
		if baseDelay > 5*time.Second {
			baseDelay = 5 * time.Second
		}

		jitterRange := float64(baseDelay) * 0.2
		jitter := time.Duration(float64(baseDelay) - jitterRange + (2 * jitterRange * float64(time.Now().UnixNano()%100) / 100))

		w.logger.Debug("Retrying failed payment",
			"correlationId", msg.CorrelationId,
			"retryCount", msg.RetryCount,
			"delay", jitter.String())

		time.Sleep(jitter)

		data, err := json.Marshal(msg)
		if err != nil {
			w.logger.Error("error while marshalling the Payment", "error", err)
			return
		}

		w.redisClient.XAdd(ctx, &redis.XAddArgs{
			Stream: "payments",
			Values: map[string]interface{}{
				"data": string(data),
			},
		}).Result()
	}()
}
