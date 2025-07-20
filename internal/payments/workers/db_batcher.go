package workers

import (
	"context"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
	"log/slog"
	"rinha/internal/payments"
	"time"
)

const (
	maxBatchSize   = 100
	maxBatchWindow = 2 * time.Millisecond
)

type DbBatcher struct {
	dbpool   *pgxpool.Pool
	bufferCh chan payments.Payment
	logger   *slog.Logger
}

func NewDbBatcher(dbpool *pgxpool.Pool, logger *slog.Logger) *DbBatcher {
	return &DbBatcher{
		dbpool:   dbpool,
		bufferCh: make(chan payments.Payment, 10*maxBatchSize),
		logger:   logger,
	}
}

func (db *DbBatcher) PushPayment(payment payments.Payment) {
	select {
	case db.bufferCh <- payment:
	default:
		db.logger.Error("Buffer channel is full, dropping payment")
	}
}

func (db *DbBatcher) Run() {
	var (
		batch      []payments.Payment
		timer      *time.Timer
		timerCh    <-chan time.Time
		addToBatch = func(payment payments.Payment) {
			batch = append(batch, payment)
			if len(batch) == 1 {
				if timer == nil {
					timer = time.NewTimer(maxBatchWindow)
				} else {
					timer.Reset(maxBatchWindow)
				}
				timerCh = timer.C
			}
			if len(batch) >= maxBatchSize {
				db.flush(batch)
				batch = nil
				if timer != nil {
					timer.Stop()
				}
				timerCh = nil
			}
		}
	)

	for {
		select {
		case payment := <-db.bufferCh:
			addToBatch(payment)
		case <-timerCh:
			if len(batch) > 0 {
				db.logger.Debug("Flushing batch", "batchSize", len(batch))
				db.flush(batch)
				batch = nil
			}
			timerCh = nil
		}
	}
}

var tracer = otel.Tracer("db-batcher")

func (db *DbBatcher) flush(batch []payments.Payment) {
	go func(batchCopy []payments.Payment) {
		ctx, span := tracer.Start(
			context.Background(),
			"db_batcher.flush",
			trace.WithAttributes(
				attribute.Int("batch.size", len(batchCopy)),
			),
		)
		defer span.End()

		if len(batchCopy) == 1 {
			_, err := db.dbpool.Exec(
				ctx,
				"INSERT INTO payments (amount, requested_at, service_used, correlation_id) VALUES ($1, $2, $3, $4)",
				batchCopy[0].Amount,
				batchCopy[0].RequestedAt,
				batchCopy[0].ServiceUsed,
				batchCopy[0].CorrelationId,
			)
			if err != nil {
				db.logger.Error("failed to insert payment into database", "error", err)
			}
			return
		}

		_, err := db.dbpool.CopyFrom(
			ctx,
			pgx.Identifier{"payments"},
			[]string{"amount", "requested_at", "service_used", "correlation_id"},
			pgx.CopyFromSlice(len(batchCopy), func(i int) ([]any, error) {
				return []any{batchCopy[i].Amount, batchCopy[i].RequestedAt, batchCopy[i].ServiceUsed, batchCopy[i].CorrelationId}, nil
			}),
		)
		if err != nil {
			db.logger.Error("failed to insert payment into database", "error", err)
			span.RecordError(err)
			span.SetStatus(codes.Error, err.Error())
		} else {
			span.SetStatus(codes.Ok, "")
			span.SetAttributes(attribute.Int("rows.inserted", len(batchCopy)))
		}

	}(batch)
}
