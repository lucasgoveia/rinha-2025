package payments

import (
	"context"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"log"
	"log/slog"
	"time"
)

const (
	bufferSize     = 1000
	maxBatchSize   = 100
	maxBatchWindow = 2 * time.Millisecond
)

type PaymentStore struct {
	dbpool *pgxpool.Pool
	buffer chan Payment
	done   chan struct{}
	logger *slog.Logger
	count  uint64
}

func NewPaymentStore(dbpool *pgxpool.Pool, logger *slog.Logger) *PaymentStore {
	ps := &PaymentStore{
		dbpool: dbpool,
		buffer: make(chan Payment, bufferSize),
		done:   make(chan struct{}),
		logger: logger,
	}
	go ps.consume()
	return ps
}

func (ps *PaymentStore) Add(p Payment) bool {
	select {
	case ps.buffer <- p:
		return true
	default:
		ps.logger.Warn("payment buffer is full, dropping payment", "correlationId", p.CorrelationId)
		return false
	}
}

func (ps *PaymentStore) Summary(ctx context.Context, from, to *time.Time) (*Summary, error) {

	const query = `
	SELECT COUNT(*) AS total_requests,
	      SUM(amount) AS total_amount,
	      service_used
	FROM payments
	WHERE ($1::timestamp IS NULL OR requested_at >= $1::timestamp)
	 AND ($2::timestamp IS NULL OR requested_at <= $2::timestamp)
	GROUP BY service_used;
	`

	var defaultsummary ProcessorSummary
	var fallbacksummary ProcessorSummary

	rows, err := ps.dbpool.Query(ctx, query, from, to)
	defer rows.Close()
	if err != nil {
		return nil, err
	}

	for rows.Next() {
		var totalRequests int64
		var totalAmount float64
		var processor string
		_ = rows.Scan(&totalRequests, &totalAmount, &processor)

		if processor == "default" {
			defaultsummary.TotalRequests = totalRequests
			defaultsummary.TotalAmount = totalAmount
		} else {
			fallbacksummary.TotalRequests = totalRequests
			fallbacksummary.TotalAmount = totalAmount
		}
	}

	return &Summary{
		Default:  defaultsummary,
		Fallback: fallbacksummary,
	}, nil
}

func (ps *PaymentStore) Purge(ctx context.Context) error {
	_, err := ps.dbpool.Exec(ctx, "TRUNCATE TABLE payments")
	return err
}

func (ps *PaymentStore) Close() { close(ps.done) }

func (ps *PaymentStore) consume() {
	var (
		batch      []Payment
		timer      *time.Timer
		timerCh    <-chan time.Time
		addToBatch = func(payment Payment) {
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
				ps.flush(batch)
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
		case payment := <-ps.buffer:
			addToBatch(payment)
		case <-timerCh:
			if len(batch) > 0 {
				ps.flush(batch)
				batch = nil
			}
			timerCh = nil
		case <-ps.done:
			if len(batch) > 0 {
				ps.flush(batch)
				batch = nil
			}
			return
		}
	}
}

func (ps *PaymentStore) flush(batch []Payment) {
	ctx := context.Background()

	go func(batchCopy []Payment) {
		if len(batchCopy) == 1 {
			_, err := ps.dbpool.Exec(
				ctx,
				"INSERT INTO payments (amount, requested_at, service_used, correlation_id) VALUES ($1, $2, $3, $4)",
				batchCopy[0].Amount,
				batchCopy[0].RequestedAt,
				batchCopy[0].Processor,
				batchCopy[0].CorrelationId,
			)
			if err != nil {
				log.Println("failed to insert payment into database", "error", err)
			}
			return
		}

		_, err := ps.dbpool.CopyFrom(
			ctx,
			pgx.Identifier{"payments"},
			[]string{"amount", "requested_at", "service_used", "correlation_id"},
			pgx.CopyFromSlice(len(batchCopy), func(i int) ([]any, error) {
				return []any{batchCopy[i].Amount, batchCopy[i].RequestedAt, batchCopy[i].Processor, batchCopy[i].CorrelationId}, nil
			}),
		)
		if err != nil {
			log.Println("failed to insert payment into database", "error", err)
		} else {
		}

	}(batch)
}
