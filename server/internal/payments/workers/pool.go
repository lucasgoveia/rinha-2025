package workers

import (
	"container/heap"
	"context"
	"errors"
	"log/slog"
	"math/rand"
	"rinha/internal/payments"
	"runtime"
	"time"
)

const (
	queueCapacity  = 32768
	retryCapacity  = 8192
	maxRetries     = 20
	baseBackoff    = 500 * time.Millisecond
	maxBackoff     = 5 * time.Second
	jitterFraction = 0.20 // 20 %
)

var numWorkers = runtime.GOMAXPROCS(0) * 8

type retryItem struct {
	msg         *payments.PaymentMessage
	nextAttempt time.Time
	index       int
}

type retryHeap []*retryItem

func (h retryHeap) Len() int           { return len(h) }
func (h retryHeap) Less(i, j int) bool { return h[i].nextAttempt.Before(h[j].nextAttempt) }
func (h retryHeap) Swap(i, j int)      { h[i], h[j] = h[j], h[i]; h[i].index = i; h[j].index = j }
func (h *retryHeap) Push(x any)        { *h = append(*h, x.(*retryItem)) }
func (h *retryHeap) Pop() any {
	old := *h
	n := len(old)
	item := old[n-1]
	*h = old[:n-1]
	return item
}

type WorkerPool struct {
	rng               *rand.Rand
	queue             chan *payments.PaymentMessage
	retryQueue        chan *retryItem
	defaultProcessor  *payments.PaymentProcessor
	fallbackProcessor *payments.PaymentProcessor
	store             *payments.PaymentStore
	logger            *slog.Logger
	healthMonitor     *ProcessorHealthMonitor
}

func NewWorkerPool(def, fallbackProcessor *payments.PaymentProcessor, store *payments.PaymentStore, logger *slog.Logger, healthMonitor *ProcessorHealthMonitor) *WorkerPool {
	return &WorkerPool{
		rng:               rand.New(rand.NewSource(time.Now().UnixNano())),
		queue:             make(chan *payments.PaymentMessage, queueCapacity),
		retryQueue:        make(chan *retryItem, retryCapacity),
		defaultProcessor:  def,
		fallbackProcessor: fallbackProcessor,
		store:             store,
		logger:            logger,
		healthMonitor:     healthMonitor,
	}
}

func (p *WorkerPool) Start() {
	for range numWorkers {
		go p.run()
	}
	go p.retryLoop()
}

func (p *WorkerPool) Stop() {
	close(p.queue)
	close(p.retryQueue)
}

func (p *WorkerPool) Submit(msg *payments.PaymentMessage) bool {
	select {
	case p.queue <- msg:
		return true
	default:
		p.logger.Warn("Submit queue is full, dropping message")
		return false
	}
}

func (p *WorkerPool) run() {
	ctx := context.Background()
	for msg := range p.queue {
		p.process(ctx, msg)
	}
}

func (p *WorkerPool) process(ctx context.Context, msg *payments.PaymentMessage) {
	m := msg
	processorType, err := p.healthMonitor.DetermineProcessor()

	if err != nil && errors.Is(err, ErrBothProcessorsUnavailable) {
		p.retry(m)
	}

	if processorType == payments.ProcessorTypeDefault {
		p.processDefault(ctx, m)
		return
	} else if processorType == payments.ProcessorTypeFallback {
		p.processFallback(ctx, m)
		return
	}
}

func (p *WorkerPool) processDefault(ctx context.Context, msg *payments.PaymentMessage) {
	err := p.defaultProcessor.Process(ctx, msg)
	if err != nil && errors.Is(err, payments.ErrUnavailableProcessor) {
		p.logger.Debug("Default processor unavailable")
		p.retry(msg)
		return
	}

	p.store.Add(payments.NewPayment(msg.Amount, msg.CorrelationId, payments.ProcessorTypeDefault, msg.RequestedAt))
}

func (p *WorkerPool) processFallback(ctx context.Context, msg *payments.PaymentMessage) {
	err := p.fallbackProcessor.Process(ctx, msg)
	if err != nil && errors.Is(err, payments.ErrUnavailableProcessor) {
		p.logger.Debug("Fallback processor unavailable")
		p.retry(msg)
		return
	}

	p.store.Add(payments.NewPayment(msg.Amount, msg.CorrelationId, payments.ProcessorTypeFallback, msg.RequestedAt))
}

func (p *WorkerPool) retry(msg *payments.PaymentMessage) {
	if msg.RetryCount >= maxRetries {
		// Drop the message
		p.logger.Warn("Max retries exceeded, dropping message", "correlationId", msg.CorrelationId)
		return
	}

	msg.RetryCount++

	delay := calcBackoff(p.rng, msg.RetryCount)

	item := &retryItem{
		msg:         msg,
		nextAttempt: time.Now().Add(delay),
	}

	select {
	case p.retryQueue <- item:
	default:
		p.logger.Warn("Retry queue is full, dropping message", "correlationId", msg.CorrelationId)
	}
}

func (p *WorkerPool) retryLoop() {
	h := &retryHeap{}
	heap.Init(h)

	var timer *time.Timer
	resetTimer := func(d time.Duration) {
		if timer == nil {
			timer = time.NewTimer(d)
			return
		}

		if !timer.Stop() {
			select {
			case <-timer.C:
			default:
			}
		}
		timer.Reset(d)
	}

	for {
		var nextCh <-chan time.Time
		if h.Len() > 0 {
			delay := time.Until((*h)[0].nextAttempt)
			if delay < 0 {
				delay = 0
			}
			resetTimer(delay)
			nextCh = timer.C
		}
		select {
		case item, ok := <-p.retryQueue:
			if !ok {
				return
			}
			heap.Push(h, item)
		case <-nextCh:
			now := time.Now()
			for h.Len() > 0 && now.After((*h)[0].nextAttempt) {
				it := heap.Pop(h).(*retryItem)
				_ = p.Submit(it.msg)
			}
		}
	}
}

func calcBackoff(rng *rand.Rand, retryCount int) time.Duration {
	delay := baseBackoff * (1 << uint(retryCount))
	if delay > maxBackoff {
		delay = maxBackoff
	}
	jitterRange := int64(float64(delay) * jitterFraction)
	return delay - time.Duration(jitterRange) + time.Duration(rng.Int63n(2*jitterRange))
}
