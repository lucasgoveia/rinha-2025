package messages

import (
	"bufio"
	"context"
	"encoding/json"
	"log/slog"
	"net"
	"os"
	"rinha/internal/payments"
	"rinha/internal/payments/workers"
	"sync"
	"time"
)

type Receiver struct {
	socketPath string
	ln         net.Listener
	logger     *slog.Logger
	workers    *workers.WorkerPool

	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup
}

func NewReceiver(socketPath string, workers *workers.WorkerPool, logger *slog.Logger) *Receiver {
	ctx, cancel := context.WithCancel(context.Background())
	return &Receiver{
		socketPath: socketPath,
		ctx:        ctx,
		cancel:     cancel,
		workers:    workers,
		logger:     logger,
	}
}

func (r *Receiver) Start() error {
	_ = os.Remove(r.socketPath)

	ln, err := net.Listen("unix", r.socketPath)

	if err != nil {
		return err
	}
	r.ln = ln

	r.wg.Add(1)
	r.acceptLoop()
	return nil
}

func (r *Receiver) Stop() {
	r.cancel()
	_ = r.ln.Close()
	r.wg.Wait()
}

func (r *Receiver) acceptLoop() {
	defer r.wg.Done()

	for {
		conn, err := r.ln.Accept()
		if err != nil {
			r.logger.Error("Failed to accept connection", "err", err)
			select {
			case <-r.ctx.Done():
				return
			default:
				time.Sleep(10 * time.Millisecond)
				continue
			}
		}

		r.logger.Info("Accepted UDS connection", "remoteAddr", conn.RemoteAddr())
		r.wg.Add(1)
		go r.readProducer(conn)
	}
}

func (r *Receiver) readProducer(conn net.Conn) {
	defer r.wg.Done()
	defer conn.Close()

	sc := bufio.NewScanner(conn)
	for sc.Scan() {
		var msg payments.PaymentMessage
		if err := json.Unmarshal(sc.Bytes(), &msg); err != nil {
			continue
		}

		select {
		case <-r.ctx.Done():
			return
		default:
			r.logger.Info("Received message", "msg", msg)
			r.workers.Submit(&msg)
		}
	}
}
