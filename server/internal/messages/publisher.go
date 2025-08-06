package messages

import (
	"bufio"
	"net"
	"sync"
	"time"
)

type Publisher struct {
	socketPath string
	maxConns   int
	conns      chan net.Conn
	dialer     *net.Dialer
	mu         sync.Mutex
}

func NewPublisher(socketPath string, maxConns int) (*Publisher, error) {
	p := &Publisher{
		socketPath: socketPath,
		maxConns:   maxConns,
		conns:      make(chan net.Conn, maxConns),
		dialer: &net.Dialer{
			Timeout:   100 * time.Millisecond,
			KeepAlive: 30 * time.Second,
		},
	}

	c, err := p.dialer.Dial("unix", socketPath)
	if err != nil {
		return nil, err
	}
	p.conns <- c
	return p, nil
}

var bwPool = sync.Pool{
	New: func() any { return bufio.NewWriterSize(nil, 512) },
}

func (p *Publisher) Publish(msg []byte) error {
	conn, err := p.acquire()
	if err != nil {
		return err
	}

	bw := bwPool.Get().(*bufio.Writer)
	bw.Reset(conn)

	if _, err = bw.Write(msg); err == nil {
		err = bw.WriteByte('\n')
	}
	if err == nil {
		err = bw.Flush()
	}

	bwPool.Put(bw)

	if err != nil {
		// problema na conexão; fecha-a e substitui
		_ = conn.Close()
		p.replace()
		return err
	}

	p.release(conn)
	return nil
}

func (p *Publisher) acquire() (net.Conn, error) {
	select {
	case c := <-p.conns:
		return c, nil
	default:
		return p.dialer.Dial("unix", p.socketPath)
	}
}

func (p *Publisher) release(conn net.Conn) {
	select {
	case p.conns <- conn:
	default:
		_ = conn.Close()
	}
}

func (p *Publisher) replace() {
	c, err := p.dialer.Dial("unix", p.socketPath)
	if err != nil {
		return
	}
	p.release(c)
}
