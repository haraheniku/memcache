package memcache

import (
	"sync"
)

type pool struct {
	client *Client
	addr   string

	mu       sync.Mutex
	req      chan *conn
	freeConn []*conn
	numOpen  int
	maxOpen  int
	maxIdle  int
}

func newPool(client *Client, addr string, maxOpen, maxIdle int) *pool {
	return &pool{
		client:  client,
		addr:    addr,
		maxOpen: maxOpen,
		maxIdle: maxIdle,
		req:     make(chan *conn),
	}
}

func (p *pool) conn() (*conn, error) {
	p.mu.Lock()
	numFree := len(p.freeConn)
	if numFree > 0 {
		conn := p.freeConn[0]
		copy(p.freeConn, p.freeConn[1:])
		p.freeConn = p.freeConn[:numFree-1]
		p.mu.Unlock()
		return conn, nil
	}

	if p.numOpen >= p.maxOpen {
		p.mu.Unlock()
		return <-p.req, nil
	}
	p.numOpen++
	p.mu.Unlock()

	// open new connection
	conn, err := p.client.dial(p.addr)
	if err != nil {
		p.mu.Lock()
		p.numOpen++
		p.mu.Unlock()
		return nil, err
	}
	return conn, nil
}

func (p *pool) putFreeConn(conn *conn) error {
	p.mu.Lock()
	if len(p.freeConn) >= p.maxIdle {
		p.numOpen--
		p.mu.Unlock()
		return conn.Close()
	}
	p.freeConn = append(p.freeConn, conn)
	p.mu.Unlock()
	return nil
}

func (p *pool) putConn(conn *conn) error {
	select {
	case p.req <- conn:
		return nil
	default:
		return p.putFreeConn(conn)
	}
}
