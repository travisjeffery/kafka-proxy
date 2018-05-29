package kafkaproxy

import (
	"context"
	"sync"

	"github.com/travisjeffery/jocko/jocko"
	"github.com/travisjeffery/jocko/protocol"
)

// Proxy enables you to proxy requests to a Kafka broker and inspect or modify the requests and
// responses with middleware.
type Proxy struct {
	conn          Conn
	handlerLock   sync.Mutex
	requestStack  []HandlerFunc
	responseStack []HandlerFunc
	shutdown      bool
	shutdownLock  sync.Mutex
	shutdownCh    chan struct{}
}

// New creates a new proxy connected to the broker behind the given conn.
func New(conn Conn) *Proxy {
	return &Proxy{conn: conn}
}

// UseRequest pushes a handler on the middleware stack to handle requests.
func (p *Proxy) UseRequest(h HandlerFunc) *Proxy {
	p.handlerLock.Lock()
	defer p.handlerLock.Unlock()
	p.requestStack = append(p.requestStack, h)
	return p
}

// UseReponse pushes a handler on the middleware stack to handle responses.
func (p *Proxy) UseResponse(h HandlerFunc) *Proxy {
	p.handlerLock.Lock()
	defer p.handlerLock.Unlock()
	p.responseStack = append(p.responseStack, h)
	return p
}

// Run starts reading requests and writing responses, while dispatching to their respective handlers.
func (p *Proxy) Run(ctx context.Context, requests <-chan *jocko.Context, responses chan<- *jocko.Context) {
	for {
		select {
		case <-p.shutdownCh:
			return
		case <-ctx.Done():
			return
		case reqCtx := <-requests:
			// 1. call middleware on request
			for _, fn := range p.requestStack {
				fn(reqCtx)
			}
			// 2. send to kafka
			// todo: handle rest of request types
			switch req := reqCtx.Request.(type) {
			case *protocol.FetchRequest:
				reqCtx.Response, reqCtx.Error = p.conn.Fetch(req)
			}
			// 3. call middlware on response
			for _, fn := range p.responseStack {
				fn(reqCtx)
			}
			// 4. write response
			responses <- reqCtx
		}
	}
}

// Shutdown stops the proxy from running.
func (p *Proxy) Shutdown() error {
	p.shutdownLock.Lock()
	defer p.shutdownLock.Unlock()
	p.shutdown = true
	close(p.shutdownCh)
	return nil
}

// Conn is used to send requests to a Kafka broker.
type Conn interface {
	Fetch(*protocol.FetchRequest) (*protocol.FetchResponses, error)
}

// Handler is used to handle request/response contexts.
type HandlerFunc func(*jocko.Context)
