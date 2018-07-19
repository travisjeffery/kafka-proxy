package kafkaproxy

import (
	"context"
	"net"
	"sync"

	"gopkg.in/bufio.v1"

	"github.com/travisjeffery/jocko/protocol"
)

// SizeLen is the number of bytes that make up the size field of the requet.
const SizeLen = 4

// Endpoint handles a API request and returns a response.
type Endpoint func(ctx context.Context, request *protocol.Request) (response *protocol.Response, err error)

// Middleware is a chainable behavior modifier for endpoints.
type Middleware func(Endpoint) Endpoint

// Client represents a Kafka client.
type Client interface {
	Run(ctx context.Context, req *protocol.Request) (*protocol.Response, error)
}

// Proxy is a layer 7/application level proxy for Kafka with pluggable middleware support. You can
// change client requests before being sent to the broker, or change responses from the broker
// before being sent to the client.
type Proxy struct {
	sync.Mutex
	err      error
	donec    chan struct{}
	ipPort   string
	endpoint Endpoint
	ln       net.Listener

	ListenFunc func(net, laddr string) (net.Listener, error)
}

// New creates a new kafka proxy that proxies requests made on the conn to the brokers connected to
// the client.
func New(ipPort string, c Client) *Proxy {
	return &Proxy{
		ipPort:     ipPort,
		donec:      make(chan struct{}),
		ListenFunc: net.Listen,
		endpoint: func(ctx context.Context, request *protocol.Request) (*protocol.Response, error) {
			return c.Run(ctx, request)
		},
	}
}

// With adds a new middleware to handle requests and responses.
func (p *Proxy) With(me Middleware) {
	p.Lock()
	defer p.Unlock()
	p.endpoint = me(p.endpoint)
}

// Run starts the listener and request proxy.
func (p *Proxy) Run(ctx context.Context) (err error) {
	p.ln, err = p.netListen()("tcp", p.ipPort)
	if err != nil {
		return err
	}
	errc := make(chan error, 1)
	go p.serveListener(errc, p.ln)
	go p.awaitFirstError(errc)
	return nil
}

// Wait waits for the Proxy to finish running. Currently this can only happen if a Listener is closed, or Close is already called on the proxy.
//
// It is only valid to call Wait after a successful call to Run.
func (p *Proxy) Wait() error {
	close(p.donec)
	return p.err
}

// Close closes the proxy's listener.
func (p *Proxy) Close() error {
	return p.ln.Close()
}

func (p *Proxy) serveListener(errc chan error, ln net.Listener) {
	for {
		c, err := ln.Accept()
		if err != nil {
			errc <- err
			return
		}
		go p.serveConn(errc, c)
	}
}

func (p *Proxy) serveConn(errc chan error, c net.Conn) {
	ctx := context.Background()
	br := bufio.NewReader(c)

	for {
		sizePeek, err := br.Peek(SizeLen)
		if err != nil {
			errc <- err
			return
		}

		reqSize := SizeLen + int(protocol.Encoding.Uint32(sizePeek))

		var reqb []byte
		for {
			b, err := br.ReadN(reqSize)
			if err == bufio.ErrBufferFull && reqSize != br.Buffered() {
				reqb = append(reqb, b...)
				continue
			}
			if err != nil {
				errc <- err
				return
			}
			reqb = append(reqb, b...)
			break
		}

		// TODO: a decoder that takes a bufio.Reader would be nice
		d := protocol.NewDecoder(reqb)

		header := new(protocol.RequestHeader)
		if err := header.Decode(d); err != nil {
			errc <- err
			return
		}

		var req protocol.VersionedDecoder

		switch header.APIKey {
		case protocol.ProduceKey:
			req = &protocol.ProduceRequest{}
		case protocol.FetchKey:
			req = &protocol.FetchRequest{}
		case protocol.OffsetsKey:
			req = &protocol.OffsetsRequest{}
		case protocol.MetadataKey:
			req = &protocol.MetadataRequest{}
		case protocol.LeaderAndISRKey:
			req = &protocol.LeaderAndISRRequest{}
		case protocol.StopReplicaKey:
			req = &protocol.StopReplicaRequest{}
		case protocol.UpdateMetadataKey:
			req = &protocol.UpdateMetadataRequest{}
		case protocol.ControlledShutdownKey:
			req = &protocol.ControlledShutdownRequest{}
		case protocol.OffsetCommitKey:
			req = &protocol.OffsetCommitRequest{}
		case protocol.OffsetFetchKey:
			req = &protocol.OffsetFetchRequest{}
		case protocol.FindCoordinatorKey:
			req = &protocol.FindCoordinatorRequest{}
		case protocol.JoinGroupKey:
			req = &protocol.JoinGroupRequest{}
		case protocol.HeartbeatKey:
			req = &protocol.HeartbeatRequest{}
		case protocol.LeaveGroupKey:
			req = &protocol.LeaveGroupRequest{}
		case protocol.SyncGroupKey:
			req = &protocol.SyncGroupRequest{}
		case protocol.DescribeGroupsKey:
			req = &protocol.DescribeGroupsRequest{}
		case protocol.ListGroupsKey:
			req = &protocol.ListGroupsRequest{}
		case protocol.SaslHandshakeKey:
			req = &protocol.SaslHandshakeRequest{}
		case protocol.APIVersionsKey:
			req = &protocol.APIVersionsRequest{}
		case protocol.CreateTopicsKey:
			req = &protocol.CreateTopicRequests{}
		case protocol.DeleteTopicsKey:
			req = &protocol.DeleteTopicsRequest{}
		}

		if err := req.Decode(d, header.APIVersion); err != nil {
			errc <- err
			return
		}

		res, err := p.endpoint(ctx, &protocol.Request{
			CorrelationID: header.CorrelationID,
			ClientID:      header.ClientID,
			Body:          req.(protocol.Body),
		})
		if err != nil {
			errc <- err
			return
		}

		resb, err := protocol.Encode(res)
		if err != nil {
			errc <- err
			return
		}

		_, err = c.Write(resb)
		if err != nil {
			errc <- err
			return
		}
	}
}

func (p *Proxy) awaitFirstError(errc chan error) {
	p.err = <-errc
	close(p.donec)
}

func (p *Proxy) netListen() func(net, laddr string) (net.Listener, error) {
	if p.ListenFunc != nil {
		return p.ListenFunc
	}
	return net.Listen
}
