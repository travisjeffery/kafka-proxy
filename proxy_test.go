package kafkaproxy_test

//go:generate mocker --out client_test.go  --pkg kafkaproxy_test . Client

import (
	"bufio"
	"context"
	"errors"
	"net"
	"reflect"
	"testing"
	"time"

	"github.com/travisjeffery/jocko/protocol"
	kafkaproxy "github.com/travisjeffery/kafka-proxy"
	"github.com/travisjeffery/kafka-proxy/middleware"
)

func TestProxy(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	for _, test := range []struct {
		// name of the test.
		name string
		// request sent from client, pre mw.
		clientReq *protocol.Request
		// response sent back to client, post mw.
		clientRes *protocol.Response
		// request server sees, post mw.
		serverReq *protocol.Request
		// response server sends back, pre mw.
		serverRes *protocol.Response
		// mw to setup the proxy with.
		mw []kafkaproxy.Middleware
	}{
		{
			name: "passthrough with log",
			mw:   []kafkaproxy.Middleware{middleware.Log()},
			clientReq: &protocol.Request{
				CorrelationID: 1,
				ClientID:      "proxytest",
				Body: &protocol.ProduceRequest{
					APIVersion: 1,
					Timeout:    time.Second,
					TopicData:  []*protocol.TopicData{},
				},
			},
			serverReq: &protocol.Request{
				CorrelationID: 1,
				ClientID:      "proxytest",
				Body: &protocol.ProduceRequest{
					APIVersion: 1,
					Timeout:    time.Second,
					TopicData:  []*protocol.TopicData{},
				},
			},
			serverRes: &protocol.Response{
				Body: &protocol.ProduceResponse{
					APIVersion:   1,
					ThrottleTime: time.Second,
					Responses:    []*protocol.ProduceTopicResponse{},
				},
			},
			clientRes: &protocol.Response{
				Body: &protocol.ProduceResponse{
					APIVersion:   1,
					ThrottleTime: time.Second,
					Responses:    []*protocol.ProduceTopicResponse{},
				},
			},
		},
		{
			name: "modify",
			mw:   []kafkaproxy.Middleware{timeoutmw(time.Second * 3)},
			clientReq: &protocol.Request{
				CorrelationID: 1,
				ClientID:      "proxytest",
				Body: &protocol.ProduceRequest{
					APIVersion: 1,
					Timeout:    time.Second,
					TopicData:  []*protocol.TopicData{},
				},
			},
			serverReq: &protocol.Request{
				CorrelationID: 1,
				ClientID:      "proxytest",
				Body: &protocol.ProduceRequest{
					APIVersion: 1,
					Timeout:    3 * time.Second,
					TopicData:  []*protocol.TopicData{},
				},
			},
			serverRes: &protocol.Response{
				Body: &protocol.ProduceResponse{
					APIVersion:   1,
					ThrottleTime: time.Second,
					Responses:    []*protocol.ProduceTopicResponse{},
				},
			},
			clientRes: &protocol.Response{
				Body: &protocol.ProduceResponse{
					APIVersion:   1,
					ThrottleTime: time.Second * 3,
					Responses:    []*protocol.ProduceTopicResponse{},
				},
			},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := &MockClient{
				RunFunc: func(ctx context.Context, got interface{}) (interface{}, error) {
					if !reflect.DeepEqual(test.serverReq.Body, got) {
						t.Fatalf("requests don't match: got: %v, want: %v", got, test.clientReq.Body)
					}
					return test.serverRes, nil
				},
			}

			ln := newLocalListener(t)
			p := kafkaproxy.New(":0", c)
			p.ListenFunc = testListenFunc(t, ln)

			for _, mw := range test.mw {
				p.With(mw)
			}

			go func() {
				if err := p.Run(ctx); err != nil {
					t.Fatal(err)
				}
			}()

			client := testConn(t, ln)

			b, err := protocol.Encode(test.clientReq)
			if err != nil {
				t.Fatal(err)
			}
			_, err = client.Write(b)
			if err != nil {
				t.Fatal(err)
			}

			time.Sleep(100 * time.Millisecond)

			if !c.RunCalled() {
				t.Fatalf("run not called")
			}

			br := bufio.NewReader(client)
			sizePeek, err := br.Peek(kafkaproxy.SizeLen)
			if err != nil {
				t.Fatal(err)
			}
			size := int(protocol.Encoding.Uint32(sizePeek))
			resPeek, err := br.Peek(kafkaproxy.SizeLen + size)
			if err != nil {
				t.Fatal(err)
			}
			d := protocol.NewDecoder(resPeek)
			clientRes := &protocol.Response{
				Body: &protocol.ProduceResponse{},
			}
			if err = clientRes.Decode(d, test.clientReq.Body.Version()); err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(test.clientRes, clientRes) {
				t.Fatalf("client response: got: %v, want: %v", clientRes, test.clientRes)
			}
		})
	}
}

func newLocalListener(t *testing.T) net.Listener {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		ln, err = net.Listen("tcp", "[::1]:0")
		if err != nil {
			t.Fatal(err)
		}
	}
	return ln
}

func testListenFunc(t *testing.T, ln net.Listener) func(network, laddr string) (net.Listener, error) {
	return func(network, laddr string) (net.Listener, error) {
		if network != "tcp" {
			t.Errorf("got Listen call with network %q, not tcp", network)
			return nil, errors.New("invalid network")
		}
		return ln, nil
	}
}

func testConn(t *testing.T, ln net.Listener) net.Conn {
	t.Helper()
	client, err := net.Dial("tcp", ln.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	return client
}

func timeoutmw(timeout time.Duration) kafkaproxy.Middleware {
	return func(next kafkaproxy.Endpoint) kafkaproxy.Endpoint {
		return func(ctx context.Context, request interface{}) (interface{}, error) {
			req := request.(*protocol.ProduceRequest)
			req.Timeout = timeout
			response, err := next(ctx, request)
			res := response.(*protocol.Response).Body.(*protocol.ProduceResponse)
			res.ThrottleTime = timeout
			return response, err
		}
	}
}
