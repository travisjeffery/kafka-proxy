package kafkaproxy_test

//go:generate mocker --out conn_test.go --pkg kafkaproxy_test . Conn

import (
	"context"
	"testing"

	"github.com/travisjeffery/jocko/jocko"
	"github.com/travisjeffery/jocko/protocol"
	"github.com/travisjeffery/kafka-proxy"
)

func TestProxy(t *testing.T) {
	conn := &MockConn{
		FetchFunc: func(req *protocol.FetchRequest) (*protocol.FetchResponses, error) {
			return &protocol.FetchResponses{
				APIVersion: 1,
			}, nil
		},
	}

	proxy := kafkaproxy.New(conn)
	requests := make(chan *jocko.Context, 8)
	responses := make(chan *jocko.Context, 8)

	proxy.UseResponse(func(ctx *jocko.Context) {
		ctx.Response.(*protocol.FetchResponses).APIVersion = 2
	})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go proxy.Run(ctx, requests, responses)

	requests <- &jocko.Context{
		Request: &protocol.FetchRequest{},
	}

	response := <-responses

	got, want := response.Response.(*protocol.FetchResponses).APIVersion, int16(2)
	if got != want {
		t.Fatalf("got api version: %d, want %d", got, want)
	}
}
