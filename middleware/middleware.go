package middleware

import (
	"context"
	"log"
	"time"

	"github.com/travisjeffery/jocko/protocol"
	kafkaproxy "github.com/travisjeffery/kafka-proxy"
)

// Log is middleware that logs the request's api key and its duration.
func Log() kafkaproxy.Middleware {
	return func(next kafkaproxy.Endpoint) kafkaproxy.Endpoint {
		return func(ctx context.Context, request interface{}) (interface{}, error) {
			t := time.Now()
			res, err := next(ctx, request)
			req := request.(protocol.Body)
			log.Printf("api key: %d, duration: %v\n", req.Key(), time.Since(t))
			return res, err
		}
	}
}
