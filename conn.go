package kafkaproxy

import (
	"context"

	"github.com/travisjeffery/jocko/jocko"
	"github.com/travisjeffery/jocko/protocol"
)

type Conn struct {
	*jocko.Conn
}

func (c *Conn) Run(ctx context.Context, request *protocol.Request) (*protocol.Response, error) {
	response := &protocol.Response{
		CorrelationID: request.CorrelationID,
	}
	var err error
	switch req := request.Body.(type) {
	case *protocol.ProduceRequest:
		response.Body, err = c.Produce(req)
	case *protocol.FetchRequest:
		response.Body, err = c.Fetch(req)
	case *protocol.OffsetsRequest:
		response.Body, err = c.Offsets(req)
	case *protocol.MetadataRequest:
		response.Body, err = c.Metadata(req)
	case *protocol.LeaderAndISRRequest:
		response.Body, err = c.LeaderAndISR(req)
	case *protocol.StopReplicaRequest:
		response.Body, err = c.StopReplica(req)
	case *protocol.UpdateMetadataRequest:
		response.Body, err = c.UpdateMetadata(req)
	case *protocol.ControlledShutdownRequest:
		response.Body, err = c.ControlledShutdown(req)
	case *protocol.OffsetCommitRequest:
		response.Body, err = c.OffsetCommit(req)
	case *protocol.OffsetFetchRequest:
		response.Body, err = c.OffsetFetch(req)
	case *protocol.FindCoordinatorRequest:
		response.Body, err = c.FindCoordinator(req)
	case *protocol.JoinGroupRequest:
		response.Body, err = c.JoinGroup(req)
	case *protocol.HeartbeatRequest:
		response.Body, err = c.Heartbeat(req)
	case *protocol.LeaveGroupRequest:
		response.Body, err = c.LeaveGroup(req)
	case *protocol.SyncGroupRequest:
		response.Body, err = c.SyncGroup(req)
	case *protocol.DescribeGroupsRequest:
		response.Body, err = c.DescribeGroups(req)
	case *protocol.ListGroupsRequest:
		response.Body, err = c.ListGroups(req)
	case *protocol.SaslHandshakeRequest:
		response.Body, err = c.SaslHandshake(req)
	case *protocol.APIVersionsRequest:
		response.Body, err = c.APIVersions(req)
	case *protocol.CreateTopicRequests:
		response.Body, err = c.CreateTopics(req)
	case *protocol.DeleteTopicsRequest:
		response.Body, err = c.DeleteTopics(req)
	}
	return response, err
}
