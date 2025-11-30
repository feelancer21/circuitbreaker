package circuitbreaker

import (
	"context"

	"github.com/lightningnetwork/lnd/routing/route"
)

// PreProcessorFactory is a function that creates a PreProcessor for a given node.
type PreProcessorFactory func(route.Vertex) PreProcessor

// PreProcessor is a function that preprocesses intercepted HTLC events.
// If it returns false, the HTLC is rejected immediately. Otherwise, it is processed
// normally by the circuitbreaker.
type PreProcessor func(ctx context.Context, event InterceptEvent) bool

// InterceptEvent represents an intercepted HTLC event.
type InterceptEvent interface {
	// The id of the channel that is part of the incoming circuit.
	GetIncomingChanID() uint64

	// The index of the incoming htlc in the incoming channel.
	GetIncomingHtlcId() uint64

	// The incoming HTLC amount.
	GetIncomingMsat() uint64

	// The requested outgoing channel id for this forwarded HTLC. Because of
	// non-strict forwarding, this isn't necessarily the channel over which the
	// packet will be forwarded eventually. A different channel to the same
	// peer may be selected as well.
	GetOutgoingReqChanID() uint64

	// The outgoing HTLC amount.
	GetOutgoingMsat() uint64

	// The HTLC payment hash. This value is not guaranteed to be unique per request.
	GetPaymentHash() []byte
}

// DefaultPreProcessorFactory returns a PreProcessor that accepts all HTLCs.
func DefaultPreProcessorFactory(nodePub route.Vertex) PreProcessor {
	return func(ctx context.Context, event InterceptEvent) bool {
		// By default, we accept all HTLCs.
		return true
	}
}
