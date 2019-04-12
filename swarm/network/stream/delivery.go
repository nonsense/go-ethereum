// Copyright 2018 The go-ethereum Authors
// This file is part of the go-ethereum library.
//
// The go-ethereum library is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// The go-ethereum library is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with the go-ethereum library. If not, see <http://www.gnu.org/licenses/>.

package stream

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/ethereum/go-ethereum/metrics"
	"github.com/ethereum/go-ethereum/p2p/enode"
	"github.com/ethereum/go-ethereum/swarm/chunk"
	"github.com/ethereum/go-ethereum/swarm/log"
	"github.com/ethereum/go-ethereum/swarm/network"
	"github.com/ethereum/go-ethereum/swarm/network/timeouts"
	"github.com/ethereum/go-ethereum/swarm/spancontext"
	"github.com/ethereum/go-ethereum/swarm/storage"
	opentracing "github.com/opentracing/opentracing-go"
	olog "github.com/opentracing/opentracing-go/log"
)

var (
	processReceivedChunksCount    = metrics.NewRegisteredCounter("network.stream.received_chunks.count", nil)
	handleRetrieveRequestMsgCount = metrics.NewRegisteredCounter("network.stream.handle_retrieve_request_msg.count", nil)
	retrieveChunkFail             = metrics.NewRegisteredCounter("network.stream.retrieve_chunks_fail.count", nil)

	requestFromPeersCount     = metrics.NewRegisteredCounter("network.stream.request_from_peers.count", nil)
	requestFromPeersEachCount = metrics.NewRegisteredCounter("network.stream.request_from_peers_each.count", nil)

	lastReceivedChunksMsg = metrics.GetOrRegisterGauge("network.stream.received_chunks", nil)
)

type Delivery struct {
	netStore *storage.NetStore
	kad      *network.Kademlia
	getPeer  func(enode.ID) *Peer
	quit     chan struct{}
}

func NewDelivery(kad *network.Kademlia, netStore *storage.NetStore) *Delivery {
	return &Delivery{
		netStore: netStore,
		kad:      kad,
		quit:     make(chan struct{}),
	}
}

// RetrieveRequestMsg is the protocol msg for chunk retrieve requests
type RetrieveRequestMsg struct {
	Addr      storage.Address
	SkipCheck bool
	HopCount  uint8
}

func (d *Delivery) handleRetrieveRequestMsg(ctx context.Context, sp *Peer, req *RetrieveRequestMsg) error {
	log.Trace("handle retrieve request", "peer", sp.ID(), "hash", req.Addr)
	handleRetrieveRequestMsgCount.Inc(1)

	ctx, osp := spancontext.StartSpan(
		ctx,
		"handle.retrieve.request")

	osp.LogFields(olog.String("ref", req.Addr.String()))

	defer osp.Finish()

	ctx, cancel := context.WithTimeout(ctx, timeouts.FetcherGlobalTimeout)
	defer cancel()

	r := &storage.Request{
		Addr:     req.Addr,
		Origin:   sp.ID(),
		HopCount: req.HopCount,
	}
	chunk, err := d.netStore.Get(ctx, chunk.ModeGetRequest, r)
	if err != nil {
		retrieveChunkFail.Inc(1)
		log.Debug("ChunkStore.Get can not retrieve chunk", "peer", sp.ID().String(), "addr", req.Addr, "hopcount", req.HopCount, "err", err)
		return nil
	}

	log.Trace("retrieve request, delivery", "ref", req.Addr, "peer", sp.ID())
	syncing := false
	err = sp.Deliver(ctx, chunk, syncing)
	if err != nil {
		log.Warn("ERROR in handleRetrieveRequestMsg", "err", err)
	}
	osp.LogFields(olog.Bool("delivered", true))

	return nil
}

//Chunk delivery always uses the same message type....
type ChunkDeliveryMsg struct {
	Addr  storage.Address
	SData []byte // the stored chunk Data (incl size)
	peer  *Peer  // set in handleChunkDeliveryMsg
}

//...but swap accounting needs to disambiguate if it is a delivery for syncing or for retrieval
//as it decides based on message type if it needs to account for this message or not

//defines a chunk delivery for retrieval (with accounting)
type ChunkDeliveryMsgRetrieval ChunkDeliveryMsg

//defines a chunk delivery for syncing (without accounting)
type ChunkDeliveryMsgSyncing ChunkDeliveryMsg

// chunk delivery msg is response to retrieverequest msg
func (d *Delivery) handleChunkDeliveryMsg(ctx context.Context, sp *Peer, req interface{}) error {
	var osp opentracing.Span
	ctx, osp = spancontext.StartSpan(
		ctx,
		"handle.chunk.delivery")

	processReceivedChunksCount.Inc(1)

	// record the last time we received a chunk delivery message
	lastReceivedChunksMsg.Update(time.Now().UnixNano())

	var msg *ChunkDeliveryMsg
	var mode chunk.ModePut
	switch r := req.(type) {
	case *ChunkDeliveryMsgRetrieval:
		msg = (*ChunkDeliveryMsg)(r)
		peerPO := chunk.Proximity(sp.ID().Bytes(), msg.Addr)
		po := chunk.Proximity(d.kad.BaseAddr(), msg.Addr)
		depth := d.kad.NeighbourhoodDepth()
		// chunks within the area of responsibility should always sync
		// https://github.com/ethersphere/go-ethereum/pull/1282#discussion_r269406125
		if po >= depth || peerPO < po {
			mode = chunk.ModePutSync
		} else {
			// do not sync if peer that is sending us a chunk is closer to the chunk then we are
			mode = chunk.ModePutRequest
		}
	case *ChunkDeliveryMsgSyncing:
		msg = (*ChunkDeliveryMsg)(r)
		mode = chunk.ModePutSync
	case *ChunkDeliveryMsg:
		msg = r
		mode = chunk.ModePutSync
	}

	log.Trace("handle.chunk.delivery", "ref", msg.Addr, "from peer", sp.ID())

	go func() {
		defer osp.Finish()

		msg.peer = sp
		log.Trace("handle.chunk.delivery", "put", msg.Addr)
		_, err := d.netStore.Put(ctx, mode, storage.NewChunk(msg.Addr, msg.SData))
		if err != nil {
			if err == storage.ErrChunkInvalid {
				// we removed this log because it spams the logs
				// TODO: Enable this log line
				// log.Warn("invalid chunk delivered", "peer", sp.ID(), "chunk", msg.Addr, )
				msg.peer.Drop()
			}
		}
		log.Trace("handle.chunk.delivery", "done put", msg.Addr, "err", err)
	}()
	return nil
}

func (d *Delivery) Close() {
	d.kad.CloseNeighbourhoodDepthC()
	d.kad.CloseAddrCountC()
	close(d.quit)
}

// RequestFromPeers sends a chunk retrieve request to a peer
// The closest peer that hasn't already been sent to is chosen
func (d *Delivery) RequestFromPeers(ctx context.Context, req *storage.Request, localID enode.ID) (*enode.ID, error) {
	metrics.GetOrRegisterCounter("delivery.requestfrompeers", nil).Inc(1)

	var sp *Peer

	var err error

	depth := d.kad.NeighbourhoodDepth()

	d.kad.EachConn(req.Addr[:], 255, func(p *network.Peer, po int) bool {
		id := p.ID()

		// skip light nodes
		if p.LightNode {
			return true
		}

		// do not send request back to peer who asked us. maybe merge with SkipPeer at some point
		if req.Origin.String() == id.String() {
			return true
		}

		// skip peers that we have already tried
		if req.SkipPeer(id.String()) {
			log.Trace("Delivery.RequestFromPeers: skip peer", "peer", id, "ref", req.Addr.String())
			return true
		}

		// if origin is farther away from req.Addr and origin is not in our depth
		prox := chunk.Proximity(req.Addr, d.kad.BaseAddr())
		// proximity between the req.Addr and our base addr
		if po < depth && prox >= depth {
			log.Trace("Delivery.RequestFromPeers: skip peer because depth", "po", po, "depth", depth, "peer", id, "ref", req.Addr.String())

			err = fmt.Errorf("not going outside of depth; ref=%s po=%v depth=%v prox=%v", req.Addr.String(), po, depth, prox)
			return false
		}

		sp = d.getPeer(id)

		// sp is nil, when we encounter a peer that is not registered for delivery, i.e. doesn't support the `stream` protocol
		if sp == nil {
			return true
		}

		return false
	})

	if err != nil {
		log.Error(err.Error())
		return nil, err
	}

	if sp == nil {
		return nil, errors.New("no peer found") // TODO: maybe clear the peers to skip and try again, or return a failure?
	}

	// setting this value in the context creates a new span that can persist across the sendpriority queue and the network roundtrip
	// this span will finish only when delivery is handled (or times out)
	r := &RetrieveRequestMsg{
		Addr:      req.Addr,
		HopCount:  req.HopCount + 1,
		SkipCheck: true, // this has something to do with old syncing
	}
	log.Trace("sending retrieve request", "ref", r.Addr, "peer", sp.ID().String(), "origin", localID)
	err = sp.Send(ctx, r)
	if err != nil {
		log.Error(err.Error())
		return nil, err
	}

	spID := sp.ID()
	return &spID, nil
}
