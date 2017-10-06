// Copyright 2017 The go-ethereum Authors
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

package simulations

import (
	"context"
	"sync"
	"sync/atomic"

	"github.com/ethereum/go-ethereum/event"
	"github.com/ethereum/go-ethereum/node"
	"github.com/ethereum/go-ethereum/p2p"
	"github.com/ethereum/go-ethereum/p2p/discover"
	"github.com/ethereum/go-ethereum/p2p/simulations/adapters"
	"github.com/ethereum/go-ethereum/rpc"
)

// multiplyService implements the node.Service interface and provides protocols
// and APIs which are useful for testing nodes in a simulation network
type multiplyService struct {
	id discover.NodeID

	// peerCount is incremented once a peer handshake has been performed
	peerCount int64

	peers    map[discover.NodeID]*testPeer
	peersMtx sync.Mutex

	// state stores []byte which is used to test creating and loading
	// snapshots
	state atomic.Value
}

func newMultiplyService(ctx *adapters.ServiceContext) (node.Service, error) {
	svc := &multiplyService{
		id:    ctx.Config.ID,
		peers: make(map[discover.NodeID]*testPeer),
	}
	svc.state.Store(ctx.Snapshot)
	return svc, nil
}

func (t *multiplyService) peer(id discover.NodeID) *testPeer {
	t.peersMtx.Lock()
	defer t.peersMtx.Unlock()
	if peer, ok := t.peers[id]; ok {
		return peer
	}
	peer := &testPeer{
		testReady: make(chan struct{}),
		dumReady:  make(chan struct{}),
	}
	t.peers[id] = peer
	return peer
}

func (t *multiplyService) Protocols() []p2p.Protocol {
	return []p2p.Protocol{
		{
			Name:    "test",
			Version: 1,
			Length:  3,
			Run:     t.RunTest,
		},
		{
			Name:    "dum",
			Version: 1,
			Length:  1,
			Run:     t.RunDum,
		},
		{
			Name:    "prb",
			Version: 1,
			Length:  1,
			Run:     t.RunPrb,
		},
	}
}

func (t *multiplyService) APIs() []rpc.API {
	return []rpc.API{{
		Namespace: "test",
		Version:   "1.0",
		Service: &MultiplyAPI{
			state:     &t.state,
			peerCount: &t.peerCount,
		},
	}}
}

func (t *multiplyService) Start(server *p2p.Server) error {
	return nil
}

func (t *multiplyService) Stop() error {
	return nil
}

// handshake performs a peer handshake by sending and expecting an empty
// message with the given code
func (t *multiplyService) handshake(rw p2p.MsgReadWriter, code uint64) error {
	errc := make(chan error, 2)
	go func() { errc <- p2p.Send(rw, code, struct{}{}) }()
	go func() { errc <- p2p.ExpectMsg(rw, code, struct{}{}) }()
	for i := 0; i < 2; i++ {
		if err := <-errc; err != nil {
			return err
		}
	}
	return nil
}

func (t *multiplyService) RunTest(p *p2p.Peer, rw p2p.MsgReadWriter) error {
	peer := t.peer(p.ID())

	// perform three handshakes with three different message codes,
	// used to test message sending and filtering
	if err := t.handshake(rw, 2); err != nil {
		return err
	}
	if err := t.handshake(rw, 1); err != nil {
		return err
	}
	if err := t.handshake(rw, 0); err != nil {
		return err
	}

	// close the testReady channel so that other protocols can run
	close(peer.testReady)

	// track the peer
	atomic.AddInt64(&t.peerCount, 1)
	defer atomic.AddInt64(&t.peerCount, -1)

	// block until the peer is dropped
	for {
		_, err := rw.ReadMsg()
		if err != nil {
			return err
		}
	}
}

func (t *multiplyService) RunDum(p *p2p.Peer, rw p2p.MsgReadWriter) error {
	peer := t.peer(p.ID())

	// wait for the test protocol to perform its handshake
	<-peer.testReady

	// perform a handshake
	if err := t.handshake(rw, 0); err != nil {
		return err
	}

	// close the dumReady channel so that other protocols can run
	close(peer.dumReady)

	// block until the peer is dropped
	for {
		_, err := rw.ReadMsg()
		if err != nil {
			return err
		}
	}
}
func (t *multiplyService) RunPrb(p *p2p.Peer, rw p2p.MsgReadWriter) error {
	peer := t.peer(p.ID())

	// wait for the dum protocol to perform its handshake
	<-peer.dumReady

	// perform a handshake
	if err := t.handshake(rw, 0); err != nil {
		return err
	}

	// block until the peer is dropped
	for {
		_, err := rw.ReadMsg()
		if err != nil {
			return err
		}
	}
}

func (t *multiplyService) Snapshot() ([]byte, error) {
	return t.state.Load().([]byte), nil
}

// MultiplyAPI provides a test API to:
// * get the peer count
// * get and set an arbitrary state byte slice
// * get and increment a counter
// * subscribe to counter increment events
type MultiplyAPI struct {
	state     *atomic.Value
	peerCount *int64
	feed      event.Feed
}

func (t *MultiplyAPI) PeerCount() int64 {
	return atomic.LoadInt64(t.peerCount)
}

func (t *MultiplyAPI) MultiplyByThree(delta int64) int64 {
	t.feed.Send(delta)
	return delta * 3
}

func (t *MultiplyAPI) GetState() []byte {
	return t.state.Load().([]byte)
}

func (t *MultiplyAPI) SetState(state []byte) {
	t.state.Store(state)
}

func (t *MultiplyAPI) Events(ctx context.Context) (*rpc.Subscription, error) {
	notifier, supported := rpc.NotifierFromContext(ctx)
	if !supported {
		return nil, rpc.ErrNotificationsUnsupported
	}

	rpcSub := notifier.CreateSubscription()

	go func() {
		events := make(chan int64)
		sub := t.feed.Subscribe(events)
		defer sub.Unsubscribe()

		for {
			select {
			case event := <-events:
				notifier.Notify(rpcSub.ID, event)
			case <-sub.Err():
				return
			case <-rpcSub.Err():
				return
			case <-notifier.Closed():
				return
			}
		}
	}()

	return rpcSub, nil
}
