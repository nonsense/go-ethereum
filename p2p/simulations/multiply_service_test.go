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

	"github.com/ethereum/go-ethereum/event"
	"github.com/ethereum/go-ethereum/node"
	"github.com/ethereum/go-ethereum/p2p"
	"github.com/ethereum/go-ethereum/p2p/discover"
	"github.com/ethereum/go-ethereum/p2p/simulations/adapters"
	"github.com/ethereum/go-ethereum/rpc"
	metrics "github.com/rcrowley/go-metrics"
)

// multiplyService implements the node.Service interface and provides protocols
// and APIs which are useful for testing nodes in a simulation network
type multiplyService struct {
	id discover.NodeID
}

func newMultiplyService(ctx *adapters.ServiceContext) (node.Service, error) {
	svc := &multiplyService{
		id: ctx.Config.ID,
	}
	return svc, nil
}

func (t *multiplyService) Protocols() []p2p.Protocol {
	return []p2p.Protocol{
		{
			Name:    "multiply",
			Version: 1,
			Length:  3,
			Run:     t.RunTest,
		},
	}
}

func (t *multiplyService) APIs() []rpc.API {
	return []rpc.API{{
		Namespace: "multiply",
		Version:   "1.0",
		Service:   &MultiplyAPI{},
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

	// block until the peer is dropped
	for {
		_, err := rw.ReadMsg()
		if err != nil {
			return err
		}
	}
}

// MultiplyAPI provides a test API to:
// * multiply number by 3
// * subscribe to multiply API call events
type MultiplyAPI struct {
	feed event.Feed
}

func (t *MultiplyAPI) MultiplyByThree(delta int64) int64 {
	t.feed.Send(delta)
	return delta * 3
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
				//case _ = <-events:
				metrics.GetOrRegisterCounter("multiply_event", nil).Inc(1)
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
