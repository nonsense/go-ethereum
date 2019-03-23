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

package network

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/ethereum/go-ethereum/p2p/enode"
	"github.com/ethereum/go-ethereum/swarm/storage"
)

const (
	// maximum number of forwarded requests (hops), to make sure requests are not
	// forwarded forever in peer loops
	maxHopCount uint8 = 20
)

// Time to consider peer to be skipped.
// Also used in stream delivery.
var FailedPeerSkipDelay = 10 * time.Second

var RequestTimeout = 10 * time.Second

var FetcherTimeout = 10 * time.Second

var SearchTimeout = 1 * time.Second

var RemoteGet func(ctx context.Context, req *Request) (*enode.ID, error)

type Request struct {
	Addr        storage.Address // chunk address
	PeersToSkip sync.Map        // peers not to request chunk from (only makes sense if source is nil)
	HopCount    uint8           // number of forwarded requests (hops)
}

func RemoteFetch(ctx context.Context, ref storage.Address, fi *storage.FetcherItem) error {
	req := NewRequest(ref)

	// initial call to search for chunk
	currentPeer, err := RemoteGet(ctx, req)
	if err != nil {
		return err
	}

	// add peer to the set of peers to skip from now
	req.PeersToSkip.Store(currentPeer.String(), time.Now())

	// while we haven't timed-out, and while we don't have a chunk,
	// iterate over peers and try to find a chunk
	gt := time.After(FetcherTimeout)
	for {
		select {
		case <-fi.Delivered:
			return nil
		case <-time.After(SearchTimeout):
			currentPeer, err := RemoteGet(context.TODO(), req)
			if err != nil {
				return err
			}
			// add peer to the set of peers to skip from now
			req.PeersToSkip.Store(currentPeer.String(), time.Now())
		case <-gt:
			return errors.New("chunk couldnt be retrieved from remote nodes")
		}
	}
}

// NewRequest returns a new instance of Request based on chunk address skip check and
// a map of peers to skip.
func NewRequest(addr storage.Address) *Request {
	return &Request{
		Addr:        addr,
		PeersToSkip: sync.Map{},
	}
}

// SkipPeer returns if the peer with nodeID should not be requested to deliver a chunk.
// Peers to skip are kept per Request and for a time period of FailedPeerSkipDelay.
func (r *Request) SkipPeer(nodeID string) bool {
	val, ok := r.PeersToSkip.Load(nodeID)
	if !ok {
		return false
	}
	t, ok := val.(time.Time)
	if ok && time.Now().After(t.Add(FailedPeerSkipDelay)) {
		r.PeersToSkip.Delete(nodeID)
		return false
	}
	return true
}
