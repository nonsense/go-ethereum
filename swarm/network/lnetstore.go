// Copyright 2016 The go-ethereum Authors
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

	"github.com/ethereum/go-ethereum/p2p/enode"
	"github.com/ethereum/go-ethereum/swarm/storage"
)

type LNetStore struct {
	store *NetStore
}

func NewLNetStore(store *NetStore) *LNetStore {
	return &LNetStore{
		store: store,
	}
}

func (n *LNetStore) Put(ctx context.Context, chunk storage.Chunk) error {
	return n.store.Put(ctx, chunk)
}

func (n *LNetStore) Has(ctx context.Context, ref storage.Address) bool {
	return n.store.Has(ctx, ref)
}

func (n *LNetStore) Close() {
	n.store.Close()
}

func (n *LNetStore) Get(ctx context.Context, ref storage.Address) (ch storage.Chunk, err error) {
	req := &Request{
		Addr:     ref,
		HopCount: 0,
		Origin:   enode.ID{},
	}

	return n.store.Get(ctx, req)
}
