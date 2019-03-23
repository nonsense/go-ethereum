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

package storage

import (
	"context"
	"errors"
	"sync"

	"github.com/ethereum/go-ethereum/swarm/log"
	"github.com/syndtr/goleveldb/leveldb"
	"golang.org/x/sync/singleflight"
)

var RemoteFetch func(ref Address, fi *FetcherItem) error

// FetcherItem are stored in fetchers map and signal to all interested parties if a given chunk is delivered
// the mutex controls who closes the channel, and make sure we close the channel only once
type FetcherItem struct {
	Delivered chan struct{} // when closed, it means that the chunk is delivered
	Mu        sync.Mutex
}

func (fi *FetcherItem) SafeClose() {
	fi.Mu.Lock()
	_, ok := <-fi.Delivered
	if !ok {
		close(fi.Delivered)
	}
	fi.Mu.Unlock()
}

// NetStore is an extension of LocalStore
// it implements the ChunkStore interface
// on request it initiates remote cloud retrieval
type NetStore struct {
	store      *LocalStore
	fetchers   sync.Map
	fetchersMu sync.Mutex
}

// NewNetStore creates a new NetStore object using the given local store. newFetchFunc is a
// constructor function that can create a fetch function for a specific chunk address.
func NewNetStore(store *LocalStore) *NetStore {
	return &NetStore{
		store:      store,
		fetchers:   sync.Map{},
		fetchersMu: sync.Mutex{},
	}
}

// Put stores a chunk in localstore, and delivers to all requestor peers using the fetcher stored in
// the fetchers cache
func (n *NetStore) Put(ctx context.Context, chunk Chunk) error {
	// put the chunk to the localstore, there should be no error
	err := n.store.Put(ctx, chunk)
	if err != nil {
		return err
	}

	// notify RemoteGet about a chunk being stored
	fi, ok := n.fetchers.Load(chunk.Address().String())
	if ok {
		// we need SafeClose, because it is possible for a chunk to both be
		// delivered through syncing and through a retrieve request
		fii := fi.(FetcherItem)
		fii.SafeClose()
	}

	return nil
}

func (n *NetStore) BinIndex(po uint8) uint64 {
	return n.store.BinIndex(po)
}

func (n *NetStore) Iterator(from uint64, to uint64, po uint8, f func(Address, uint64) bool) error {
	return n.store.Iterator(from, to, po, f)
}

// Close chunk store
func (n *NetStore) Close() {
	n.store.Close()
}

// Get retrieves a chunk
// If it is not found in the LocalStore then it uses RemoteGet to fetch from the network.
func (n *NetStore) Get(ctx context.Context, ref Address) (Chunk, error) {
	chunk, err := n.store.Get(ctx, ref)
	if err != nil {
		// TODO: fix comparison - we should be comparing against leveldb.ErrNotFound, this error should be wrapped.
		if err != ErrChunkNotFound && err != leveldb.ErrNotFound {
			log.Error("got error from LocalStore other than leveldb.ErrNotFound or ErrChunkNotFound", "err", err)
		}

		var requestGroup singleflight.Group

		v, err, _ := requestGroup.Do(ref.String(), func() (interface{}, error) {
			fi, _ := n.fetchers.LoadOrStore(ref.String(), FetcherItem{make(chan struct{}), sync.Mutex{}})
			defer func() {
				n.fetchersMu.Lock()
				n.fetchers.Delete(ref.String())
				n.fetchersMu.Unlock()
			}()

			r := fi.(FetcherItem)

			err := RemoteFetch(ref, &r)
			if err != nil {
				return nil, err
			}

			chunk, err := n.store.Get(ctx, ref)
			if err != nil {
				log.Error(err.Error())
				return nil, errors.New("item should have been in localstore, but it is not")
			}

			return chunk, nil
		})

		if err != nil {
			log.Error(err.Error())
			return nil, err
		}

		return v.(Chunk), nil
	}

	return chunk, nil
}

// Has is the storage layer entry point to query the underlying
// database to return if it has a chunk or not.
func (n *NetStore) Has(ctx context.Context, ref Address) bool {
	return n.store.Has(ctx, ref)
}

func (n *NetStore) HasWithCallback(ctx context.Context, ref Address) (bool, *FetcherItem) {
	n.fetchersMu.Lock()
	defer n.fetchersMu.Unlock()

	if n.store.Has(ctx, ref) {
		return true, nil
	}

	fi, _ := n.fetchers.LoadOrStore(ref.String(), FetcherItem{make(chan struct{}), sync.Mutex{}})
	r := fi.(FetcherItem)
	return false, &r
}
