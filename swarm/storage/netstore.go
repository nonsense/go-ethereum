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
	"fmt"
	"sync"
	"time"

	"github.com/ethereum/go-ethereum/metrics"
	"github.com/ethereum/go-ethereum/p2p/enode"
	"github.com/ethereum/go-ethereum/swarm/chunk"
	"github.com/ethereum/go-ethereum/swarm/log"
	"github.com/ethereum/go-ethereum/swarm/network/timeouts"
	"github.com/ethereum/go-ethereum/swarm/spancontext"
	olog "github.com/opentracing/opentracing-go/log"
	"github.com/syndtr/goleveldb/leveldb"
	"golang.org/x/sync/singleflight"
)

var hopcountHistogram = metrics.GetOrRegisterResettingTimer("hopcount", nil)

// FetcherItem are stored in fetchers map and signal to all interested parties if a given chunk is delivered
// the mutex controls who closes the channel, and make sure we close the channel only once
type FetcherItem struct {
	Delivered chan struct{} // when closed, it means that the chunk this FetcherItem refers to is delivered

	// it is possible for multiple actors to be delivering the same chunk,
	// for example through syncing and through retrieve request. however we want the `Delivered` channel to be closed only
	// once, even if we put the same chunk multiple times in the NetStore.
	once sync.Once

	CreatedAt time.Time // timestamp when the fetcher was created, used for metrics measuring lifetime of fetchers
	CreatedBy string    // who created the fethcer - "request" or "syncing", used for metrics measuring lifecycle of fetchers

	RequestedBySyncer bool // whether we have issued at least once a request through Offered/Wanted hashes flow
}

// NewFetcherItem is a constructor for a FetcherItem
func NewFetcherItem() *FetcherItem {
	return &FetcherItem{make(chan struct{}), sync.Once{}, time.Now(), "", false}
}

// SafeClose signals to interested parties (those waiting for a signal on fi.Delivered) that a chunk is delivered.
// It closes the fi.Delivered channel through the sync.Once object, because it is possible for a chunk to be
// delivered multiple times concurrently.
func (fi *FetcherItem) SafeClose() {
	fi.once.Do(func() {
		close(fi.Delivered)
	})
}

type RemoteGetFunc func(ctx context.Context, req *Request, localID enode.ID) (*enode.ID, error)

// NetStore is an extension of LocalStore
// it implements the ChunkStore interface
// on request it initiates remote cloud retrieval
type NetStore struct {
	chunk.Store
	localID      enode.ID // our local enode - used when issuing RetrieveRequests
	fetchers     sync.Map
	putMu        sync.Mutex
	requestGroup singleflight.Group
	RemoteGet    RemoteGetFunc
}

// NewNetStore creates a new NetStore using the provided chunk.Store and localID of the node.
func NewNetStore(store chunk.Store, localID enode.ID) *NetStore {
	return &NetStore{
		Store:   store,
		localID: localID,
	}
}

// Put stores a chunk in localstore, and delivers to all requestor peers using the fetcher stored in
// the fetchers cache
func (n *NetStore) Put(ctx context.Context, mode chunk.ModePut, ch Chunk) (bool, error) {
	n.putMu.Lock()
	defer n.putMu.Unlock()

	log.Trace("netstore.put", "ref", ch.Address().String(), "mode", mode)

	// put the chunk to the localstore, there should be no error
	exists, err := n.Store.Put(ctx, mode, ch)
	if err != nil {
		return exists, err
	}

	// notify RemoteGet about a chunk being stored
	fi, ok := n.fetchers.Load(ch.Address().String())
	if ok {
		// we need SafeClose, because it is possible for a chunk to both be
		// delivered through syncing and through a retrieve request
		fii := fi.(*FetcherItem)
		fii.SafeClose()
		log.Trace("netstore.put chunk delivered and stored", "ref", ch.Address().String())

		metrics.GetOrRegisterResettingTimer(fmt.Sprintf("netstore.fetcher.lifetime.%s", fii.CreatedBy), nil).UpdateSince(fii.CreatedAt)

		// helper snippet to log if a chunk took way to long to be delivered
		if time.Since(fii.CreatedAt) > 5*time.Second {
			log.Trace("netstore.put slow chunk delivery", "ref", ch.Address().String())
		}

		n.fetchers.Delete(ch.Address().String())
	}

	return exists, nil
}

// Close chunk store
func (n *NetStore) Close() error {
	return n.Store.Close()
}

// Get retrieves a chunk
// If it is not found in the LocalStore then it uses RemoteGet to fetch from the network.
func (n *NetStore) Get(ctx context.Context, mode chunk.ModeGet, req *Request) (Chunk, error) {
	metrics.GetOrRegisterCounter("netstore.get", nil).Inc(1)
	start := time.Now()

	ref := req.Addr

	log.Trace("netstore.get", "ref", ref.String())

	ch, err := n.Store.Get(ctx, mode, ref)
	if err != nil {
		// TODO: fix comparison - we should be comparing against leveldb.ErrNotFound, this error should be wrapped.
		if err != ErrChunkNotFound && err != leveldb.ErrNotFound {
			log.Error("localstore get error", "err", err)
		}

		// keep track of hopCount for informational purposes
		hopcountHistogram.Update(time.Duration(req.HopCount))

		log.Trace("netstore.chunk-not-in-localstore", "ref", ref.String(), "hopCount", req.HopCount)

		v, err, _ := n.requestGroup.Do(ref.String(), func() (interface{}, error) {
			// currently we issue a retrieve request if a fetcher
			// has already been created by a syncer for that particular chunk.
			// so it is possible to
			// have 2 in-flight requests for the same chunk - one by a
			// syncer (offered/wanted/deliver flow) and one from
			// here - retrieve request
			fi, _, ok := n.GetOrCreateFetcherItem(ctx, ref, "request")
			if ok {
				err := n.RemoteFetch(ctx, req, fi)
				if err != nil {
					return nil, err
				}
			}

			ch, err := n.Store.Get(ctx, mode, ref)
			if err != nil {
				log.Error(err.Error(), "ref", ref)
				return nil, errors.New("item should have been in localstore, but it is not")
			}

			// fi could be nil (when ok == false) if the chunk was added to the NetStore between n.store.Get and the call to n.GetOrCreateFetcherItem
			if fi != nil {
				metrics.GetOrRegisterResettingTimer(fmt.Sprintf("fetcher.%s.request", fi.CreatedBy), nil).UpdateSince(start)
			}

			return ch, nil
		})

		if err != nil {
			log.Trace(err.Error(), "ref", ref)
			return nil, err
		}

		c := v.(Chunk)

		log.Trace("netstore.singleflight returned", "ref", ref.String(), "err", err)

		log.Trace("netstore return", "ref", ref.String(), "chunk len", len(c.Data()))

		return c, nil
	}

	ctx, ssp := spancontext.StartSpan(
		ctx,
		"localstore.get")
	defer ssp.Finish()

	return ch, nil
}

func (n *NetStore) RemoteFetch(ctx context.Context, req *Request, fi *FetcherItem) error {
	// while we haven't timed-out, and while we don't have a chunk,
	// iterate over peers and try to find a chunk
	metrics.GetOrRegisterCounter("remote.fetch", nil).Inc(1)

	ref := req.Addr

	for {
		metrics.GetOrRegisterCounter("remote.fetch.inner", nil).Inc(1)

		innerCtx, osp := spancontext.StartSpan(
			ctx,
			"remote.fetch")
		osp.LogFields(olog.String("ref", ref.String()))

		log.Trace("remote.fetch", "ref", ref)

		nctx := context.WithValue(innerCtx, "remote.fetchh", osp)
		currentPeer, err := n.RemoteGet(nctx, req, n.localID)
		if err != nil {
			log.Trace(err.Error(), "ref", ref)
			osp.LogFields(olog.String("err", err.Error()))
			osp.Finish()
			return err
		}
		//osp.LogFields(olog.String("peer", currentPeer.String()))

		// add peer to the set of peers to skip from now
		log.Trace("remote.fetch, adding peer to skip", "ref", ref, "peer", currentPeer.String())
		req.PeersToSkip.Store(currentPeer.String(), time.Now())

		select {
		case <-fi.Delivered:
			log.Trace("remote.fetch, chunk delivered", "ref", ref)

			osp.LogFields(olog.Bool("delivered", true))
			osp.Finish()
			return nil
		case <-time.After(timeouts.SearchTimeout):
			metrics.GetOrRegisterCounter("remote.fetch.timeout.search", nil).Inc(1)

			osp.LogFields(olog.Bool("timeout", true))
			osp.Finish()
			break
		case <-ctx.Done(): // global fetcher timeout
			log.Trace("remote.fetch, fail", "ref", ref)
			metrics.GetOrRegisterCounter("remote.fetch.timeout.global", nil).Inc(1)

			osp.LogFields(olog.Bool("fail", true))
			osp.Finish()
			return errors.New("chunk couldnt be retrieved from remote nodes")
		}
	}
}

// Has is the storage layer entry point to query the underlying
// database to return if it has a chunk or not.
func (n *NetStore) Has(ctx context.Context, ref Address) (bool, error) {
	return n.Store.Has(ctx, ref)
}

// GetOrCreateFetcherItem returns a FetcherItem for a given chunk, if this chunk is not in the LocalStore.
// If the chunk is in the LocalStore, it returns nil for the FetcherItem and ok == false
func (n *NetStore) GetOrCreateFetcherItem(ctx context.Context, ref Address, interestedParty string) (fi *FetcherItem, loaded bool, ok bool) {
	n.putMu.Lock()
	defer n.putMu.Unlock()

	has, err := n.Store.Has(ctx, ref)
	if err != nil {
		log.Error(err.Error())
	}
	if has {
		return nil, false, false
	}

	fi = NewFetcherItem()
	v, loaded := n.fetchers.LoadOrStore(ref.String(), fi)
	log.Trace("netstore.has-with-callback.loadorstore", "ref", ref.String(), "loaded", loaded)
	if loaded {
		fi = v.(*FetcherItem)
	} else {
		fi.CreatedBy = interestedParty
	}

	// if fetcher created by request, but we get a call from syncer, make sure we issue a second request
	if fi.CreatedBy != interestedParty && !fi.RequestedBySyncer {
		fi.RequestedBySyncer = true
		return fi, false, true
	}

	return fi, loaded, true
}
