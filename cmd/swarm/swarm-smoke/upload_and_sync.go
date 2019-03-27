// Copyright 2018 The go-ethereum Authors
// This file is part of go-ethereum.
//
// go-ethereum is free software: you can redistribute it and/or modify
// it under the terms of the GNU General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// go-ethereum is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU General Public License for more details.
//
// You should have received a copy of the GNU General Public License
// along with go-ethereum. If not, see <http://www.gnu.org/licenses/>.

package main

import (
	"bytes"
	"context"
	"fmt"
	"io/ioutil"
	"math/rand"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/ethereum/go-ethereum/log"
	"github.com/ethereum/go-ethereum/metrics"
	"github.com/ethereum/go-ethereum/rpc"
	"github.com/ethereum/go-ethereum/swarm/api"
	"github.com/ethereum/go-ethereum/swarm/storage"
	"github.com/ethereum/go-ethereum/swarm/testutil"

	cli "gopkg.in/urfave/cli.v1"
)

func uploadAndSyncCmd(ctx *cli.Context) error {
	// use input seed if it has been set
	if inputSeed != 0 {
		seed = inputSeed
	}

	randomBytes := testutil.RandomBytes(seed, filesize*1000)

	errc := make(chan error)

	go func() {
		errc <- uploadAndSync(ctx, randomBytes)
	}()

	var err error
	select {
	case err = <-errc:
		if err != nil {
			metrics.GetOrRegisterCounter(fmt.Sprintf("%s.fail", commandName), nil).Inc(1)
		}
	case <-time.After(time.Duration(timeout) * time.Second):
		metrics.GetOrRegisterCounter(fmt.Sprintf("%s.timeout", commandName), nil).Inc(1)

		err = fmt.Errorf("timeout after %v sec", timeout)
	}

	// trigger debug functionality on randomBytes
	e := trackChunks(randomBytes[:])
	if e != nil {
		log.Error(e.Error())
	}

	return err
}

func trackChunks(testData []byte) error {
	addrs, err := getAllRefs(testData)
	if err != nil {
		return err
	}

	for i, ref := range addrs {
		log.Debug(fmt.Sprintf("ref %d", i), "ref", ref)
	}

	for _, host := range hosts {
		httpHost := fmt.Sprintf("ws://%s:%d", host, 8546)

		hostChunks := []string{}

		rpcClient, err := rpc.Dial(httpHost)
		if err != nil {
			log.Error("error dialing host", "err", err, "host", httpHost)
			continue
		}

		var hasInfo []api.HasInfo
		err = rpcClient.Call(&hasInfo, "bzz_has", addrs)
		if err != nil {
			log.Error("error calling rpc client", "err", err, "host", httpHost)
			continue
		}

		yes, no := 0, 0
		for _, info := range hasInfo {
			if info.Has {
				hostChunks = append(hostChunks, "1")
				yes++
			} else {
				hostChunks = append(hostChunks, "0")
				no++
			}
		}

		if no == 0 {
			log.Info("host reported to have all chunks", "host", host)
		}

		log.Debug("chunks", "chunks", strings.Join(hostChunks, ""), "yes", yes, "no", no, "host", host)
	}
	return nil
}

func getAllRefs(testData []byte) (storage.AddressCollection, error) {
	datadir, err := ioutil.TempDir("", "chunk-debug")
	if err != nil {
		return nil, fmt.Errorf("unable to create temp dir: %v", err)
	}
	defer os.RemoveAll(datadir)
	fileStore, err := storage.NewLocalFileStore(datadir, make([]byte, 32))
	if err != nil {
		return nil, err
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(trackTimeout)*time.Second)
	defer cancel()

	reader := bytes.NewReader(testData)
	return fileStore.GetAllReferences(ctx, reader, false)
}

func uploadAndSync(c *cli.Context, randomBytes []byte) error {
	log.Info("uploading to "+httpEndpoint(hosts[0])+" and syncing", "seed", seed)

	t1 := time.Now()
	hash, err := upload(randomBytes, httpEndpoint(hosts[0]))
	if err != nil {
		log.Error(err.Error())
		return err
	}
	t2 := time.Since(t1)
	metrics.GetOrRegisterResettingTimer("upload-and-sync.upload-time", nil).Update(t2)

	fhash, err := digest(bytes.NewReader(randomBytes))
	if err != nil {
		log.Error(err.Error())
		return err
	}

	log.Info("uploaded successfully", "hash", hash, "took", t2, "digest", fmt.Sprintf("%x", fhash))

	var wg sync.WaitGroup
	wg.Add(len(hosts))
	for i := 0; i < len(hosts); i++ {
		i := i
		go func(idx int) {
			waitUntilSyncingStops(wsEndpoint(hosts[idx]))
			wg.Done()
		}(i)
	}
	wg.Wait()

	log.Debug("chunks before fetch attempt", "hash", hash)

	err = trackChunks(randomBytes)
	if err != nil {
		log.Error(err.Error())
	}

	randIndex := 1 + rand.Intn(len(hosts)-1)

	for {
		start := time.Now()
		err := fetch(hash, httpEndpoint(hosts[randIndex]), fhash, "")
		if err != nil {
			time.Sleep(2 * time.Second)
			continue
		}
		ended := time.Since(start)

		metrics.GetOrRegisterResettingTimer("upload-and-sync.single.fetch-time", nil).Update(ended)
		log.Info("fetch successful", "took", ended, "endpoint", httpEndpoint(hosts[randIndex]))
		break
	}

	return nil
}

func waitUntilSyncingStops(wsHost string) {
	defer metrics.GetOrRegisterResettingTimer("upload-and-sync.single.wait-for-sync", nil).UpdateSince(time.Now())

	var rpcClient *rpc.Client
	var err error

	for {
		rpcClient, err = rpc.Dial(wsHost)
		if err != nil {
			log.Error("error dialing host", "err", err)

			time.Sleep(5 * time.Second)
			continue
		}
		break
	}

	for {
		var isSyncing bool
		err = rpcClient.Call(&isSyncing, "bzz_isSyncing")
		if err != nil {
			log.Error("error calling host for isSyncing", "err", err)

			time.Sleep(5 * time.Second)
			continue
		}

		log.Info("isSyncing result", "host", wsHost, "isSyncing", isSyncing)
		if isSyncing {
			time.Sleep(5 * time.Second)
			continue
		} else {
			return
		}
	}
}
