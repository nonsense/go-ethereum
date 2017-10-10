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
	"encoding/json"
	"flag"
	"fmt"
	"io/ioutil"
	"math/rand"
	"net/http"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ethereum/go-ethereum/log"
	"github.com/ethereum/go-ethereum/p2p/discover"
	"github.com/ethereum/go-ethereum/p2p/simulations/adapters"
	"github.com/ethereum/go-ethereum/rpc"
	metrics "github.com/rcrowley/go-metrics"
)

var (
	nodecount = flag.Int("nodecount", 16, "number of nodes")
	msgs      = flag.Int("msgs", 5000, "numner of messages to send")
	adapter   = flag.String("adapter", "sock", "adapter type (sim, sock, exec)")
)

// setup services
var services = adapters.Services{
	"multiply": newMultiplyService,
}

func init() {
	flag.Parse()

	adapters.RegisterServices(services)

	// configure logger
	loglevel := log.LvlTrace
	//loglevel := log.LvlDebug
	//loglevel := log.LvlInfo
	//loglevel := log.LvlCrit

	hs := log.StreamHandler(os.Stdout, log.TerminalFormat(true))
	hf := log.LvlFilterHandler(loglevel, hs)
	log.PrintOrigins(true)
	log.Root().SetHandler(hf)
}

func TestSimpleNetwork(t *testing.T) {
	go func() {
		fmt.Println(http.ListenAndServe("localhost:6060", nil))
	}()

	t.Logf("simple network test: %d, %d, %s", *nodecount, *msgs, *adapter)

	nodes := make([]discover.NodeID, *nodecount)
	rpcs := make(map[discover.NodeID]*rpc.Client, *nodecount)

	trigger := make(chan discover.NodeID)

	// setup adapter
	var a adapters.NodeAdapter
	if *adapter == "exec" {
		dirname, err := ioutil.TempDir(".", "")
		if err != nil {
			t.Fatal(err)
		}
		a = adapters.NewExecAdapter(dirname)
	} else if *adapter == "sock" {
		a = adapters.NewSocketAdapter(services)
	} else if *adapter == "sim" {
		a = adapters.NewSimAdapter(services)
	}

	// setup simulations network
	net := NewNetwork(a, &NetworkConfig{
		ID:             "0",
		DefaultService: "multiply",
	})
	defer net.Shutdown()

	// load network snapshot
	f, err := os.Open(fmt.Sprintf("testdata/snapshot_%d.json", *nodecount))
	if err != nil {
		t.Fatal(err)
	}
	jsonbyte, err := ioutil.ReadAll(f)
	if err != nil {
		t.Fatal(err)
	}
	var snap Snapshot
	err = json.Unmarshal(jsonbyte, &snap)
	if err != nil {
		t.Fatal(err)
	}
	err = net.Load(&snap)
	if err != nil {
		t.Fatal(err)
	}

	time.Sleep(100 * time.Millisecond)

	// triggers
	triggerChecks := func(trigger chan discover.NodeID, id discover.NodeID, rpcclient *rpc.Client) error {
		eventC := make(chan int64)
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		sub, err := rpcclient.Subscribe(ctx, "multiply", eventC, "events")
		if err != nil {
			t.Fatal(err)
		}
		go func() {
			defer sub.Unsubscribe()
			for {
				select {
				case <-eventC:
					metrics.GetOrRegisterCounter("trigger", nil).Inc(1)
					trigger <- id
				case <-sub.Err():
					return
				}
			}
		}()
		return nil
	}

	for i, nod := range net.GetNodes() {
		nodes[i] = nod.ID()
		rpcs[nodes[i]], err = nod.Client()
		if err != nil {
			t.Fatal(err)
		}

		err = triggerChecks(trigger, nodes[i], rpcs[nodes[i]])
		if err != nil {
			t.Fatal(err)
		}
	}

	// correct results == sent messages
	var correctResults uint64
	var wg sync.WaitGroup
	wg.Add(*msgs)
	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	// send messages
	for i := 0; i < *msgs; i++ {
		sendnodeidx := rand.Intn(*nodecount)
		recvnodeidx := rand.Intn(*nodecount - 1)
		if recvnodeidx >= sendnodeidx {
			recvnodeidx++
		}

		go func() {
			// call some RPC methods
			var resp int64
			req := rand.Int63n(100000)
			if err := rpcs[nodes[sendnodeidx]].Call(&resp, "multiply_multiplyByThree", req); err != nil {
				//t.Fatalf("error calling RPC method: %s", err)
				wg.Done()
				return
			}
			if req != resp/3 {
				//t.Fatalf("faulty rpc handler, requested: %d, got: %d", req, resp)
				wg.Done()
				return
			}

			atomic.AddUint64(&correctResults, 1)
			wg.Done()
		}()
	}

	// count triggers
	finalmsgcount := 0
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
outer:
	for i := 0; i < *msgs; i++ {
		select {
		case <-trigger:
			finalmsgcount++
		case <-ctx.Done():
			log.Warn("timeout")
			break outer
		}
	}

	// wait for correct message counters
	select {
	case <-done:
	case <-time.After(500 * time.Millisecond):
	}

	correctResultsFinal := atomic.LoadUint64(&correctResults)

	t.Logf("%d of %d messages received", finalmsgcount, *msgs)
	t.Logf("%d of correct results received", correctResultsFinal)

	if finalmsgcount != *msgs {
		t.Fatalf("%d messages were not received", *msgs-finalmsgcount)
	}

	if int(correctResultsFinal) != *msgs {
		t.Fatalf("%d correct results were not received", *msgs-int(correctResultsFinal))
	}

	metrics.GraphiteOnce(gc)
}
