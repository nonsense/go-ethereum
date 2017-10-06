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
	"fmt"
	"io/ioutil"
	"math/rand"
	"net/http"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/ethereum/go-ethereum/log"
	"github.com/ethereum/go-ethereum/p2p/discover"
	"github.com/ethereum/go-ethereum/p2p/simulations/adapters"
	"github.com/ethereum/go-ethereum/rpc"
)

func init() {
	// configure logger
	//loglevel := log.LvlTrace
	loglevel := log.LvlDebug
	//loglevel := log.LvlInfo

	hs := log.StreamHandler(os.Stdout, log.TerminalFormat(true))
	hf := log.LvlFilterHandler(loglevel, hs)
	log.PrintOrigins(true)
	log.Root().SetHandler(hf)
}

func TestSimpleNetwork(t *testing.T) {
	go func() {
		fmt.Println(http.ListenAndServe("localhost:6060", nil))
	}()

	t.Run("8/50/exec", testSimpleNetwork)
}

func testSimpleNetwork(t *testing.T) {
	nodecount, msgcount, adapterType := parseName(t.Name())

	log.Info("simple network test", "nodecount", nodecount, "msgcount", msgcount)

	nodes := make([]discover.NodeID, nodecount)
	rpcs := make(map[discover.NodeID]*rpc.Client, nodecount)

	trigger := make(chan discover.NodeID)

	// setup services
	services := adapters.Services{
		"test": newTestService,
	}

	// setup adapter
	var adapter adapters.NodeAdapter
	if adapterType == "exec" {
		dirname, err := ioutil.TempDir(".", "")
		if err != nil {
			t.Fatal(err)
		}
		adapters.RegisterServices(services)
		adapter = adapters.NewExecAdapter(dirname)
	} else if adapterType == "sock" {
		adapter = adapters.NewSocketAdapter(services)
	} else if adapterType == "sim" {
		adapter = adapters.NewSimAdapter(services)
	}

	// setup simulations network
	net := NewNetwork(adapter, &NetworkConfig{
		ID:             "0",
		DefaultService: "test",
	})
	defer net.Shutdown()

	// load network snapshot
	f, err := os.Open(fmt.Sprintf("testdata/snapshot_%d.json", nodecount))
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

	// triggers
	triggerChecks := func(trigger chan discover.NodeID, id discover.NodeID, rpcclient *rpc.Client) error {
		eventC := make(chan int64)
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		sub, err := rpcclient.Subscribe(ctx, "test", eventC, "events")
		if err != nil {
			t.Fatal(err)
		}
		go func() {
			defer sub.Unsubscribe()
			for {
				select {
				case <-eventC:
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

	// send messages
	for i := 0; i < int(msgcount); i++ {
		sendnodeidx := rand.Intn(int(nodecount))
		recvnodeidx := rand.Intn(int(nodecount - 1))
		if recvnodeidx >= sendnodeidx {
			recvnodeidx++
		}

		go func() {
			// call some RPC methods
			if err := rpcs[nodes[sendnodeidx]].Call(nil, "test_add", 10); err != nil {
				t.Fatalf("error calling RPC method: %s", err)
			}
			var result int64
			if err := rpcs[nodes[sendnodeidx]].Call(&result, "test_get"); err != nil {
				t.Fatalf("error calling RPC method: %s", err)
			}
			if result%10 != 0 {
				t.Fatalf("expected result to be 10 or 20, got %d", result)
			}
		}()
	}

	// count triggers
	finalmsgcount := 0
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
	defer cancel()
outer:
	for i := 0; i < int(msgcount); i++ {
		select {
		case <-trigger:
			finalmsgcount++
		case <-ctx.Done():
			log.Warn("timeout")
			break outer
		}
	}

	t.Logf("%d of %d messages received", finalmsgcount, msgcount)

	if finalmsgcount != int(msgcount) {
		t.Fatalf("%d messages were not received", int(msgcount)-finalmsgcount)
	}

}

func parseName(name string) (int64, int64, string) {
	paramstring := strings.Split(name, "/")

	nodecount, _ := strconv.ParseInt(paramstring[1], 10, 0)
	msgcount, _ := strconv.ParseInt(paramstring[2], 10, 0)
	adapter := paramstring[3]

	return nodecount, msgcount, adapter
}
