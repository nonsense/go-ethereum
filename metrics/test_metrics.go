// Copyright 2015 The go-ethereum Authors
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

// Package metrics provides general system and process level metrics collection.
package metrics

import (
	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/metrics"
	"github.com/go-kit/kit/metrics/statsd"
	//	"github.com/rcrowley/go-metrics"
)

//var gc metrics.GraphiteConfig
var m *statsd.Statsd

var (
	RpcClientSubscribe metrics.Counter
	RpcClientWrite     metrics.Counter
	RpcClientSend      metrics.Counter
)

func SetupTestMetrics(namespace string) {
	m = statsd.New("pss.", log.NewNopLogger())
	RpcClientSubscribe = m.NewCounter("rpc.client.subscribe", 1.0)
	RpcClientSend = m.NewCounter("rpc.client.send", 1.0)
	RpcClientWrite = m.NewCounter("rpc.client.write", 1.0)

	//setupStatsdReporter()
	//setupGraphiteReporter(namespace)
}

func ShutdownTestMetrics() {
	//metrics.GraphiteOnce(gc)
}

//func setupGraphiteReporter(namespace string) {
//addr, _ := net.ResolveTCPAddr("tcp", "127.0.0.1:2003")

//gc = metrics.GraphiteConfig{
//Addr:          addr,
//Registry:      metrics.DefaultRegistry,
//FlushInterval: 100 * time.Millisecond,
//DurationUnit:  time.Nanosecond,
//Prefix:        namespace,
//Percentiles:   []float64{0.5, 0.75, 0.95, 0.99, 0.999},
//}

//go metrics.GraphiteWithConfig(gc)
//}

//func setupStatsdReporter() {
//reporter, err := metrics.NewStatsdReporter(
//metrics.DefaultRegistry,
//"127.0.0.1:8125", // DogStatsD UDP address
//time.Second*1,    // Update interval
//)
//if err != nil {
//panic(err)
//}

//// configure a prefix, and send the EC2 availability zone as a tag with
//// every metric.
//reporter.Client.Namespace = "pss."
////reporter.Client.Tags = append(reporter.Client.Tags, "us-east-1a")

//go reporter.Flush()
//}
