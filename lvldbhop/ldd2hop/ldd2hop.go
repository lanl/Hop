// Copyright 2015 The Hop Authors.  All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package main

import (
	"flag"
	"fmt"
	"hop"
	"hop/d2hop"
	"hop/rmt/hopclnt"
	"hop/lvldbhop"
	"runtime"
	"time"
)

var proto = flag.String("proto", "tcp", "connection protocol")
var addr = flag.String("addr", ":5004", "network address")
var debug = flag.Int("d", 0, "debuglevel")
var logsz = flag.Int("l", 2048, "log size")
var maddr = flag.String("maddr", "", "master address (master if empty)")
var dbname = flag.String("dbname", "", "Leveldb database name")

func main() {
	flag.Parse()
	runtime.GOMAXPROCS(runtime.NumCPU())
	hopclnt.DefaultDebuglevel = *debug

	ldhop, err := lvldbhop.NewLDHop(*dbname, 50242880, 50242880, 256)
	if err != nil {
		fmt.Printf("Error: %v\n", err)
		return
	}

	s, err := d2hop.NewD2Hop(*proto, *addr, *maddr, ldhop)
	if err != nil {
		fmt.Printf("Error: %v", err)
		return
	}

	s.SetLogger(hop.NewLogger(*logsz))
	s.SetDebugLevel(*debug)
	for {
		time.Sleep(1000 * time.Millisecond)
	}

	return
}
