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
	"hop/kchop"
	"runtime"
	"time"
)

var proto = flag.String("proto", "tcp", "connection protocol")
var addr = flag.String("addr", ":5004", "network address")
var debug = flag.Int("d", 0, "debuglevel")
var logsz = flag.Int("l", 2048, "log size")
var maddr = flag.String("maddr", "", "master address (master if empty)")
var dbname = flag.String("dbname", "", "Kyoto cabinet database name")
var sync = flag.Bool("sync", false, "auto sync")

func main() {
	flag.Parse()
	runtime.GOMAXPROCS(runtime.NumCPU())
	hopclnt.DefaultDebuglevel = *debug

	kchop, err := kchop.NewKCHop(*dbname, *sync)
	if err != nil {
		fmt.Printf("Error: %v\n", err)
		return
	}

	s, err := d2hop.NewD2Hop(*proto, *addr, *maddr, kchop)
	if err != nil {
		fmt.Printf("Error: %v", err)
		return
	}

	s.SetLogger(hop.NewLogger(*logsz))
	s.SetDebugLevel(*debug)
	for {
		time.Sleep(1000 * time.Millisecond)
		kchop.Sync()
	}

	return
}
