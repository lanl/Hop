// Copyright 2013 The Hop Authors.  All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package main

import (
	"flag"
	"fmt"
	"hop"
	"hop/d2hop"
	"hop/rmt/hopclnt"
	"hop/shop"
	"log"
	"runtime"
	"time"
)

var proto = flag.String("proto", "tcp", "connection protocol")
var addr = flag.String("addr", ":5004", "network address")
var debug = flag.Int("d", 0, "debuglevel")
var logsz = flag.Int("l", 2048, "log size")
var maddr = flag.String("maddr", "", "master address (master if empty)")

func main() {
	flag.Parse()
	runtime.GOMAXPROCS(runtime.NumCPU())
	hopclnt.DefaultDebuglevel = *debug

	shop := shop.NewSHop()
	s, err := d2hop.NewD2Hop(*proto, *addr, *maddr, shop)
	if err != nil {
		log.Println(fmt.Sprintf("Error: %s", err))
		return
	}

	s.SetLogger(hop.NewLogger(*logsz))
	s.SetDebugLevel(*debug)
	for {
		time.Sleep(1000 * time.Millisecond)
	}

	return
}
