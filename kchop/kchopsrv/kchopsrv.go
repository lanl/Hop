// Copyright 2015 The Hop Authors.  All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package main

import (
	"flag"
	"fmt"
	"hop"
	"hop/rmt"
	"hop/rmt/hopsrv"
	"hop/kchop"
	"time"
)

var proto = flag.String("proto", "tcp", "connection protocol")
var addr = flag.String("addr", ":5004", "network address")
var debug = flag.Int("d", 0, "debuglevel")
var logsz = flag.Int("l", 2048, "log size")
var dbname = flag.String("db", "", "database name")

func main() {
	flag.Parse()
	if *dbname == "" {
		fmt.Printf("Error: missing database name\n")
		return
	}

	h, err := kchop.NewKCHop(*dbname)
	if err != nil {
		fmt.Printf("Error: %v\n", err)
		return
	}

	rmtsrv := new(hopsrv.Srv)
	rmtsrv.Log = hop.NewLogger(*logsz)
	rmtsrv.Debuglevel = *debug
	if !rmtsrv.Start(h) {
		fmt.Printf("Error: can't start the server\n")
		return
	}

	rmtsrv.Id = "KCHop"
	laddr, err := rmt.Listen(*proto, *addr, rmtsrv)
	if err != nil {
		goto error
	}

	fmt.Printf("Listening on %v\n", laddr)
	for {
		time.Sleep(1000 * time.Millisecond)
	}
	return

error:
	fmt.Printf("Error: %s\n", err)
}
