// Copyright 2013 The Hop Authors.  All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package main

import (
	"flag"
	"fmt"
	"hop"
	"hop/rmt"
	"hop/rmt/hopsrv"
	"hop/shop"
	"log"
	"time"
)

var proto = flag.String("proto", "tcp", "connection protocol")
var addr = flag.String("addr", ":5004", "network address")
var debug = flag.Int("d", 0, "debuglevel")
var logsz = flag.Int("l", 2048, "log size")

func main() {
	flag.Parse()
	sh := shop.NewSHop()
	sh.AddEntry("#/id", []byte("SHop"), nil)
	rmtsrv := new(hopsrv.Srv)
	rmtsrv.Log = hop.NewLogger(*logsz)
	rmtsrv.Debuglevel = *debug
	if !rmtsrv.Start(sh) {
		log.Println(fmt.Sprintf("Error: can't start the server\n"))
		return
	}

	rmtsrv.Id = "SHop"
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
	log.Println(fmt.Sprintf("Error: %s", err))
}
