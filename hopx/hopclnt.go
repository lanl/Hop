// Copyright 2013 The Hop Authors.  All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// +build !dhop
// +build !d2hop
// +build !chord
// +build !cache

package main

import (
	"flag"
	"hop"
	"hop/rmt/hopclnt"
	"strings"
)

var proto = flag.String("proto", "tcp", "connection protocol")
var addr = flag.String("addr", "127.0.0.1:5004", "network address")
var debug = flag.Bool("d", false, "enable debugging (fcalls)")
var debugall = flag.Bool("D", false, "enable debugging (raw packets)")

func Connect() (hop.Hop, error) {
	if *debug {
		hopclnt.DefaultDebuglevel = 1
	}

	if *debugall {
		hopclnt.DefaultDebuglevel = 2
	}

	naddr := *addr
	if strings.LastIndex(naddr, ":") == -1 {
		naddr = naddr + ":5004"
	}

	return hopclnt.Connect(*proto, naddr)
}

func Stats() string {
	return ""
}
