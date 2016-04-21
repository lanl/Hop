// Copyright 2013 The Hop Authors.  All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// +build cache

package main

import (
	"flag"
	"hop"
	"hop/cache"
	"hop/rmt/hopclnt"
	"strings"
)

var proto = flag.String("proto", "tcp", "connection protocol")
var addr = flag.String("addr", "127.0.0.1:5004", "network address")
var debug = flag.Bool("d", false, "enable debugging (fcalls)")
var debugall = flag.Bool("D", false, "enable debugging (raw packets)")
var maxmem = flag.Int64("maxmem", 64*1024*1024, "cache maximum memory use")
var maxelem = flag.Int("maxelem", 128, "cache maximum number of elements")

var c *cache.CHop

func Connect() (h hop.Hop, err error) {
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

	h, err = hopclnt.Connect(*proto, naddr)
	if err != nil {
		return
	}

	c = cache.NewCache(h, uint64(*maxmem), *maxelem)

	return c, nil
}

func Stats() string {
	return c.Stats()
}
