// Copyright 2013 The Hop Authors.  All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package main

import (
	"flag"
	"fmt"
	"hop"
	"hop/shop"
	"hop/rmt/hopclnt"
	"hop/d2hop"
	"hop/chord"
	"hop/chop"
	"hop/kchop"
	"math"
	"math/rand"
	"runtime"
	"time"
)

const (
	Opget = iota + 1
	Opset
	Opcreate
	Opremove
	Optestset
	Opatomic
)

var ops = [100]int{
	0:	Opget,
	55:	Opset,
	85:	Opcreate,
	90:	Opremove,
	94:	Optestset,
	97:	Opatomic,
}

// benchmark flags
var vminlen = flag.Int64("vmin", 512, "minumum value length")
var vmaxlen = flag.Int64("vmax", 512*1024, "maximum value length")
var keynum = flag.Int64("knum", 16*1024*1024, "maximum number of keys to create")
var numop = flag.Int("N", math.MaxInt32, "total number of operations per thread")
var seed = flag.Int64("S", 1, "seed for the random number generator")
var threadnum = flag.Int("threadnum", 1, "number of op threads")
var anyratio = flag.Int("anyratio", 50, "percent of Any versions used in Get (the rest Newest)")
var sleep = flag.Int("T", 0, "time to sleep before starting the tests")

// D2Hop | Chord flags
var hoptype = flag.String("hop", "chord", "chord | d2hop")
var proto = flag.String("proto", "tcp", "connection protocol")
var addr = flag.String("addr", "", "address for the server (client if empty)")
var maddr = flag.String("maddr", "", "master address (master if empty)")

// MHop flags
var mhop = flag.Bool("mhop", false, "use MHop")

// CHop flags
var chopaddr = flag.String("chopaddr", "", "address for the CHop instance")
var chopmaddr = flag.String("chopmaddr", "", "master of the CHop group")
var chopmem = flag.Uint64("chopmem", 16*1024*1024, "maximum memory used for cache in CHop")
var chopelem = flag.Int("chopelem", 1024, "maximum number of elements in the cache in CHop")
var chopdomain = flag.String("chopdomain", "", "CHop consistency domain")

// KCHop flags
var kcdbname = flag.String("kcdb", "", "Kyoto Cabinet database name")

// common server flags
var debug = flag.Int("d", 0, "debuglevel")
var logsz = flag.Int("l", 2048, "log size")

var h hop.Hop
var errchan chan error
var rval []byte
var keyprefix string

type Tdata struct {
	rnd	 *rand.Rand

	// stats
	datasent uint64
	datarecv uint64
	reqnum   int
	errnum	 int
}


func newThread(id int) *Tdata {
	t := new(Tdata)
	t.rnd = rand.New(rand.NewSource(int64(id) + *seed*1024))

	return t
}

func (t *Tdata) genkey() string {
	var buf [6]byte

	n := uint64(t.rnd.Int63n(*keynum))
	buf[0] = byte(n & 0x3f) + '0'
	buf[1] = byte((n >> 6) & 0x3f) + '0'
	buf[2] = byte((n >> 12) & 0x3f) + '0'
	buf[3] = byte((n >> 18) & 0x3f) + '0'
	buf[4] = byte((n >> 24) & 0x3f) + '0'
	buf[5] = byte((n >> 32) & 0x3f) + '0'

	return keyprefix + string(buf[0:])
}

func (t *Tdata) genval() []byte {
	l := t.rnd.Intn(int(*vmaxlen - *vminlen)) + int(*vminlen)
	start := t.rnd.Intn(len(rval) - l - 1)

	return rval[start : start + l]
}

func (t *Tdata) testget() {
	key := t.genkey()

	ver := uint64(hop.Any)
	if n := t.rnd.Intn(100); n > *anyratio {
		ver = hop.Newest
	}
	
	_, val, err := h.Get(key, ver)
	t.reqnum++
	t.datasent += uint64(len(key))

	if err != nil {
		t.errnum++
	} else {
		t.datarecv += uint64(len(val))
	}
}

func (t *Tdata) testset() {
	key := t.genkey()
	value := t.genval()

	_, err := h.Set(key, value)

	t.reqnum++
	t.datasent += uint64(len(value) + len(key))

	if err != nil {
		t.errnum++
	}
}

func (t *Tdata) testcreate() {
	key := t.genkey()
	value := t.genval()

	_, err := h.Create(key, "", value)
	t.reqnum++
	t.datasent += uint64(len(value) + len(key))
	if err != nil {
		t.errnum++
	}
}

func (t *Tdata) testremove() {
	key := t.genkey()
	err := h.Remove(key)
	t.reqnum++
	t.datasent += uint64(len(key))
	if err != nil {
		t.errnum++
	}
}

func (t *Tdata) testtestset() {
	key := t.genkey()
	oldval := t.genval()
	value := t.genval()

	newver, newval, err := h.TestSet(key, hop.Any, oldval, value)
	t.reqnum++
	t.datasent += uint64(len(key) + len(oldval) + len(value))
	if err != nil {
		t.errnum++
	} else {
		t.datarecv += uint64(len(newval))
	}

	_, newval, err = h.TestSet(key, newver, nil, value)
	t.reqnum++
	t.datasent += uint64(len(key) + len(value))
	if err != nil {
		t.errnum++
	} else {
		t.datarecv += uint64(len(newval))
	}
}

func (t *Tdata) testatomic() {
	key := t.genkey()
	oldval := t.genval()
	newval := t.genval()

	if len(newval) > len(oldval) {
		newval = newval[0:len(oldval)]
	}

	values := [][]byte{oldval, newval}
	_, vals, err := h.Atomic(key, hop.Replace, values)
	t.reqnum++
	t.datasent += uint64(len(key))
	for _, v := range(values) {
		t.datasent += uint64(len(v))
	}

	if err != nil {
		t.errnum++
	} else {
		for _, v := range(vals) {
			if v != nil {
				t.datarecv += uint64(len(v))
			}
		}
	}
}

func (t *Tdata) testloop() {
	for t.reqnum < *numop {
		op := ops[t.rnd.Intn(100)]
		switch op {
		case Opget:
			t.testget()

		case Opset:
			t.testset()

		case Opcreate:
			t.testcreate()

		case Opremove:
			t.testremove()

		case Optestset:
			t.testtestset()

		case Opatomic:
			t.testatomic()
		}
	}

	errchan <- nil
}

func main() {
	var v int
	var err error
	var hops, hoph, hopm, hopc hop.Hop
	var chophop *chop.CHop

	// fill the blanks in the ops array
	for i, op := range ops {
		if op != 0 {
			v = op
		} else {
			ops[i] = v
		}
	}

	flag.Parse()
	runtime.GOMAXPROCS(runtime.NumCPU())
	hopclnt.DefaultDebuglevel = *debug

	if *maddr == "" {
		fmt.Printf("maddr flag required\n")
		return
	}

	// create the backing Hop instance, if not client-only
	if *addr != "" {
		if (*kcdbname != "") {
			var err error

			hops, err = kchop.NewKCHop(*kcdbname)
			if err != nil {
				fmt.Printf("Error: %v\n", err)
				return
			}
		} else {
			hops = shop.NewSHop()
		}
	}

	// create the distributed Hop instance
	switch *hoptype {
	case "chord":
		s, err := chord.NewChord(*proto, *addr, *maddr, hops)
		if err != nil {
			fmt.Printf("Can't create Chord instance: %v\n", err)
			return
		}
		hoph = s
		s.SetLogger(hop.NewLogger(*logsz))
		s.SetDebugLevel(*debug)

	case "d2hop":
		s, err := d2hop.NewD2Hop(*proto, *addr, *maddr, hops)
		if err != nil {
			fmt.Printf("Can't create D2Hop instance: %v\n", err)
			return
		}
		hoph = s
		s.SetLogger(hop.NewLogger(*logsz))
		s.SetDebugLevel(*debug)

	case "shop":
		hoph = hops

	default:
		fmt.Printf("invalid hop type: %s\n", *hoptype)
		return
	}

	// create the CHop instance, if requested
	if *chopaddr != "" {
		var err error

		chophop, err = chop.NewCache(hoph, *chopmem, *chopelem, *proto, *chopaddr, *chopmaddr)
		if err != nil {
			fmt.Printf("Can't create CHop instance: %v\n", err)
			return
		}

		if *chopdomain != "" {
			keyprefix = "#/cache/" + *chopdomain + "/"
		}
		hopc = chophop
	} else {
		hopc = hoph
	}

	// create MHop, if requested
	if *mhop {
		m := hop.NewMHop(nil)
		keyprefix = "hopm/" + keyprefix
		m.AddAfter("hopm/", false, true, hopc)
		hopm = m
	} else {
		hopm = hopc
	}

	h = hopm

	rand.Seed(*seed)
	errchan = make(chan error)
	rval = make([]byte, *vmaxlen * 2)
	for i := 0; i < len(rval); i++ {
		rval[i] = byte(rand.Intn(256))
	}

	time.Sleep(time.Duration(*sleep) * time.Second)
	st := time.Now().UnixNano() / 1000
	// start the threads
	tds := make([]*Tdata, *threadnum)
	for i := 0; i < *threadnum; i++ {
		tds[i] = newThread(i)
		go tds[i].testloop()
	}

	// wait for the threads to finish
	for i := 0; i < *threadnum; i++ {
		err = <-errchan
		if err != nil {
			fmt.Printf("Error: %v\n", err)
			return
		}
	}
	et := time.Now().UnixNano() / 1000

	datasent := uint64(0)
	datarecv := uint64(0)
	reqnum := 0
	errnum := 0
	for _, td := range tds {
		datasent += td.datasent
		datasent += uint64(td.reqnum * 8)		// not exact, but close enough
		datarecv += td.datarecv
		datarecv += uint64(td.reqnum * 8)		// not exact, but close enough
		reqnum += td.reqnum
		errnum += td.errnum
	}


	if chophop != nil {
		fmt.Printf("%s", chophop.Stats())
	}

	fmt.Printf("Time: %v us\n", et-st)
	fmt.Printf("Data sent: %v bytes\n", datasent)
	fmt.Printf("Data received: %v bytes\n", datarecv)
	fmt.Printf("Number of requests: %d\n", reqnum)
	fmt.Printf("Number of errors: %d\n", errnum)
	fmt.Printf("\n\n")
	fmt.Printf("Bandwidth: %.2f MB/s\n", (float64(datasent+datarecv)*1000000.0)/(float64(et-st)*1024.0*1024.0))
	fmt.Printf("Rate: %.2f requests/s\n", (float64(reqnum)*1000000.0)/float64(et-st))
	fmt.Printf("ReqSize: %.2f bytes\n", float64(datasent+datarecv) / float64(reqnum))

	for {
		time.Sleep(time.Second)
	}
}
