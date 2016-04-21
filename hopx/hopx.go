// Copyright 2013 The Hop Authors.  All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package main

import (
	"errors"
	"flag"
	"fmt"
	"hop"
	"hop/shop"
	"math"
	"math/rand"
	"sync"
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
	0:  Opget,
	40: Opset,
	80: Opcreate,
	90: Opremove,
	//80:	Optestset,
	//90:	Opatomic,
}

var vminlen = flag.Int64("vmin", 512, "minumum value length")
var vmaxlen = flag.Int64("vmax", 512*1024, "maximum value length")
var kminlen = flag.Int("kmin", 2, "minimum key length")
var kmaxlen = flag.Int("kmax", 64, "maximum key length")
var kprefix = flag.String("kprefix", "", "key prefix")
var keynum = flag.Int("knum", 256, "maximum number of keys to create")
var numop = flag.Int("N", math.MaxInt32, "total number of operations")
var seed = flag.Int64("S", 1, "seed for the random number generator")
var threadnum = flag.Int("threadnum", 1, "number of op threads")
var check = flag.Bool("c", false, "check if the values are correct")
//var oplist = flag.String("oplist", "get,set,create,remove", "comma-delimited list of operations")

var h hop.Hop
var checkHop hop.Hop
var errchan chan error

type Tdata struct {
	sync.RWMutex
	suffix string
	keys   []string
	kmap   map[string]bool

	datasent uint64
	datarecv uint64
	reqnum   int
}

var rval []byte

func genkey() string {
	l := rand.Intn(*kmaxlen - *kminlen) + *kminlen
	s := *kprefix
	for i := 0; i < l; i++ {
		s += string(byte(rand.Intn(72) + '0'))
	}

	return s
}

func genval() []byte {
	pos := rand.Intn(int(*vmaxlen - *vminlen))
	length := rand.Intn(len(rval) - pos)

	return rval[pos : pos+length]
}

func (t *Tdata) getkey() string {
	t.RLock()
	defer t.RUnlock()

	if t.keys == nil || len(t.keys) == 0 {
		return ""
	}

	return t.keys[rand.Intn(len(t.keys))]
}

func checkval(val, cval []byte) error {
	if len(val) != len(cval) {
		return errors.New(fmt.Sprintf("wrong value length: %d %d", len(val), len(cval)))
	}

	for i, v := range val {
		if v != cval[i] {
			return errors.New(fmt.Sprintf("wrong value: %v instead of %v", val, cval))
		}
	}

	return nil
}

func (t *Tdata) testget() error {
	key := t.getkey()
	if key == "" {
		return nil
	}

	_, val, err := h.Get(key, hop.Any)
	if checkHop != nil {
		_, cval, cerr := checkHop.Get(key, hop.Any)
		if cerr != nil {
			if err == nil {
				return cerr
			} else {
				err = nil
			}
		}
		err = checkval(val, cval)
	}

	t.Lock()
	t.reqnum++
	t.datarecv += uint64(len(val) + len(key))
	t.Unlock()

	return err
}

func (t *Tdata) testset() error {
	key := t.getkey()
	if key == "" {
		return nil
	}

	value := genval()

	_, err := h.Set(key, value)
	if checkHop != nil {
		_, cerr := checkHop.Set(key, value)
		if cerr != nil {
			if err == nil {
				return cerr
			} else {
				err = nil
			}
		}
	}

	t.Lock()
	t.reqnum++
	t.datasent += uint64(len(value) + len(key))
	t.Unlock()

	return err
}

func (t *Tdata) testcreate() error {
	t.Lock()
	key := t.suffix + genkey()
	for t.kmap[key] {
		key = genkey() + t.suffix
	}

	t.kmap[key] = true
	t.Unlock()

	value := genval()
	_, err := h.Create(key, "", value)
	if checkHop != nil {
		_, cerr := checkHop.Create(key, "", value)
		if cerr != nil {
			if err == nil {
				return cerr
			} else {
				err = nil
			}
		}
	}

	if err == nil {
		t.Lock()
		t.keys = append(t.keys, key)
		t.reqnum++
		t.datasent += uint64(len(value) + len(key))
		t.Unlock()
	}

	return err
}

func (t *Tdata) testremove() error {
	t.Lock()
	if t.keys == nil || len(t.keys) == 0 {
		t.Unlock()
		return nil
	}

	n := rand.Intn(len(t.keys))
	key := t.keys[n]
	if n+1 < len(t.keys) {
		copy(t.keys[n:], t.keys[n+1:])
	}

	t.keys = t.keys[0 : len(t.keys)-1]
	t.Unlock()

	err := h.Remove(key)
	if checkHop != nil {
		cerr := checkHop.Remove(key)
		if cerr != nil {
			if err == nil {
				return cerr
			} else {
				err = nil
			}
		}
	}

	if err == nil {
		t.Lock()
		delete(t.kmap, key)
		t.reqnum++
		t.datasent += uint64(len(key))
		t.Unlock()
	}

	return err
}

func (t *Tdata) testtestset() error {
	return errors.New("not implemented")
}

func (t *Tdata) testatomic() error {
	return errors.New("not implemented")
}

func (t *Tdata) test(op int) error {
	var err error

	switch op {
	case Opget:
		err = t.testget()

	case Opset:
		err = t.testset()

	case Opcreate:
		err = t.testcreate()

	case Opremove:
		err = t.testremove()

	case Optestset:
		err = t.testtestset()

	case Opatomic:
		err = t.testatomic()
	}

	return err
}

func (t *Tdata) testloop() {
	for i := 0; i < *numop; i++ {
		err := t.test(ops[rand.Intn(100)])
		if err != nil {
			errchan <- err
			return
		}
	}

	errchan <- nil
}

func main() {
	var v int
	var err error

	flag.Parse()

	rand.Seed(*seed)

	// fill the blanks in the ops array
	for i, op := range ops {
		if op != 0 {
			v = op
		} else {
			ops[i] = v
		}
	}

	errchan = make(chan error)
	rval = make([]byte, *vmaxlen)
	for i := 0; i < len(rval); i++ {
		rval[i] = byte(rand.Intn(256))
	}

	h, err = Connect()
	if err != nil {
		fmt.Printf("Error: %v\n", err)
		return
	}

	if *check {
		checkHop = shop.NewSHop()
	}

	st := time.Now().UnixNano() / 1000
	// start the threads
	tds := make([]*Tdata, *threadnum)
	for i := 0; i < *threadnum; i++ {
		tds[i] = new(Tdata)
		tds[i].suffix = fmt.Sprintf("%02d", i)
		tds[i].kmap = make(map[string]bool)

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
	for _, td := range tds {
		datasent += td.datasent
		datarecv += td.datarecv
		reqnum += td.reqnum
	}

	fmt.Printf("Time: %v us\n", et-st)
	fmt.Printf("Data sent: %v bytes\n", datasent)
	fmt.Printf("Data received: %v bytes\n", datarecv)
	fmt.Printf("Number of requests: %d\n", reqnum)
	fmt.Printf("\n\n")
	fmt.Printf("Bandwidth: %.2f MB/s\n", (float64(datasent+datarecv)*1000000.0)/(float64(et-st)*1024.0*1024.0))
	fmt.Printf("Rate: %.2f requests/s\n", (float64(reqnum)*1000000.0)/float64(et-st))

	stats := Stats()
	if stats != "" {
		fmt.Printf(stats)
	}
}
