// Copyright 2013 The Hop Authors.  All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package dhop

import (
	"fmt"
	"hash"
	"hash/fnv"
	"hop"
	"hop/rmt/hopclnt"
	"strings"
	"sync"
)

type DHopClnt struct {
	sync.RWMutex

	proto    string
	addr     string
	master   hop.Hop
	srvs     []hop.Hop
	smap     map[string]hop.Hop
	hashchan chan hash.Hash32
	hash     hash.Hash32
}

func Connect(proto, addr string) (hop.Hop, error) {
	c := new(DHopClnt)
	c.proto = proto
	c.addr = addr
	master, err := hopclnt.Connect(proto, addr)
	if err != nil {
		return nil, err
	}

	c.master = master
	c.smap = make(map[string]hop.Hop)
	c.hashchan = make(chan hash.Hash32, 32)
	if version, e := c.updatePeers(hop.Any); e == nil {
		go c.confproc(version)
	} else {
		return nil, e
	}

	return c, nil
}

func (c *DHopClnt) updatePeers(version uint64) (ver uint64, err error) {
	var val []byte

	ver, val, err = c.master.Get("#/peers", version)
	if err != nil {
		fmt.Printf("Error: %v\n", err)
		return
	}

	ss := strings.Split(string(val), ",")
	srvs := make([]hop.Hop, len(ss))
	smap := make(map[string]hop.Hop)
	for i, s := range ss {
		if h, ok := c.smap[s]; ok {
			srvs[i] = h
			smap[s] = h
		} else if h, e := hopclnt.Connect(c.proto, s); err == nil {
			srvs[i] = h
			smap[s] = h
		} else {
			err = e
			return
		}

	}

	c.Lock()
	c.srvs = srvs
	c.smap = smap
	c.Unlock()

	return
}

func (c *DHopClnt) confproc(version uint64) {
	var err error

	for {
		version, err = c.updatePeers(version + 1)
		if err != nil {
			fmt.Printf("Error: %v\n", err)
			return
		}
	}
}

func (c *DHopClnt) getServer(key string) hop.Hop {
	var h hash.Hash32

	// get the hash function
	select {
	case h = <-c.hashchan:
		// got a hash
	default:
		h = fnv.New32a()
	}

	h.Reset()
	h.Write([]byte(key))
	hcode := h.Sum32()

	// release the hash function
	select {
	case c.hashchan <- h:
	default:
	}

	c.RLock()
	s := c.srvs[hcode%uint32(len(c.srvs))]
	c.RUnlock()

	return s
}

func (c *DHopClnt) Create(key, flags string, value []byte) (version uint64, err error) {
	return c.getServer(key).Create(key, flags, value)
}

func (c *DHopClnt) Remove(key string) (err error) {
	return c.getServer(key).Remove(key)
}

func (c *DHopClnt) Get(key string, version uint64) (ver uint64, value []byte, err error) {
	var v []byte

	if strings.HasPrefix(key, "#/keys") {
		c.RLock()
		_, value, err = c.srvs[0].Get(key, hop.Any)
		if err != nil {
			return 0, nil, err
		}

		for i := 1; i < len(c.srvs); i++ {
			_, v, err = c.srvs[1].Get(key, hop.Any)
			if err != nil {
				return 0, nil, err
			}

			value = append(value, ',')
			value = append(value, v...)
		}
		c.RUnlock()

		return 1, value, nil
	}

	return c.getServer(key).Get(key, version)
}

func (c *DHopClnt) Set(key string, value []byte) (version uint64, err error) {
	return c.getServer(key).Set(key, value)
}

func (c *DHopClnt) TestSet(key string, oldversion uint64, oldvalue, val []byte) (version uint64, value []byte, err error) {
	return c.getServer(key).TestSet(key, oldversion, oldvalue, value)
}

func (c *DHopClnt) AtomicSet(key string, op uint16, val []byte) (version uint64, value []byte, err error) {
	return c.getServer(key).AtomicSet(key, op, val)
}
