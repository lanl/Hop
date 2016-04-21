// Copyright 2013 The Hop Authors.  All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package chop

import (
	"errors"
	"fmt"
	"hop"
	"hop/d2hop"
	"strings"
	"sync"
	"sync/atomic"
)

type CHop struct {
	sync.Mutex
	hop	hop.Hop		// the service we cache
	maxmem	uint64		// maximum size of memory that the cache is allowed to use
	maxelem	int		// maximum number of elements that cache is allowed to have

	entries	map[string]*CEntry
	lru	*CEntry		// least recently used entry
	mru	*CEntry		// most recently used entry
	memsz	uint64		// currently used memory (approximation)

	dhop	*d2hop.D2Hop

	// stats
	hits	uint64
	drops	uint64
	dsent	uint64		// sent to a domain
	drecv	uint64		// received for a domain
}

type CEntry struct {
	key	string
	version	uint64
	value	[]byte
	lru	*CEntry		// less used entry
	mru	*CEntry		// more used entry
}

var Einval = errors.New("invalid cache entry")

func NewCache(hop hop.Hop, maxmem uint64, maxelem int, proto, listenaddr, masteraddr string) (c *CHop, err error) {
	c = new(CHop)
	c.hop = hop
	c.maxmem = maxmem
	c.maxelem = maxelem
	c.entries = make(map[string]*CEntry)

	if proto != "" {
		c.dhop, err = d2hop.NewD2Hop(proto, listenaddr, masteraddr, c)
		if err != nil {
			return nil, err
		}
	}

	return c, nil
}

func (c *CHop) getEntry(key string) (ver uint64, val []byte) {
	c.Lock()
	e := c.entries[key]
	if e != nil {
		c.hits++
		ver = e.version
		val = e.value

		// move the entry to the MRU spot
		if e.lru != nil {
			e.lru.mru = e.mru
		} else {
			c.lru = e.mru
		}

		if e.mru != nil {
			e.mru.lru = e.lru
		} else {
			c.mru = e.lru
		}

		e.lru = c.mru
		e.mru = nil

		if c.mru != nil {
			c.mru.mru = e
		}

		c.mru = e
	}
	c.Unlock()
	return
}

func (c *CHop) updateEntry(key string, ver uint64, val []byte) {
	c.Lock()

	n := 1					// new entry
	sz := uint64(len(key) + len(val))	// approximate size
	e := c.entries[key]
	if e != nil {
		c.memsz -= uint64(len(key) + len(e.value))
		n = 0

		c.lruremove(e)
	}

	// remove an entry if we are over the element number limit
	if len(c.entries) + n > c.maxelem {
		c.removeLRU()
	}

	// remove entries if we are over the memory size limit
	for c.memsz + sz > c.maxmem {
		c.removeLRU()
	}

	if e == nil {
		e = new(CEntry)
		e.key = key
		c.entries[key] = e
	}

	e.version = ver
	e.value = val

	// put as MRU
	e.lru = c.mru
	if c.mru != nil {
		c.mru.mru = e
	}

	if c.lru == nil {
		c.lru = e
	}

	c.mru = e
	c.memsz += sz
	c.Unlock()
}

func (c *CHop) removeEntry(key string) {
	c.Lock()
	e := c.entries[key]
	if e != nil {
		delete(c.entries, key)
		c.lruremove(e)
		c.memsz -= uint64(len(e.key) + len(e.value))
	}
	c.Unlock()
}

// called with c lock held
func (c *CHop) removeLRU() {
	le := c.lru
	if le == nil {
		fmt.Printf("LRU empty but %d/%d entries %d/%d mem\n", len(c.entries), c.maxelem, c.memsz, c.maxmem)
		return
	}

	delete(c.entries, le.key)
	if le.mru != nil {
		le.mru.lru = nil
	}

	c.lru = le.mru
	if c.mru == le {
		c.mru = nil
	}

	c.memsz -= uint64(len(le.key) + len(le.value))
	le.lru = nil
	le.mru = nil
	c.drops++
}

// removes an entry from the LRU
// called with c lock held
func (c *CHop) lruremove(e *CEntry) {
	if e.lru != nil {
		e.lru.mru = e.mru
	} else {
		c.lru = e.mru
	}

	if e.mru != nil {
		e.mru.lru = e.lru
	} else {
		c.mru = e.lru
	}

	e.lru = nil
	e.mru = nil
}

func (c *CHop) Create(key, flags string, value []byte) (ver uint64, err error) {
	if c.dhop!=nil && strings.HasPrefix(key, "#/cache/") {
		key = "#/chop/" + key[8:]
		atomic.AddUint64(&c.dsent, 1)
		return c.dhop.Create(key, flags, value)
	} else if strings.HasPrefix(key, "#/chop/") {
		key = key[7:]
		if n := strings.Index(key, "/"); n >= 0 {
			key = key[n+1:]
		} else {
			return 0, Einval
		}
		atomic.AddUint64(&c.drecv, 1)
	}

	ver, err = c.hop.Create(key, flags, value)
	if err == nil && ver != 0 {
		c.updateEntry(key, ver, value)
	}

	return
}

func (c *CHop) Remove(key string) (err error) {
	if c.dhop!=nil && strings.HasPrefix(key, "#/cache/") {
		key = "#/chop/" + key[8:]
		atomic.AddUint64(&c.dsent, 1)
		return c.dhop.Remove(key)
	} else if strings.HasPrefix(key, "#/chop/") {
		key = key[7:]
		if n := strings.Index(key, "/"); n >= 0 {
			key = key[n+1:]
		} else {
			return Einval
		}
		atomic.AddUint64(&c.drecv, 1)
	}

	err = c.hop.Remove(key)
	if err == nil {
		c.removeEntry(key)
	}

	return
}

func (c *CHop) Get(key string, version uint64) (ver uint64, val []byte, err error) {
	if c.dhop!=nil && strings.HasPrefix(key, "#/cache/") {
		key = "#/chop/" + key[8:]
		atomic.AddUint64(&c.dsent, 1)
		return c.dhop.Get(key, version)
	} else if strings.HasPrefix(key, "#/chop/") {
		key = key[7:]
		if n := strings.Index(key, "/"); n >= 0 {
			key = key[n+1:]
		} else {
			return 0, nil, Einval
		}
		atomic.AddUint64(&c.drecv, 1)
	}

	ver, val = c.getEntry(key)
	if ver != 0 && (version == hop.Any || ver > version) {
		return
	}

	ver, val, err = c.hop.Get(key, version)
	if err == nil && ver != 0 {
		c.updateEntry(key, ver, val)
	}

	return
}

func (c *CHop) Set(key string, value []byte) (ver uint64, err error) {
	if c.dhop!=nil && strings.HasPrefix(key, "#/cache/") {
		key = "#/chop/" + key[8:]
		atomic.AddUint64(&c.dsent, 1)
		return c.dhop.Set(key, value)
	} else if strings.HasPrefix(key, "#/chop/") {
		key = key[7:]
		if n := strings.Index(key, "/"); n >= 0 {
			key = key[n+1:]
		} else {
			return 0, Einval
		}
		atomic.AddUint64(&c.drecv, 1)
	}

	ver, err = c.hop.Set(key, value)
	if err == nil && ver != 0 {
		c.updateEntry(key, ver, value)
	}

	return
}

func (c *CHop) TestSet(key string, oldversion uint64, oldvalue, value []byte) (ver uint64, val []byte, err error) {
	if c.dhop!=nil && strings.HasPrefix(key, "#/cache/") {
		key = "#/chop/" + key[8:]
		atomic.AddUint64(&c.dsent, 1)
		return c.dhop.TestSet(key, oldversion, oldvalue, value)
	} else if strings.HasPrefix(key, "#/chop/") {
		key = key[7:]
		if n := strings.Index(key, "/"); n >= 0 {
			key = key[n+1:]
		} else {
			return 0, nil, Einval
		}
		atomic.AddUint64(&c.drecv, 1)
	}

	ver, val, err = c.hop.TestSet(key, oldversion, oldvalue, value)
	if err == nil && ver != 0 {
		c.updateEntry(key, ver, val)
	}

	return
}

func (c *CHop) Atomic(key string, op uint16, values [][]byte) (ver uint64, vals [][]byte, err error) {
	if c.dhop!=nil && strings.HasPrefix(key, "#/cache/") {
		key = "#/chop/" + key[8:]
		atomic.AddUint64(&c.dsent, 1)
		return c.dhop.Atomic(key, op, values)
	} else if strings.HasPrefix(key, "#/chop/") {
		key = key[7:]
		if n := strings.Index(key, "/"); n >= 0 {
			key = key[n+1:]
		} else {
			return 0, nil, Einval
		}
		atomic.AddUint64(&c.drecv, 1)
	}

	ver, vals, err = c.hop.Atomic(key, op, values)
	if err != nil || ver == 0 {
		return
	}

	if vals==nil {
		fmt.Printf("ver %d err %v vals %v\n", ver, err, vals)
		return
	}

	// try to update the cached entry from the values returnes by known ops
	switch op {
	case hop.Add, hop.Sub, hop.BitSet, hop.BitClear, hop.Append, hop.Remove, hop.Replace:
		c.updateEntry(key, ver, vals[0])
	}

	return
}

func (c *CHop) Stats() (ret string) {
	ret += fmt.Sprintf("Cache Elements: %d\n", len(c.entries))
	ret += fmt.Sprintf("Cache Size: %d\n", c.memsz)
	ret += fmt.Sprintf("Cache Hits: %d\n", c.hits)
	ret += fmt.Sprintf("Cache Drops: %d\n", c.drops)
	ret += fmt.Sprintf("Cache Domain Sent: %d\n", c.dsent)
	ret += fmt.Sprintf("Cache Domain Recv: %d\n", c.drecv)

	return
}
