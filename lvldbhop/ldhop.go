// Copyright 2015 The Hop Authors.  All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package lvldbhop

/*
#cgo CFLAGS: 
#cgo LDFLAGS: -lleveldb

#include <stdlib.h>
#include <string.h>
#include <leveldb/c.h>


*/
import "C"

import (
	"errors"
//	"fmt"
	"hop"
	"strings"
	"sync"
	"unsafe"
)

type entry struct {
	sync.RWMutex
	sync.Cond
	version		uint64
	value		[]byte
	maxver		uint64
}

type LDHop struct {
	sync.RWMutex
	db	*C.leveldb_t
	opts	*C.leveldb_options_t
	cmp	*C.leveldb_comparator_t
	flt	*C.leveldb_filterpolicy_t
	cache	*C.leveldb_cache_t
	env	*C.leveldb_env_t
	wopts	*C.leveldb_writeoptions_t
	ropts	*C.leveldb_readoptions_t

	// the entries map contains "interesting" entries, i.e. 
	// entries with pending operations (non-existing, or future versions)
	entries	map[string] *entry
	keynumEntry *entry
	keysEntry *entry
}

var Eparams = errors.New("invalid parameter number")
var Enil = errors.New("nil value")
var Einval = errors.New("invalid value")

func NewLDHop(filename string, cachesz uint64, wbufsz uint64, bloomsz int) (*LDHop, error) {
	var err *C.char

	h := new(LDHop)
	h.opts = C.leveldb_options_create()
	C.leveldb_options_set_create_if_missing(h.opts, 1);

	h.flt = C.leveldb_filterpolicy_create_bloom(C.int(bloomsz))
	h.cache = C.leveldb_cache_create_lru(C.size_t(cachesz))
	h.env = C.leveldb_create_default_env()
	h.wopts = C.leveldb_writeoptions_create()
	C.leveldb_writeoptions_set_sync(h.wopts, 0)
	h.ropts = C.leveldb_readoptions_create()
	C.leveldb_options_set_cache(h.opts, h.cache)
	C.leveldb_options_set_filter_policy(h.opts, h.flt)
	C.leveldb_options_set_write_buffer_size(h.opts, C.size_t(wbufsz))
	C.leveldb_options_set_env(h.opts, h.env)

	cname := C.CString(filename)
	defer C.free(unsafe.Pointer(cname))
	h.db = C.leveldb_open(h.opts, cname, &err)
	if err != nil {
		return nil, errors.New(C.GoString(err))
	}

	h.entries = make(map[string]*entry)
	h.keynumEntry = new(entry)
	h.keynumEntry.L = h.keynumEntry.RLocker()
	h.entries["#/keynum"] = h.keynumEntry
	h.keysEntry = new(entry)
	h.keysEntry.L = h.keysEntry.RLocker()
	h.entries["#/keys"] = h.keysEntry

	return h, nil
}

func (h *LDHop) getKeys(key string) (ver uint64, val []byte, err error) {
/*
	var regex string

	if strings.HasPrefix(key, "#/keys:") {
		regex = key[7:]
	} else {
		regex = ".*"
	}

	cregex := C.CString(regex)
	max := int(C.kcdbcount(h.db))
	recs := make([]*C.char, int(max))
	n := int(C.kcdbmatchregex(h.db, cregex, &recs[0], C.size_t(max)))
	if n < 0 {
		err = h.error()
		return
	}

	val = []byte{}
	for i:=0; i < n; i++ {
		key := C.GoString(recs[i])
		val = append(val, key...)
		val = append(val, 0)
		C.kcfree(unsafe.Pointer(recs[i]))
	}

	if len(val) > 0 {
		// remove the trailing zero
		val = val[0 : len(val)-1]
	}

	ver = h.keysEntry.version
	return
*/
	return 0, nil, errors.New("not implemented yet")
}

func (h *LDHop) getKeynum() (ver uint64, val []byte, err error) {
/*	n := C.kcdbcount(h.db)
	if n < 0 {
		err = h.error()
		return
	}

	return h.keynumEntry.version, []byte(fmt.Sprintf("%d", n)), nil
*/
	return 0, nil, errors.New("not implemented yet")
}

func (e *entry) IncreaseVersion() {
	e.version++

	if e.version >= hop.Highest {
		e.version = hop.Lowest
	}
}

func (h *LDHop) keysModified() {
	h.keysEntry.Lock()
	h.keysEntry.IncreaseVersion()
	h.keysEntry.Unlock()
	h.keysEntry.Broadcast()

	h.keynumEntry.Lock()
	h.keynumEntry.IncreaseVersion()
	h.keynumEntry.Unlock()
	h.keynumEntry.Broadcast()
}


func ldvalToValue(val []byte) (version uint64, value []byte) {
	version, _ = hop.Gint64(val)
	value = val[8:]
	return
}

func valueToLdval(version uint64, value []byte) (val []byte) {
	val = make([]byte, 8 + len(value))
	hop.Pint64(version, val)
	copy(val[8:], value)
	return
}

func (h *LDHop) Create(key, flags string, value []byte) (version uint64, err error) {
	var cerr *C.char

	if strings.HasPrefix(key, "#/") {
		return 0, hop.Eperm
	}

	if value == nil {
		return 0, Enil
	}

	ckey := C.CString(key)
	defer C.free(unsafe.Pointer(ckey))
	cvalue := valueToLdval(hop.Lowest, value)
	C.leveldb_put(h.db, h.wopts, ckey, C.strlen(ckey), (*C.char)(unsafe.Pointer(&cvalue[0])), C.size_t(len(cvalue)), &cerr)
	if cerr != nil {
		err = errors.New(C.GoString(cerr))
		C.free(unsafe.Pointer(cerr))
		return
	}

	h.Lock()
	e := h.entries[key]
	if e != nil {
		delete(h.entries, key)
	}
	h.Unlock()

	// if anybody was waiting for the entry, let them know it was created
	if e != nil {
		e.Broadcast()
	}

	h.keysModified()
	return hop.Lowest, nil
}

func (h *LDHop) Remove(key string) (err error) {
	var cerr *C.char

	ckey := C.CString(key)
	defer C.free(unsafe.Pointer(ckey))

	C.leveldb_delete(h.db, h.wopts, ckey, C.strlen(ckey), &cerr)
	if cerr != nil {
		err = errors.New(C.GoString(cerr))
		C.free(unsafe.Pointer(cerr))
		return
	}

	h.Lock()
	e := h.entries[key]
	if e != nil {
		delete(h.entries, key)
	}
	h.Unlock()

	// if anybody is waiting for the future versions (or entry being created),
	// let them fail
	if e != nil {
		e.version = hop.Removed
		e.Broadcast()
	}

	h.keysModified()
	return
}

// get the actual value from the cabinet
func (h *LDHop) getLdvalue(ckey *C.char) (val []byte, err error) {
	var vlen C.size_t
	var cerr *C.char

	cdata := C.leveldb_get(h.db, h.ropts, ckey, C.strlen(ckey), &vlen, &cerr);
	if cerr != nil {
		err = errors.New(C.GoString(cerr))
		C.free(unsafe.Pointer(cerr))
		return
	}

	if vlen == 0 {
		val = nil
	} else {
		val = make([]byte, int(vlen))
		C.memcpy(unsafe.Pointer(&val[0]), unsafe.Pointer(cdata), vlen)
		C.leveldb_free(unsafe.Pointer(cdata))
	}

	return
}

func (h *LDHop) getvalue(key string) (k string, ver uint64, val []byte, err error) {
	k = key
	if strings.HasPrefix(key, "#/keys") {
		ver, val, err = h.getKeys(key)
		k = "#/keys"
	} else if key == "#/keynum" {
		ver, val, err = h.getKeynum()
	} else {
		var ldval []byte

		ckey := C.CString(key)
		defer C.free(unsafe.Pointer(ckey))
		ldval, err = h.getLdvalue(ckey)
		if err != nil || ldval == nil {
			return
		}

		ver, val = ldvalToValue(ldval)
	}

	return
}

func (h *LDHop) Get(key string, version uint64) (ver uint64, val []byte, err error) {
	key, ver, val, err = h.getvalue(key)
	if err != nil {
		return
	}

	if version==hop.Any || version==hop.Newest || version <= ver {
		return
	}

	if version == hop.PastNewest {
		version = ver + 1
	}

	h.Lock()
	e := h.entries[key]
	if e == nil {
		// create a new entry so everybody can wait on it
		e = new(entry)
		e.L = e.RLocker()
		e.maxver = version
		key, e.version, e.value, err = h.getvalue(key)	// this can probably be smarter
		if err != nil {
			h.Unlock()
			return
		}
		h.entries[key] = e
	} else if e.maxver < version {
		e.maxver = version
	}
	h.Unlock()

	e.RLock()
	if e.maxver < version {
		e.maxver = version
	}

	ver = e.version
	for ver != hop.Removed && ver < version {
		e.Wait()
		ver = e.version
	}

	ver = e.version
	val = e.value
	e.RUnlock()

	h.Lock()
	if e.maxver <= version && e != h.keynumEntry && e != h.keysEntry {
		delete(h.entries, key)
	}
	h.Unlock()

	if ver == hop.Removed {
		// the entry has been removed
		ver = 0
		val = nil
	}

	return
}

func (h *LDHop) Set(key string, value []byte) (ver uint64, err error) {
	ver, _, err = h.TestSet(key, hop.Any, nil, value)
	return
}

func (h *LDHop)	TestSet(key string, oldversion uint64, oldvalue, value []byte) (ver uint64, val []byte, err error) {
	var cerr *C.char

	if value == nil {
		return 0, nil, Enil
	}

	h.Lock()
	e := h.entries[key]
	if e == nil {
		// create a new entry so everybody can wait on it
		e = new(entry)
		e.L = e.RLocker()
		e.maxver = 0 // hop.Lowest // version
		h.entries[key] = e
	}
	h.Unlock()
	
	e.Lock()
	key, e.version, e.value, err = h.getvalue(key)
	if err != nil {
		e.Unlock()
		return
	}

	changed := false
	ver = e.version
	val = e.value

	if oldversion == hop.Any {
		oldversion = e.version
	} else if oldversion < hop.Lowest || oldversion > hop.Highest {
		err = errors.New("invalid version")
		goto done
	}

        if oldversion != e.version {
                goto done
        }

        if oldvalue != nil {
                if len(oldvalue) != len(e.value) {
                        goto done
                }

                for i, s := range oldvalue {
                        if s != e.value[i] {
                                goto done
                        }
                }
        }

        e.IncreaseVersion()
        ver = e.version

	{
		ckey := C.CString(key)
		defer C.free(unsafe.Pointer(ckey))
		cvalue := valueToLdval(ver, value)
		C.leveldb_put(h.db, h.wopts, ckey, C.strlen(ckey), (*C.char)(unsafe.Pointer(&cvalue[0])), C.size_t(len(cvalue)), &cerr)
		if cerr != nil {
			err = errors.New(C.GoString(cerr))
			C.free(unsafe.Pointer(cerr))
			goto done
		}

		_, e.value = ldvalToValue(cvalue)		// save one slice allocation
		changed = true
	}


done:

	h.Lock()
	if e.maxver <= ver {
		delete(h.entries, key)
	}
	h.Unlock()

	if !changed {
		e.Unlock()
		return
	}

	// value was successfully changed, notify waiters...
	if e.version < ver {
		// it is possible that another test&set updates before this one...
		e.version = ver
		e.value = val
	}
	e.Unlock()
	e.Broadcast()

	h.keysModified()
	return
}

func (s *LDHop) Atomic(key string, op uint16, values [][]byte) (ver uint64, vals [][]byte, err error) {
	return 0, nil, errors.New("not implemented")
}
