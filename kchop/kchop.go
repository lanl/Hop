// Copyright 2015 The Hop Authors.  All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package kchop

/*
#cgo pkg-config: kyotocabinet

#include <kclangc.h>

typedef struct tsetentry {
	uint64_t	oldver;
	char*		oldval;
	uint32_t	oldvalsz;
	uint64_t	newver;
	char*		newval;
	uint32_t	newvalsz;
	int		err;
} tsetentry;

static uint64_t getver(const char *data) {
	return (uint64_t)data[0] | ((uint64_t)data[1]<<8) | ((uint64_t)data[2]<<16) |
                ((uint64_t)data[3]<<24) | ((uint64_t)data[4]<<32) | ((uint64_t)data[5]<<40) |
                ((uint64_t)data[6]<<48) | ((uint64_t)data[7]<<56);
}

static void putver(char *data, uint64_t val) {
	data[0] = val;
	data[1] = val >> 8;
	data[2] = val >> 16;
	data[3] = val >> 24;
	data[4] = val >> 32;
	data[5] = val >> 40;
	data[6] = val >> 48;
	data[7] = val >> 56;
}

static const char *tsetvisit(const char *kbuf, size_t ksiz, const char *vbuf, size_t vsiz, size_t *sp, void *opq) {
	uint64_t ver;
	const char *val;
	tsetentry *e = (tsetentry *) opq;

	if (vsiz < 8) {
		e->err = 1;
		return KCVISNOP;
	}

	ver = getver(vbuf);
	val = &vbuf[8];
	vsiz -= 8;
	if (e->oldver == 0)
		e->oldver = ver;

	if (e->oldval != NULL) {
		if (e->oldvalsz != vsiz || memcmp(e->oldval, val, vsiz)) {
fail:
			e->newver = ver;
			e->newval = malloc(vsiz);
			e->newvalsz = vsiz;
			memcpy(e->newval, vbuf, vsiz);
			return KCVISNOP;
		}
	}

	if (e->oldver != ver)
		goto fail;

	ver++;
	if (ver > 0x7FFFFFFFFFFFFFFELL)
		ver = 1;

	// there is space in front of the value buffer for the version
	putver(e->newval, ver);
	e->newver = ver;
	*sp = e->newvalsz;
	return e->newval;
}

static const char *tsetvisitempty(const char *kbuf, size_t ksiz, size_t *sp, void *opq) {
	tsetentry *e = (tsetentry *) opq;
	e->err = 1;
	return KCVISNOP;
}

static int testset(KCDB *db, const char *kbuf, size_t ksiz, void *opq) {
	return kcdbaccept(db, kbuf, ksiz, &tsetvisit, &tsetvisitempty, opq, 1);
}

*/
import "C"

import (
	"errors"
	"fmt"
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

type KCHop struct {
	sync.RWMutex
	db	*C.KCDB

	// the entries map contains "interesting" entries, i.e. 
	// entries with pending operations (non-existing, or future versions)
	entries	map[string] *entry
	keynumEntry *entry
	keysEntry *entry
}

var Eparams = errors.New("invalid parameter number")
var Enil = errors.New("nil value")
var Einval = errors.New("invalid value")

func NewKCHop(filename string, sync bool) (*KCHop, error) {
	h := new(KCHop)
	h.db = C.kcdbnew()

	cname := C.CString(filename)
	defer C.free(unsafe.Pointer(cname))
	flags := C.uint32_t(C.KCOWRITER | C.KCOCREATE)
	if sync {
		flags |= C.KCOAUTOSYNC
	}

	if C.kcdbopen(h.db, cname, flags) == 0 {
		fmt.Printf("Can't open the database: %s\n", filename)
		return nil, h.error()
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

func (h *KCHop) errorCode() int {
	return int(C.kcdbecode(h.db))
}

func (h *KCHop) error() error {
	ecode := C.kcdbecode(h.db)
	ename := C.GoString(C.kcecodename(ecode))
	return errors.New(ename)
}

func (h *KCHop) Sync() {
       C.kcdbsync(h.db, 0, nil, nil)
}

func (h *KCHop) getKeys(key string) (ver uint64, val []byte, err error) {
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
}

func (h *KCHop) getKeynum() (ver uint64, val []byte, err error) {
	n := C.kcdbcount(h.db)
	if n < 0 {
		err = h.error()
		return
	}

	return h.keynumEntry.version, []byte(fmt.Sprintf("%d", n)), nil
}

func (e *entry) IncreaseVersion() {
	e.version++

	if e.version >= hop.Highest {
		e.version = hop.Lowest
	}
}

func (h *KCHop) keysModified() {
	h.keysEntry.Lock()
	h.keysEntry.IncreaseVersion()
	h.keysEntry.Unlock()
	h.keysEntry.Broadcast()

	h.keynumEntry.Lock()
	h.keynumEntry.IncreaseVersion()
	h.keynumEntry.Unlock()
	h.keynumEntry.Broadcast()
}


func kcvalToValue(kcval []byte) (version uint64, value []byte) {
	version, _ = hop.Gint64(kcval)
	value = kcval[8:]
	return
}

func valueToKcval(version uint64, value []byte) (kcval []byte) {
	kcval = make([]byte, 8 + len(value))
	hop.Pint64(version, kcval)
	copy(kcval[8:], value)
	return
}

func (h *KCHop) Create(key, flags string, value []byte) (version uint64, err error) {
	if strings.HasPrefix(key, "#/") {
		return 0, hop.Eperm
	}

	if value == nil {
		return 0, Enil
	}

	kcval := valueToKcval(hop.Lowest, value)
	bkey := ([]byte)(key)
	ckey := (*C.char)(unsafe.Pointer(&bkey[0]))
	cvalue := (*C.char)(unsafe.Pointer(&kcval[0]))
	if C.kcdbadd(h.db, ckey, C.size_t(len(bkey)), cvalue, C.size_t(len(kcval))) == 0 {
		err = h.error()
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

func (h *KCHop) Remove(key string) (err error) {
	bkey := []byte(key)
	ckey := (*C.char)(unsafe.Pointer(&bkey[0]))
        if C.kcdbremove(h.db, ckey, C.size_t(len(bkey))) == 0 {
                err = h.error()
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
func (h *KCHop) getKcvalue(bkey []byte) (kcval []byte, err error) {
	var vlen C.size_t

	ckey := (*C.char)(unsafe.Pointer(&bkey[0]))
	cval := C.kcdbget(h.db, ckey, C.size_t(len(bkey)), &vlen)
	if cval == nil {
		if h.errorCode() != C.KCENOREC {
			err = h.error()
		} else {
			kcval = nil
		}
	} else {
		kcval = make([]byte, int(vlen))
		C.memcpy(unsafe.Pointer(&kcval[0]), unsafe.Pointer(cval), vlen)
		C.kcfree(unsafe.Pointer(cval))
	}

	return
}

func (h *KCHop) getvalue(key string) (k string, ver uint64, val []byte, err error) {
	k = key
	if strings.HasPrefix(key, "#/keys") {
		ver, val, err = h.getKeys(key)
		k = "#/keys"
	} else if key == "#/keynum" {
		ver, val, err = h.getKeynum()
	} else {
		var kcval []byte

		bkey := []byte(key)
		kcval, err = h.getKcvalue(bkey)
		if err != nil || kcval == nil {
			return
		}

		ver, val = kcvalToValue(kcval)
	}

	return
}

func (h *KCHop) Get(key string, version uint64) (ver uint64, val []byte, err error) {
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
	if e.maxver == version && e != h.keynumEntry && e != h.keysEntry {
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

func (h *KCHop) Set(key string, value []byte) (ver uint64, err error) {
	ver, _, err = h.TestSet(key, hop.Any, nil, value)
	return
}

func (h *KCHop)	TestSet(key string, oldversion uint64, oldvalue, value []byte) (ver uint64, val []byte, err error) {
	var t C.tsetentry

	if value == nil {
		return 0, nil, Enil
	}

	newval := make([]byte, len(value) + 8)
	copy(newval[8:], value)
	cnewval := (*C.char)(unsafe.Pointer(&newval[0]))
	t.oldver = C.uint64_t(oldversion)
	if oldvalue != nil {
		t.oldval = (*C.char)(unsafe.Pointer(&oldvalue[0]))
	} else {
		t.oldval = nil
	}

	t.oldvalsz = C.uint32_t(len(oldvalue))
	t.newval = cnewval
	t.newvalsz = C.uint32_t(len(newval))
	bkey := []byte(key)
	if C.testset(h.db, (*C.char)(unsafe.Pointer(&bkey[0])), C.size_t(len(bkey)), unsafe.Pointer(&t)) == 0 {
		err = h.error()
		return
	}

	if t.err != 0 {
		err = hop.Enoent
		return
	} else {
		ver = uint64(t.newver)
		if t.newval == cnewval {
			val = value
		} else {
			val = make([]byte, t.newvalsz)
			C.memcpy(unsafe.Pointer(&val[0]), unsafe.Pointer(t.newval), C.size_t(t.newvalsz))
			C.kcfree(unsafe.Pointer(t.newval))
			return
		}
	}

	// value was successfully changed, notify waiters...
	h.RLock()
	e := h.entries[key]
	h.RUnlock()
	if e != nil {
		e.Lock()
		if e.version < ver {
			// it is possible that another test&set updates before this one...
			e.version = ver
			e.value = val
		}
		e.Unlock()
		e.Broadcast()
	}

	h.keysModified()
	return
}

func (s *KCHop) Atomic(key string, op uint16, values [][]byte) (ver uint64, vals [][]byte, err error) {
	return 0, nil, errors.New("not implemented")
}
