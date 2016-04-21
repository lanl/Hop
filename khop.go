// Copyright 2013 The Hop Authors.  All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package hop

import (
	"errors"
	"fmt"
	"reflect"
	"sync"
)

// KHop is a Hop implementation that supports virtual entries, each of which
// also implements the Hop interface.
// KHop looks for the entry with the specified key in its entries map, and if
// found, and if it implements the required operation, locks the entry and
// calls the operation. If the operation is not implemented, KHop returns
// "not permitted" error.
// The Entry operations should store the current version of the entry in the
// Version field of the Entry. The Value field in the Entry is not used by
// KHop and can be used freely by the implementations.

type KHop struct {
	sync.RWMutex
	entries map[string]*Entry
}

type Entry struct {
	sync.RWMutex
	sync.Cond
	Version uint64
	Value   []byte

	ops interface{}
}

func NewKHop() *KHop {
	h := new(KHop)
	h.InitKHop()

	return h
}

func (h *KHop) InitKHop() {
	h.entries = make(map[string]*Entry)
}

func tryFindEntry(ops interface{}) (e *Entry) {
	if ops == nil {
		return nil
	}

	etype := reflect.TypeOf(e).Elem()

	if reflect.TypeOf(ops).Kind() != reflect.Ptr {
		return nil
	}

	v := reflect.Indirect(reflect.ValueOf(ops))
	t := v.Type()
	if t.Kind() != reflect.Struct {
		return nil
	}

	for idx := 0; idx < t.NumField(); idx++ {
		if t.Field(idx).Type == etype {
			f := v.Field(idx)
			e = f.Addr().Interface().(*Entry)
			break
		}
	}

	return
}

func (h *KHop) FindEntry(key string) (ops interface{}) {
	h.RLock()
	e := h.entries[key]
	if e != nil {
		ops = e.ops
	}
	h.RUnlock()

	return
}

// If ops is a pointer to a struct that has field of type Entry,
// that entry is initialized and its value is put in the entries map.
func (h *KHop) AddEntry(key string, val []byte, ops interface{}) (e *Entry, err error) {
	var oe *Entry

	e = tryFindEntry(ops)
	if e == nil {
//		fmt.Printf("KHop.AddEntry '%s' no ops\n", key)
		e = new(Entry)
	}

	e.L = e.RLocker()
	e.Version = Lowest
	e.Value = val
	e.ops = ops

	h.Lock()
	oe, ok := h.entries[key]
	if !ok || oe.Version == 0 {
		h.entries[key] = e
	}
	h.Unlock()

	if oe == nil {
		return
	}

	if oe.Version==0 {
		// there is a placeholder entry and somebody is waiting
		// for a real entry to be created
		oe.Lock()
		oe.ops = e
		oe.Unlock()
		oe.Broadcast()
	} else {
		e = nil
		err = Eexist
	}

	return
}

func (h *KHop) RemoveEntry(key string) (err error) {
	var e *Entry
	var ok bool

	h.Lock()
	if e, ok = h.entries[key]; ok {
		if e.Version != 0 {
			delete(h.entries, key)
		}
	} else {
		err = Enoent
	}
	h.Unlock()

	if e != nil {
		// inform everybody waiting on the value change that
		// the entry has been removed
		e.Lock()
		if e.Version != 0 {
			// check if it is a fake entry
			e.Version = Removed
			e.ops = nil
			e.Broadcast()
		} else {
			err = Enoent
		}
		e.Unlock()
	}

	return
}

func (h *KHop) NumEntries() (n int) {
	h.RLock()
	n = len(h.entries)
	h.RUnlock()
	return
}

// Calls the visit function for each entry. Use carefully, the h read lock is
// held while the function is called.
func (h *KHop) VisitEntries(visit func(key string, e *Entry)) {
	h.RLock()
	for k, e := range h.entries {
		visit(k, e)
	}
	h.RUnlock()
}

// Exports entries that match to a byte array, that can be used by ImportKeys.
// The exported entries are removed from the datastore and if there are waiters
// on their values, they are informed.
// At the moment, we don't support "special" entries (i.e. entries that have
// ops != nil).
func (h *KHop) ExportEntries(match func(key string) bool) (es []byte) {
	h.Lock()
	for key, e := range h.entries {
		if e.ops != nil {
			continue
		}

		if match(key) {
			delete(h.entries, key)

			e.Lock()
			if e.Version != 0 && e.Version != Removed {
				sz := 2 + len(key) + 8 + 4 + len(e.Value)	// key[s] version[8] value[n]
				buf := make([]byte, sz)
				p := Pstr(key, buf)
				p = Pint64(e.Version, p)
				p = Pblob(e.Value, p)
				es = append(es, buf...)
			}

			e.Version = Removed
			e.Broadcast()
			e.Unlock()
		}
	}
	h.Unlock()

	return
}

// imports the keys previously exported by ExportEntries. If replace is true
// overwrites existing keys. If it is false, returns the list of keys that weren't
// imported because of collisions.
// At the moment, we don't support "special" entries (i.e. entries that have
// ops != nil).
func (h *KHop) ImportKeys(es []byte, replace bool) (rejected []string, err error) {
	h.Lock()
	defer h.Unlock()

	for es!=nil && len(es) > 0 {
		var key string
		var version uint64
		var value []byte

		if len(es) < 14 {
			goto error
		}

		key, es = Gstr(es)
		if es==nil || len(es) < 8 {
			goto error
		}

		version, es = Gint64(es)
		if es==nil || len(es) < 4 {
			goto error
		}

		value, es = Gblob(es)

		if e, ok := h.entries[key]; ok {
			if !replace {
				rejected = append(rejected, key)
			} else {
				// we need to inform the waiters that the 
				// entry was removed
				e.Lock()
				e.Version = Removed
				e.Broadcast()
				e.Unlock()
			}
		}

		e := new(Entry)
		e.L = e.RLocker()
		e.Version = version
		e.Value = value
		h.entries[key] = e
	}

	return

error:
	return nil, errors.New("invalid entries")
}

func (h *KHop) Create(key, flags string, value []byte) (ver uint64, err error) {
	return 0, Eperm
}

func (h *KHop) Remove(key string) (err error) {
	return Eperm
}

func (h *KHop) Get(key string, version uint64) (ver uint64, val []byte, err error) {
again:
	h.RLock()
	e := h.entries[key]
	h.RUnlock()

	if e == nil {
		if version==Any || version==Newest {
			// no entry found, no instructions to wait
			// for specific version
			return
		}

		// create an entry that everybody can wait on
		e = new(Entry)
		e.L = e.RLocker()

		h.Lock()
		if _, ok := h.entries[key]; ok {
			// the entry was added while we were running
			h.Unlock()
			goto again
		} else {
			h.entries[key] = e
		}
		h.Unlock()
	}

	e.RLock()
	if e.Version==0 && e.ops != nil {
		ne := e.ops.(*Entry)
		e.RUnlock()
		e = ne
		e.RLock()
	}

	ver = e.Version
	oldver := ver
	switch version {
	case PastNewest:
		version = ver + 1
	case Newest, Any:
		version = ver
	}

	for ver != Removed && ver < version {
		e.Wait()
		if oldver==0 {
			// we were waiting for an entry to be created,
			// it was, and a pointer to it was assigned to
			// current entry's ops field
			ne := e.ops.(*Entry)
			e.RUnlock()
			e = ne
			e.RLock()
		}

		ver = e.Version
	}

	ver = e.Version
	val = e.Value
	ops := e.ops
	e.RUnlock()

	if ver == Removed {
		// the entry was removed
		ver = 0
		val = nil
	} else 	if ops != nil {
		if ghop, ok := ops.(GetterHop); ok {
			defer func() {
				if r := recover(); r != nil {
					fmt.Printf("Panic: key %s e %p e.ops %v ver %d ghop %v panic %v\n", key, e, ops, ver, ghop, r)
				}
			} ()

			ver, val, err = ghop.Get(key, version)
		} else {
			ver = 0
			err = Eperm
		}
	}

	return
}

func (h *KHop) Set(key string, value []byte) (ver uint64, err error) {
	h.RLock()
	e, ok := h.entries[key]
	if ok && e.Version == 0 {
		// we only have the fake entry
		ok = false
	}
	h.RUnlock()

	if !ok {
		return 0, nil
	}

	e.RLock()
	oldver := e.Version
	shop, ok := e.ops.(SetterHop)
	e.RUnlock()

	if !ok {
		return 0, Eperm
	}

	ver, err = shop.Set(key, value)
	if err == nil && ver != oldver {
		e.Modified()
	}

	return
}

func (h *KHop) TestSet(key string, oldversion uint64, oldvalue, value []byte) (ver uint64, val []byte, err error) {
	h.RLock()
	e, ok := h.entries[key]
	if ok && e.Version == 0 {
		// we only have the fake entry
		ok = false
	}
	h.RUnlock()

	if !ok {
		return 0, nil, nil
	}

	e.RLock()
	oldver := e.Version
	tshop, ok := e.ops.(TestSetterHop)
	e.RUnlock()

	if !ok {
		return 0, nil, Eperm
	}

	ver, val, err = tshop.TestSet(key, oldversion, oldvalue, value)
	if err == nil && ver != oldver {
		e.Modified()
	}

	return
}

func (h *KHop) Atomic(key string, op uint16, values [][]byte) (ver uint64, vals [][]byte, err error) {
	h.RLock()
	e, ok := h.entries[key]
	if ok && e.Version == 0 {
		// we only have the fake entry
		ok = false
	}
	h.RUnlock()

	if !ok {
		return 0, nil, nil
	}

	e.RLock()
	oldver := e.Version
	ashop, ok := e.ops.(AtomicHop)
	e.RUnlock()

	if !ok {
		return 0, nil, Eperm
	}

	ver, vals, err = ashop.Atomic(key, op, values)
	if err == nil && ver != oldver {
		e.Modified()
	}

	return
}

// should be called with e lock held
func (e *Entry) IncreaseVersion() {
	e.Version++

	if e.Version >= Highest {
		e.Version = Lowest
	}
}

func (e *Entry) Modified() {
	e.Broadcast()
}

func (e *Entry) SetValue(val []byte) {
	v := make([]byte, len(val))
	copy(v, val)
	e.Lock()
	e.IncreaseVersion()
	e.Value = v
	e.Unlock()

	e.Modified()
}

// called with e lock held
func (e *Entry) SetLocked(val []byte) {
	v := make([]byte, len(val))
	copy(v, val)
	e.IncreaseVersion()
	e.Value = v
	e.Modified()
}

func (e *Entry) SetEntry(version uint64, val []byte) {
	v := make([]byte, len(val))
	copy(v, val)
	e.Lock()
	e.Version = version
	e.Value = v
	e.Unlock()

	e.Modified()
}
