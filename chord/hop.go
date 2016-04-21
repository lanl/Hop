// Copyright 2013 The Hop Authors.  All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package chord

import (
	"errors"
	"fmt"
	"hop"
	"runtime"
	"strconv"
	"strings"
)

type LocalEntry struct {
	hop.Entry
	s *Chord
}

type IdEntry LocalEntry
type SuccEntry LocalEntry
//type KeysEntry LocalEntry
type PredEntry LocalEntry
type FingerEntry LocalEntry
type RingEntry LocalEntry
type StackEntry LocalEntry

func (s *Chord) Create(key, flags string, value []byte) (version uint64, err error) {
	nd := s.getNode(key)

//	fmt.Printf("Chord.Create key '%s' hash %016x server %016x:%s\n", key, s.keyhash.Hash(key), h.id, h.addr)
	version, err = nd.clnt.Create(key, flags, value)
	if err != nil {
		s.checkClosed(nd)
	}
	return
}

func (s *Chord) Remove(key string) (err error) {
	nd := s.getNode(key)
	err = nd.clnt.Remove(key)
	if err != nil {
		s.checkClosed(nd)
	}
	return
}

func (s *Chord) Get(key string, version uint64) (ver uint64, val []byte, err error) {
	if strings.HasPrefix(key, "#/") {
/*		if strings.HasPrefix(key, "#/keys:") {
			return s.keysentry.Get(key, version)
		} else */ if strings.HasPrefix(key, "#/chord/successor:") {
			return s.succentry.Get(key, version)
		}

		// next try the local entries
		if s.lents != nil {
			ver, val, err = s.lents.Get(key, version)
			if ver != 0 && err == nil {
				return
			}
		}
	}

	nd := s.getNode(key)
	ver, val, err = nd.clnt.Get(key, version)
	if err != nil {
		s.checkClosed(nd)
	}
	return
}

func (s *Chord) Set(key string, value []byte) (ver uint64, err error) {
	if strings.HasPrefix(key, "#/") {
		// first try the local entries
		ver, err = s.lents.Set(key, value)
		if err == nil || err != hop.Enoent {
			return
		}
	}

	nd := s.getNode(key)
	ver, err = nd.clnt.Set(key, value)
	if err != nil {
		s.checkClosed(nd)
	}
	return
}

func (s *Chord) TestSet(key string, oldversion uint64, oldvalue, value []byte) (ver uint64, val []byte, err error) {
	if strings.HasPrefix(key, "#/") {
		// first try the local entries
		ver, val, err = s.lents.TestSet(key, oldversion, oldvalue, value)
		if err == nil || err != hop.Enoent {
			return
		}
	}

	nd := s.getNode(key)
	ver, val, err = nd.clnt.TestSet(key, oldversion, oldvalue, value)
	if err != nil {
		s.checkClosed(nd)
	}
	return
}

func (s *Chord) Atomic(key string, op uint16, values [][]byte) (ver uint64, vals [][]byte, err error) {
	if strings.HasPrefix(key, "#/") {
		// first try the local entries
		ver, vals, err = s.lents.Atomic(key, op, values)
		if err == nil || err != hop.Enoent {
			return
		}
	}

	nd := s.getNode(key)
	ver, vals, err = nd.clnt.Atomic(key, op, values)
	if err != nil {
		s.checkClosed(nd)
	}
	return
}

/*
func (e *KeysEntry) Get(key string, version uint64) (ver uint64, val []byte, err error) {
	// At the moment, we don't support fancy waiting for future versions.
	// We return the maximum of all version numbers.
	if version != hop.Any && version != hop.Newest {
		return 0, nil, errors.New("unsupported version")
	}

	s := e.s
	kmap := make(map[string]bool)
	if s.lents != nil {
		if vr, vl, e := s.lents.Get(key, version); e == nil {
			ks := bytes.Split(vl, []byte{0})
			for _, k := range ks {
				kmap[string(k)] = true
			}

			ver = vr
		} else {
			err = e
			return
		}
	}

	if s.isServer() {
		if vr, vl, e := e.s.hop.Get(key, version); e == nil {
			ks := bytes.Split(vl, []byte{0})
			for _, k := range ks {
				kmap[string(k)] = true
			}

			if ver < vr {
				ver = vr
			}
		} else {
			err = e
			return
		}
	} else {
		e.s.RLock()
		smap := s.srvmap
		e.s.RUnlock()

		for _, c := range smap {
			if vr, vl, e := c.clnt.Get(key, version); e == nil {
				ks := bytes.Split(vl, []byte{0})

				for _, k := range ks {
					kmap[string(k)] = true
				}

				if ver < vr {
					ver = vr
				}
			} else {
				err = e
				return
			}
		}
	}

	for k, _ := range kmap {
		val = append(val, []byte(k)...)
		val = append(val, 0)
	}

	if len(kmap) > 0 {
		val = val[0 : len(val)-1]
	}

	return
}
*/

func (e *IdEntry) Get(key string, version uint64) (ver uint64, val []byte, err error) {
	return hop.Lowest, []byte(e.s.self.String()), nil
}

func (e *SuccEntry) Get(key string, version uint64) (ver uint64, val []byte, err error) {
	var nd *Node
	var id uint64

	srv := e.s
	if strings.HasPrefix(key, "#/chord/successor:") {
		n, err := strconv.ParseUint(key[18:], 16, 64)
		if err != nil {
			return 0, nil, err
		}

		id = uint64(n)
	} else {
		srv.RLock()
		if srv.finger[0] != nil {
			id = srv.finger[0].id
		} else {
			id = srv.self.id
		}
		srv.RUnlock()
	}

	nd, err = srv.findSuccessor(id)
	if err != nil {
		return 0, nil, err
	}

	ver = hop.Lowest
	val = []byte(nd.String())
	return
}

func (e *PredEntry) Get(key string, version uint64) (ver uint64, val []byte, err error) {
	srv := e.s
	srv.RLock()
	pred := srv.predecessor
	if pred != nil {
		val = []byte(pred.String())
	} else {
		val = []byte{}
	}
	srv.RUnlock()
	ver = hop.Lowest
	return
}

func (e *PredEntry) Atomic(key string, op uint16, values [][]byte) (ver uint64, vals [][]byte, err error) {
	srv := e.s

	if op!=PredAndNotify {
		return 0, nil, errors.New("invalid atomic operation")
	}

	ver = hop.Lowest
	vals = make([][]byte, 1)
	val := string(values[0])
	nd, err := srv.newNode(val)
	if err != nil {
		return 0, nil, err
	}
	
	srv.Lock()
	modified := false
	pred := srv.predecessor
//	if pred != nil {
//		fmt.Printf("AtomicSet: current-predecessor %016x try-predecessor %016x self %016x\n", pred.id, nd.id, srv.self.id)
//	}

	if pred==nil || between(nd.id, pred.id, srv.self.id) {
		err = nd.ConnectLocked()
		if err != nil {
			srv.Unlock()
			return
		}

		pred.DisconnectLocked()
//		if pred == nil {
//			fmt.Printf("new predecessor %016x:%s\n", nd.id, nd.addr)
//		} else {
//			fmt.Printf("new predecessor %016x:%s old %016x:%s\n", nd.id, nd.addr, pred.id, pred.addr)
//		}
		srv.predecessor = nd
		modified = true
	}

	// if we don't know of any successors, start with that node
	if srv.finger[0] == nil {
		err = nd.ConnectLocked()
		if err != nil {
			srv.Unlock()
			return
		}

//		if srv.finger[0] == nil {
//			fmt.Printf("new successor %016x:%s\n", nd.id, nd.addr)
//		}
		srv.finger[0] = nd
		modified = true
	}

	if modified {
		srv.ringModified()
	}
	srv.Unlock()

	ver = hop.Lowest
	vals = make([][]byte, 1)
	if pred!=nil {
		vals[0] = []byte(pred.String())
	} else {
		vals[0] = []byte{}
	}

//	fmt.Printf("AtomicSet: %s return %s\n", val, string(vals[0]))
	return
}

func (e *FingerEntry) Get(key string, version uint64) (ver uint64, val []byte, err error) {
	srv := e.s

	srv.RLock()
	sval := ""
	for i, nd := range srv.finger {
		sval += fmt.Sprintf("%02d:%016x %v\n", i, srv.start(uint(i)), nd)
	}
	srv.RUnlock()

	return hop.Lowest, []byte(sval[0:len(sval) - 1]), nil
}

func (e *RingEntry) Get(key string, version uint64) (ver uint64, val []byte, err error) {
	var nds []*Node
	var nd *Node

	srv := e.s
	for id := srv.self.id + 1;; {
		nd, err = srv.findSuccessor(id)
		if err != nil {
			return 0, nil, err
		}

		nds = append(nds, nd)
		if nd.id == srv.self.id {
			break
		}

		id = nd.id + 1
	}

	srv.RLock()
	sval := ""
	for _, nd := range nds {
		fns := ""
		for i, fn := range srv.finger {
			if fn != nil && nd.addr == fn.addr {
				fns += fmt.Sprintf("%d,", i)
			}
		}

		if len(fns) > 0 {
			fns = fns[0:len(fns) - 1]
		}

		sval += fmt.Sprintf("%v [%s]\n", nd, fns)
	}
	srv.RUnlock()

	return hop.Lowest, []byte(sval[0:len(sval) - 1]), nil
}

func (e *StackEntry) Get(key string, version uint64) (ver uint64, val []byte, err error) {
	buf := make([]byte, 65536)
	n := runtime.Stack(buf, true)

	return hop.Lowest, buf[0:n], nil
}
