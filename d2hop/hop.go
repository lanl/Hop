// Copyright 2013 The Hop Authors.  All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package d2hop

import (
	"bytes"
	"errors"
	_"fmt"
	"hop"
	"runtime"
	"strings"
	"time"
)

type LocalEntry struct {
	hop.Entry
	s *D2Hop
}

type ConfEntry LocalEntry
type KeysEntry LocalEntry
type StackEntry LocalEntry

func (c *Conn) Create(key, flags string, value []byte) (version uint64, err error) {
	c.alive = time.Now()
	return c.srv.Create(key, flags, value)
}

func (c *Conn) Remove(key string) (err error) {
	c.alive = time.Now()
	return c.srv.Remove(key)
}

func (c *Conn) Get(key string, version uint64) (ver uint64, val []byte, err error) {
	c.alive = time.Now()
	return c.srv.Get(key, version)
}

func (c *Conn) Set(key string, value []byte) (ver uint64, err error) {
	c.alive = time.Now()
	if c.srv.isServer() && key == "#/ctl" {
		// #/ctl is special, its behavior depends on the connection
		err = c.srv.ctl(c, string(value))
		ver = hop.Lowest
		return
	}

	return c.srv.Set(key, value)
}

func (c *Conn) TestSet(key string, oldversion uint64, oldvalue, value []byte) (ver uint64, val []byte, err error) {
	c.alive = time.Now()
	return c.srv.TestSet(key, oldversion, oldvalue, value)
}

func (c *Conn) Atomic(key string, op uint16, values [][]byte) (ver uint64, vals [][]byte, err error) {
	c.alive = time.Now()
	return c.srv.Atomic(key, op, values)
}

func (s *D2Hop) Create(key, flags string, value []byte) (version uint64, err error) {
	c := s.getServer(key)
	version, err = c.clnt.Create(key, flags, value)
	if err == nil {
		c.alive = time.Now()
	}
	return
}

func (s *D2Hop) Remove(key string) (err error) {
	c := s.getServer(key)
	err = c.clnt.Remove(key)
	if err == nil {
		c.alive = time.Now()
	}
	return
}

func (s *D2Hop) Get(key string, version uint64) (ver uint64, val []byte, err error) {
	if strings.HasPrefix(key, "#/") {
		if strings.HasPrefix(key, "#/keys") {
			return s.keysentry.Get(key, version)
		}

		// next try the local entries
		if s.lents != nil {
			ver, val, err = s.lents.Get(key, version)
			if ver != 0 && err == nil {
				return
			}
		}
	}

	c := s.getServer(key)
	ver, val, err = c.clnt.Get(key, version)
	if err == nil {
		c.alive = time.Now()
	}
	return
}

func (s *D2Hop) Set(key string, value []byte) (ver uint64, err error) {
	if strings.HasPrefix(key, "#/") {
		// first try the local entries
		ver, err = s.lents.Set(key, value)
		if err == nil || err != hop.Enoent {
			return
		}
	}

	c := s.getServer(key)
	ver, err = c.clnt.Set(key, value)
	if err == nil {
		c.alive = time.Now()
	}
	return
}

func (s *D2Hop) TestSet(key string, oldversion uint64, oldvalue, value []byte) (ver uint64, val []byte, err error) {
	if strings.HasPrefix(key, "#/") {
		// first try the local entries
		ver, val, err = s.lents.TestSet(key, oldversion, oldvalue, value)
		if err == nil || err != hop.Enoent {
			return
		}
	}

	c := s.getServer(key)
	ver, val, err = c.clnt.TestSet(key, oldversion, oldvalue, value)
	if err == nil {
		c.alive = time.Now()
	}
	return
}

func (s *D2Hop) Atomic(key string, op uint16, values [][]byte) (ver uint64, vals [][]byte, err error) {
	if strings.HasPrefix(key, "#/") {
		// first try the local entries
		ver, vals, err = s.lents.Atomic(key, op, values)
		if err == nil || err != hop.Enoent {
			return
		}
	}

	c := s.getServer(key)
	ver, vals, err = c.clnt.Atomic(key, op, values)
	if err == nil {
		c.alive = time.Now()
	}
	return
}

func (e *ConfEntry) Get(key string, version uint64) (ver uint64, val []byte, err error) {
	return e.Version, e.Value, nil
}

func (e *ConfEntry) Atomic(key string, op uint16, values [][]byte) (ver uint64, retvals [][]byte, err error) {
	s := e.s

	if !s.isMaster() {
		return 0, nil, errors.New("not a master")
	}

	if len(values) != 1 {
		return 0, nil, errors.New("invalid parameter number")
	}

	value := values[0]
	switch op {
	case hop.Append:
		s.masterAddServer(string(value))

	case hop.Remove:
		err = s.masterRemoveServer(string(value))

	default:
		return 0, nil, hop.Eperm
	}

	if err != nil {
		return 0, nil, err
	}

	return e.Version, [][]byte{e.Value}, nil
}

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

func (e *StackEntry) Get(key string, version uint64) (ver uint64, val []byte, err error) {
	buf := make([]byte, 256*1024)
	n := runtime.Stack(buf, true)

	return hop.Lowest, buf[0:n], nil
}
