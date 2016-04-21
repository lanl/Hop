// Copyright 2013 The Hop Authors.  All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package dhop

import (
	"bytes"
	"errors"
	"fmt"
	"hop"
	"hop/rmt"
	"hop/rmt/hopclnt"
	"hop/rmt/hopsrv"
	"regexp"
	"strings"
	"sync"
)

type Entry struct {
	sync.Mutex
	sync.Cond
	key     string
	version uint64
	value   []byte
}

type Conn struct {
	srv  *DHop
	conn *hopsrv.Conn
}

type DHop struct {
	hopsrv.Srv

	sync.RWMutex
	//	disp	*Dispatcher
	addr   string
	data   map[string]*Entry
	peers  []*Peer
	conns  map[string]*Conn
	master *hopclnt.Clnt

	// local entries
	keysEntry   *Entry // list of keys
	keynumEntry *Entry // number of keys
	peersEntry  *Entry // list of peers
}

//type SrvLocalEntry struct {
//	Entry
//	srv	*DHop
//}
//
//type ConnLocalEntry struct {
//	Entry
//	conn	*Conn
//}

//type KeynumEntry SrvLocalEntry
//type KeysEntry SrvLocalEntry
//type PeersEntry ConnLocalEntry

type Peer struct {
	addr string
	clnt *hopclnt.Clnt
}

func NewDHop(proto, addr, masteraddr string) (*DHop, error) {
	s := new(DHop)
	s.addr = addr
	if err := rmt.Listen(proto, addr, s); err != nil {
		return nil, err
	}

	fmt.Printf("Listening on %v\n", s.addr)
	s.data = make(map[string]*Entry)
	s.conns = make(map[string]*Conn)
	s.keynumEntry = s.newEntry("#/keynum", nil)
	s.keysEntry = s.newEntry("#/keys", nil)
	s.peersEntry = s.newEntry("#/peers", []byte(s.addr))

	if !s.Start(s) {
		return nil, errors.New("Error: can't start the server\n")
	}

	s.Id = "DHop"
	if masteraddr != "" {
		if c, err := rmt.Connect(proto, masteraddr); err == nil {
			s.master = hopclnt.NewClnt(c)
			s.NewConn(c)
		} else {
			return nil, err
		}

		go s.confproc()
	}

	return s, nil
}

func (s *DHop) ConnOpened(c *hopsrv.Conn) {
	fmt.Printf("ConnOpened: %v\n", c)
	conn := new(Conn)
	conn.srv = s
	conn.conn = c
	c.SetOps(conn)

	s.Lock()
	s.conns[c.RemoteAddr()] = conn
	s.Unlock()
}

func (s *DHop) ConnClosed(c *hopsrv.Conn) {
	s.Lock()
	delete(s.conns, c.RemoteAddr())
	s.Unlock()
}

// called with the s lock held
func (s *DHop) newEntry(key string, val []byte) *Entry {
	e := new(Entry)
	e.L = &e.Mutex
	e.key = key
	e.version = 1
	e.value = val

	s.data[key] = e
	return e
}

// called with the e lock held
func entryModified(e *Entry) {
	//	fmt.Printf("entryModified: %s\n", e.key)
	e.version++
	e.Broadcast()
}

func (s *DHop) confproc() {
	ver, val, err := s.master.AtomicSet("#/peers", hop.Append, []byte(fmt.Sprintf(",%s", s.addr)))
	if err != nil {
		fmt.Printf("Error: %v\n", err)
		return
	}

	for {
		s.peersEntry.Lock()
		s.peersEntry.value = val
		s.peersEntry.version = ver
		entryModified(s.peersEntry)
		s.peersEntry.Unlock()

		ver, val, err = s.master.Get("#/peers", ver+1)
		if err != nil {
			fmt.Printf("Error: %v\n", err)
			break
		}

		fmt.Printf("Peers: %s\n", string(val))
	}
}

func (c *Conn) Create(key, flags string, value []byte) (version uint64, err error) {
	s := c.srv
	if value == nil {
		return 0, errors.New("nil value")
	}

	val := make([]byte, len(value))
	copy(val, value)

	s.Lock()
	if _, ok := s.data[key]; ok {
		err = errors.New("key exists")
	} else {
		e := s.newEntry(key, val)
		version = e.version

		// inform everybody waiting on key list about the list change
		entryModified(s.keysEntry)
		entryModified(s.keynumEntry)
	}

	s.Unlock()
	return
}

func (c *Conn) Remove(key string) (err error) {
	s := c.srv
	s.Lock()
	e, ok := s.data[key]
	if ok {
		delete(s.data, key)
		e.Lock()
		e.key = ""
		e.Unlock()
	} else {
		err = errors.New("key not found")
	}

	// inform everybody waiting on key list about the list change
	entryModified(s.keysEntry)
	entryModified(s.keynumEntry)
	s.Unlock()

	if e != nil {
		// inform everybody waiting on value change that the value
		// was removed
		e.Lock()
		e.key = ""
		e.Unlock()
		e.Broadcast()
	}

	return
}

func (c *Conn) getLocalEntry(key string) *Entry {
	s := c.srv
	if key == "#/keynum" {
		return s.keynumEntry
	} else if strings.HasPrefix(key, "#/keys") {
		return s.keysEntry
	} else if key == "#/peers" {
		return s.peersEntry
	}

	return nil
}

func (c *Conn) getLocalValue(key string, e *Entry) (version uint64, val []byte, err error) {
	s := c.srv
	if e == s.keynumEntry {
		s.Lock()
		val = []byte(fmt.Sprintf("%d", len(s.data)))
		s.Unlock()
	} else if e == s.keysEntry {
		rexp := ".*"
		if strings.HasPrefix(key, "#/keys:") {
			rexp = key[7:]
		}

		re, err := regexp.Compile(rexp)
		if err != nil {
			return 0, nil, err
		}

		sv := ""
		for k, _ := range s.data {
			if re.MatchString(k) {
				sv = sv + "," + k
			}
		}

		if len(sv) > 1 {
			val = []byte(sv[1:])
		} else {
			val = []byte{}
		}
	} else if e == s.peersEntry {
		val = s.peersEntry.value
	} else {
		// shouldn't happen
		err = errors.New("invalid local key")
	}

	version = e.version
	return
}

func (c *Conn) Get(key string, version uint64) (ver uint64, val []byte, err error) {
	s := c.srv
	s.RLock()
	e := s.data[key]
	if e == nil && strings.HasPrefix(key, "#/") {
		e = c.getLocalEntry(key)
	}
	s.RUnlock()

	if e == nil {
		return 0, nil, errors.New("key not found")
	}

	e.Lock()
	defer e.Unlock()
	switch version {
	case hop.PastNewest:
		version = e.version + 1
	case hop.Newest, hop.Any:
		version = e.version
	}

	for e.version < version && e.key != "" {
		e.Wait()
	}

	if e.key == "" {
		// the entry was removed
		return 0, nil, errors.New("key removed")
	}

	if strings.HasPrefix(key, "#/") {
		return c.getLocalValue(key, e)
	} else {
		return e.version, e.value, nil
	}
}

func (c *Conn) Set(key string, value []byte) (ver uint64, err error) {
	ver, _, err = c.TestSet(key, 0, nil, value)
	return
}

// called with the e lock held
func (c *Conn) setLocalValue(key string, e *Entry, value []byte) (val []byte, err error) {
	//	fmt.Printf("setLocalValue: %s\n", key)
	s := c.srv
	if e != s.peersEntry {
		return nil, errors.New("read-only value")
	}

	// setup a new peer
	p := new(Peer)
	p.addr = c.conn.RemoteAddr()
	p.clnt = hopclnt.NewClnt(c.conn.Conn)
	s.Lock()
	s.peers = append(s.peers, p)
	e.value = value
	s.Unlock()

	return value, nil
}

func (c *Conn) TestSet(key string, oldversion uint64, oldvalue, value []byte) (ver uint64, val []byte, err error) {
	s := c.srv
	localEntry := strings.HasPrefix(key, "#/")
	s.RLock()
	e := s.data[key]
	if e == nil && localEntry {
		e = c.getLocalEntry(key)
	}
	s.RUnlock()

	if e == nil {
		return 0, nil, errors.New("key not found")
	}

	e.Lock()
	defer e.Unlock()
	if oldversion == hop.Any {
		oldversion = e.version
	} else if oldversion < hop.Lowest || oldversion > hop.Highest {
		return 0, nil, errors.New("invalid version")
	}

	evalue := e.value
	if localEntry {
		_, evalue, err = c.getLocalValue(key, e)
		if err != nil {
			goto done
		}
	}

	if oldversion != e.version {
		goto done
	}

	if oldvalue != nil {
		if len(oldvalue) != len(evalue) {
			goto done
		}

		for i, s := range oldvalue {
			if s != evalue[i] {
				goto done
			}
		}
	}

	if localEntry {
		evalue, err = c.setLocalValue(key, e, value)
	} else {
		evalue = make([]byte, len(value))
		copy(evalue, value)
		e.value = value
	}

	if err == nil {
		entryModified(e)
	}

done:
	return e.version, evalue, err
}

func (c *Conn) AtomicSet(key string, op uint16, value []byte) (ver uint64, val []byte, err error) {
	s := c.srv
	localEntry := strings.HasPrefix(key, "#/")
	s.RLock()
	e := s.data[key]
	if e == nil && localEntry {
		e = c.getLocalEntry(key)
	}
	s.RUnlock()

	if e == nil {
		return 0, nil, errors.New("key not found")
	}

	e.Lock()
	defer e.Unlock()

	evalue := e.value
	if localEntry {
		_, evalue, err = c.getLocalValue(key, e)
		if err != nil {
			return 0, nil, err
		}
	}

	switch op {
	default:
		return 0, nil, errors.New("invalid atomic operation")

	case hop.AtomicAdd:
		val, err = atomicAdd(evalue, value, 1)

	case hop.AtomicSub:
		val, err = atomicAdd(evalue, value, -1)

	case hop.BitSet:
		if value != nil {
			val := make([]byte, len(evalue))
			copy(val, evalue)
			for i := 0; i < len(val) && i < len(value); i++ {
				val[i] |= value[i]
			}
			evalue = val
		} else {
			i := 0
			for ; i < len(evalue); i++ {
				if evalue[i] != 0xff {
					break
				}
			}

			if i >= len(evalue) {
				return 0, nil, errors.New("all bits already set")
			}

			val = make([]byte, len(evalue))
			copy(val, evalue)
			evalue = val

			val = make([]byte, len(evalue))
			for b := uint8(1); b != 0; b = b << 1 {
				if evalue[i]&b == 0 {
					evalue[i] |= b
					val[i] |= b
					break
				}
			}
		}

	case hop.BitClear:
		if value != nil {
			val := make([]byte, len(evalue))
			copy(val, evalue)
			for i := 0; i < len(val) && i < len(value); i++ {
				val[i] &= value[i]
			}
			evalue = val
		} else {
			i := 0
			for ; i < len(evalue); i++ {
				if evalue[i] != 0 {
					break
				}
			}

			if i >= len(evalue) {
				return 0, nil, errors.New("all bits already cleared")
			}

			val = make([]byte, len(evalue))
			copy(val, evalue)
			evalue = val

			val = make([]byte, len(evalue))
			for b := uint8(1); b != 0; b = b << 1 {
				if evalue[i]&b == 1 {
					evalue[i] &= ^b
					val[i] |= b
					break
				}
			}
		}

	case hop.Append:
		if value != nil {
			val = make([]byte, len(evalue)+len(value))
			copy(val, evalue)
			copy(val[len(evalue):], value)
			evalue = val
		}

	case hop.Remove:
		if value != nil {
			val = bytes.Replace(evalue, value, []byte{}, -1)
			if len(val) != len(evalue) {
				evalue = val
			} else {
				val = nil
			}
		}
	}

	if val != nil {
		if localEntry {
			_, err = c.setLocalValue(key, e, evalue)
		} else {
			e.value = evalue
		}

		if err == nil {
			entryModified(e)
		}
	}

	return e.version, val, nil
}

func atomicAdd(v []byte, n []byte, sign int) (val []byte, err error) {
	if len(v) != len(n) {
		return nil, errors.New("type mismatch")
	}

	val = make([]byte, len(v))
	switch len(n) {
	default:
		return nil, errors.New("invalid integer size")

	case 1:
		vv, _ := rmt.Gint8(v)
		nn, _ := rmt.Gint8(n)
		if sign > 0 {
			vv += nn
		} else {
			vv -= nn
		}
		rmt.Pint8(vv, val)

	case 2:
		vv, _ := rmt.Gint16(v)
		nn, _ := rmt.Gint16(n)
		if sign > 0 {
			vv += nn
		} else {
			vv -= nn
		}
		rmt.Pint16(vv, val)

	case 4:
		vv, _ := rmt.Gint32(v)
		nn, _ := rmt.Gint32(n)
		if sign > 0 {
			vv += nn
		} else {
			vv -= nn
		}
		rmt.Pint32(vv, val)

	case 8:
		vv, _ := rmt.Gint64(v)
		nn, _ := rmt.Gint64(n)
		if sign > 0 {
			vv += nn
		} else {
			vv -= nn
		}
		rmt.Pint64(vv, val)
	}

	return val, nil
}
