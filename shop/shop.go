// Copyright 2013 The Hop Authors.  All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package shop

import (
	"bytes"
	"errors"
	"fmt"
	"hop"
	"regexp"
	"strings"
)

// simple entry (all entries created by the client)
type SEntry struct {
	hop.Entry
}

type LocalEntry struct {
	hop.Entry
	s *SHop
}

// #/keynum entry
type KeynumEntry LocalEntry // #/keynum
type KeysEntry LocalEntry   // #/keys

type SHop struct {
	hop.KHop

	// local entries
	keysEntry   KeysEntry
	keynumEntry KeynumEntry
}

var Eparams = errors.New("invalid parameter number")
var Enil = errors.New("nil value")

func NewSHop() *SHop {
	s := new(SHop)
	s.InitKHop()

	s.keysEntry.s = s
	s.AddEntry("#/keys", nil, &s.keysEntry)

	s.keynumEntry.s = s
	s.AddEntry("#/keynum", nil, &s.keynumEntry)

	s.AddEntry("#/id", []byte("SHop"), nil)
	return s
}

func (e *KeynumEntry) Get(key string, version uint64) (ver uint64, val []byte, err error) {
	return e.Version, []byte(fmt.Sprintf("%d", e.s.NumEntries())), nil
}

func (e *KeysEntry) Get(key string, version uint64) (ver uint64, val []byte, err error) {
	var re *regexp.Regexp

	if strings.HasPrefix(key, "#/keys:") {
		re, err = regexp.Compile(key[7:])
		if err != nil {
			return
		}
	}

	val = []byte{}
	e.s.VisitEntries(func (key string, e *hop.Entry) {
		if re==nil || re.MatchString(key) {
			val = append(val, []byte(key)...)
			val = append(val, 0)
		}
	})

	if len(val) > 0 {
		// remove the trailing zero
		val = val[0 : len(val)-1]
	}

	ver = e.Version
	return
}

func (s *SHop) keysModified() {
	s.keysEntry.Lock()
	s.keysEntry.IncreaseVersion()
	s.keysEntry.Unlock()
	s.keysEntry.Modified()

	s.keynumEntry.Lock()
	s.keynumEntry.IncreaseVersion()
	s.keynumEntry.Unlock()
	s.keynumEntry.Modified()
}

func (s *SHop) Create(key, flags string, value []byte) (version uint64, err error) {
	if strings.HasPrefix(key, "#/") {
		return 0, hop.Eperm
	}

	if value == nil {
		return 0, Enil
	}

	val := make([]byte, len(value))
	copy(val, value)

	_, err = s.AddEntry(key, val, new(SEntry))
	if err != nil {
		return
	}

	s.keysModified()
	return hop.Lowest, nil
}

func (s *SHop) Remove(key string) (err error) {
	err = s.RemoveEntry(key)

	if err != nil {
		s.keysModified()
	}

	return
}

func (s *SHop) Get(key string, version uint64) (ver uint64, val []byte, err error) {
	if strings.HasPrefix(key, "#/keys:") {
		return s.keysEntry.Get(key, version)
	}

	return s.KHop.Get(key, version)
}

func (e *SEntry) Get(key string, version uint64) (ver uint64, val []byte, err error) {
	if e==nil {
		panic("SEntry.Get!!!")
	}

	e.RLock()
	defer e.RUnlock()

	return e.Version, e.Value, nil
}

func (e *SEntry) Set(key string, value []byte) (ver uint64, err error) {
	ver, _, err = e.TestSet(key, hop.Any, nil, value)
	return
}

func (e *SEntry) TestSet(key string, oldversion uint64, oldvalue, value []byte) (ver uint64, val []byte, err error) {
	e.Lock()
	defer e.Unlock()

	if oldversion == hop.Any {
		oldversion = e.Version
	} else if oldversion < hop.Lowest || oldversion > hop.Highest {
		return 0, nil, errors.New("invalid version")
	}

	if oldversion != e.Version {
		goto done
	}

	if oldvalue != nil {
		if len(oldvalue) != len(e.Value) {
			goto done
		}

		for i, s := range oldvalue {
			if s != e.Value[i] {
				goto done
			}
		}
	}

	e.IncreaseVersion()
	ver = e.Version

	val = make([]byte, len(value))
	copy(val, value)
	e.Value = val

done:
	return
}

func (e *SEntry) Atomic(key string, op uint16, values [][]byte) (ver uint64, retvals [][]byte, err error) {
	var oldval, val []byte

	e.Lock()
	defer e.Unlock()
	valnum := 0
	if values != nil {
		valnum = len(values)
	}

	ver = e.Version
	oldval = e.Value
	switch op {
	default:
		return 0, nil, errors.New("invalid atomic operation")

	case hop.Add:
		if valnum != 1 {
			return 0, nil, Eparams
		}

		val, err = atomicAdd(oldval, values[0], 1)
		retvals = [][]byte{val}

	case hop.Sub:
		if valnum != 1 {
			return 0, nil, Eparams
		}

		val, err = atomicAdd(oldval, values[0], -1)
		retvals = [][]byte{val}

	case hop.BitSet:
		val, retvals, err = atomicBitSet(oldval, values)

	case hop.BitClear:
		val, retvals, err = atomicBitClear(oldval, values)

	case hop.Append:
		if valnum != 1 {
			return 0, nil, Eparams
		}

		val = make([]byte, len(oldval)+len(values[0]))
		copy(val, oldval)
		copy(val[len(oldval):], values[0])
		retvals = [][]byte{val}

	case hop.Remove:
		if valnum != 1 {
			return 0, nil, Eparams
		}

		ret := bytes.Replace(oldval, values[0], []byte{}, -1)
		if len(ret) != len(oldval) {
			val = ret
		}

		retvals = [][]byte{ret}

	case hop.Replace:
		if valnum != 2 {
			return 0, nil, Eparams
		}

		ret := bytes.Replace(oldval, values[0], values[1], -1)
		if len(ret) != len(oldval) {
			val = ret
		}

		retvals = [][]byte{ret}
	}

	if val != nil {
		e.IncreaseVersion()
		e.Value = val
	} else {
		val = e.Value
	}

	ver = e.Version
	return
}

// v is locked
func atomicAdd(v, n []byte, sign int) (val []byte, err error) {
	if len(v) != len(n) {
		return nil, errors.New("type mismatch")
	}

	val = make([]byte, len(v))
	switch len(n) {
	default:
		return nil, errors.New("invalid integer size")

	case 1:
		vv, _ := hop.Gint8(v)
		nn, _ := hop.Gint8(n)
		if sign > 0 {
			vv += nn
		} else {
			vv -= nn
		}
		hop.Pint8(vv, val)

	case 2:
		vv, _ := hop.Gint16(v)
		nn, _ := hop.Gint16(n)
		if sign > 0 {
			vv += nn
		} else {
			vv -= nn
		}
		hop.Pint16(vv, val)

	case 4:
		vv, _ := hop.Gint32(v)
		nn, _ := hop.Gint32(n)
		if sign > 0 {
			vv += nn
		} else {
			vv -= nn
		}
		hop.Pint32(vv, val)

	case 8:
		vv, _ := hop.Gint64(v)
		nn, _ := hop.Gint64(n)
		if sign > 0 {
			vv += nn
		} else {
			vv -= nn
		}
		hop.Pint64(vv, val)
	}

	return val, nil
}

func atomicBitSet(oldval []byte, values [][]byte) (val []byte, retvals [][]byte, err error) {
	if values != nil {
		// bitset with a value is equivalent to bitwise OR
		value := values[0]
		val = make([]byte, len(oldval))
		copy(val, oldval)
		for i := 0; i < len(val) && i < len(value); i++ {
			val[i] |= value[i]
		}

		retvals = [][]byte{val}
	} else {
		i := 0
		for ; i < len(oldval); i++ {
			if oldval[i] != 0xff {
				break
			}
		}

		if i >= len(oldval) {
			return nil, nil, errors.New("all bits already set")
		}

		val = make([]byte, len(oldval))
		copy(val, oldval)
		bitnum := uint32(i * 8)
		for b, n := uint8(1), 0; b != 0; b, n = b<<1, n+1 {
			if val[i]&b == 0 {
				val[i] |= b
				bitnum += uint32(n)
				break
			}
		}

		ba := make([]byte, 4)
		hop.Pint32(bitnum, ba)
		retvals = [][]byte{val, ba}
	}

	return
}

func atomicBitClear(oldval []byte, values [][]byte) (val []byte, retvals [][]byte, err error) {
	if values != nil {
		// bitclear with a value is equivalent to bitwise AND
		value := values[0]
		val = make([]byte, len(oldval))
		copy(val, oldval)
		for i := 0; i < len(val) && i < len(value); i++ {
			val[i] &= value[i]
		}

		retvals = [][]byte{val}
	} else {
		i := 0
		for ; i < len(oldval); i++ {
			if oldval[i] != 0 {
				break
			}
		}

		if i >= len(oldval) {
			return nil, nil, errors.New("all bits already cleared")
		}

		val = make([]byte, len(oldval))
		copy(val, oldval)
		bitnum := uint32(i * 8)
		for b, n := uint8(1), 0; b != 0; b, n = b<<1, n+1 {
			if val[i]&b == 1 {
				val[i] &= ^b
				bitnum += uint32(n)
				break
			}
		}

		ba := make([]byte, 4)
		hop.Pint32(bitnum, ba)
		retvals = [][]byte{val, ba}
	}

	return
}
