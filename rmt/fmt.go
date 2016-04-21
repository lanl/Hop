// Copyright 2013 The Hop Authors.  All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package rmt

import (
	"fmt"
	"hop"
)

var atomicNames = map[uint16]string{
	hop.Add:      "add",
	hop.Sub:      "sub",
	hop.BitSet:   "bitset",
	hop.BitClear: "bitclear",
	hop.Append:   "append",
	hop.Remove:   "remove",
	hop.Replace:  "replace",
}

func (m *Msg) String() string {
	ret := ""

	switch m.Type {
	default:
		ret = fmt.Sprintf("invalid message: %d", m.Type)
	case Rerror:
		ret = fmt.Sprintf("Rerror tag %d ename '%s' ecode %d", m.Tag, m.Edescr, m.Ecode)
	case Tget:
		ret = fmt.Sprintf("Tget tag %d key '%s' version %d", m.Tag, m.Key, m.Version)
	case Rget:
		ret = fmt.Sprintf("Rget tag %d version %d datalen %d", m.Tag, m.Version, len(m.Value))
	case Tset:
		ret = fmt.Sprintf("Tset tag %d key '%s' datalen %d", m.Tag, m.Key, len(m.Value))
	case Rset:
		ret = fmt.Sprintf("Rset tag %d version %d", m.Tag, m.Version)
	case Tcreate:
		ret = fmt.Sprintf("Tcreate tag %d key '%s' flags '%s'", m.Tag, m.Key, m.Flags)
	case Rcreate:
		ret = fmt.Sprintf("Rcreate tag %d version %d", m.Tag, m.Version)
	case Tremove:
		ret = fmt.Sprintf("Tremove tag %d key '%s'", m.Tag, m.Key)
	case Rremove:
		ret = fmt.Sprintf("Rset tag %d", m.Tag)
	case Ttestset:
		ret = fmt.Sprintf("Ttestset tag %d key '%s' oldlen %d version %d datalen %d", m.Tag, m.Key, len(m.Oldval), m.Version, len(m.Value))
	case Rtestset:
		ret = fmt.Sprintf("Rtestset tag %d version %d datalen %d", m.Tag, m.Version, len(m.Value))
	case Tatomic:
		opname := atomicNames[m.Atmop]
		if opname=="" {
			opname = fmt.Sprintf("%d", m.Atmop)
		}
		ret = fmt.Sprintf("Tatomic tag %d op '%s' key '%s' vals %v", m.Tag, atomicNames[m.Atmop], m.Key, m.Vals)
	case Ratomic:
		ret = fmt.Sprintf("Ratomic tag %d version %d vals %v", m.Tag, m.Version, m.Vals)
	}

	return ret
}
