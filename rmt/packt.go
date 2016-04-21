// Copyright 2013 The Hop Authors.  All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package rmt

import (
	"errors"
	"hop"
	"math"
)

func PackTget(m *Msg, key string, version uint64) error {
	size := 2 + len(key) + 8 /* key[s] version[8] */
	p, err := packCommon(m, size, Tget)
	if err != nil {
		return err
	}

	m.Key = key
	m.Version = version
	p = hop.Pstr(key, p)
	p = hop.Pint64(version, p)
	return nil
}

func PackTset(m *Msg, key string, value []byte) error {
	size := 2 + len(key) + 4 /* key[s] value[n] */
	if value != nil {
		size += len(value)
	}

	p, err := packCommon(m, size, Tset)
	if err != nil {
		return err
	}

	m.Key = key
	m.Value = value
	p = hop.Pstr(key, p)
	p = hop.Pblob(value, p)
	return nil
}

func PackTcreate(m *Msg, key, flags string, value []byte) error {
	size := 2 + len(key) + 2 + len(flags) + 4 /* key[s] flags[s] value[n] */
	if value != nil {
		size += len(value)
	}

	p, err := packCommon(m, size, Tcreate)
	if err != nil {
		return err
	}

	m.Key = key
	m.Flags = flags
	m.Value = value
	p = hop.Pstr(key, p)
	p = hop.Pstr(flags, p)
	p = hop.Pblob(value, p)
	return nil
}

func PackTremove(m *Msg, key string) error {
	size := 2 + len(key) /* key[s] */
	p, err := packCommon(m, size, Tremove)
	if err != nil {
		return err
	}

	m.Key = key
	p = hop.Pstr(key, p)
	return nil
}

func PackTtestset(m *Msg, key string, version uint64, oldvalue []byte, value []byte) error {
	size := 2 + len(key) + 8 + 4 + 4 /* key[s] version[8] oldvalue[n] value[n] */
	if oldvalue != nil {
		size += len(oldvalue)
	}

	if value != nil {
		size += len(value)
	}

	p, err := packCommon(m, size, Ttestset)
	if err != nil {
		return err
	}

	m.Key = key
	m.Version = version
	m.Oldval = oldvalue
	m.Value = value
	p = hop.Pstr(key, p)
	p = hop.Pint64(version, p)
	p = hop.Pblob(oldvalue, p)
	p = hop.Pblob(value, p)

	return nil
}

func PackTatomic(m *Msg, op uint16, key string, values [][]byte) error {
	size := 2 + 2 + len(key) + 2 /* op[2] key[s] valnum[2] */
	valnum := uint16(0)
	if values != nil {
		if len(values) > math.MaxUint16 {
			return errors.New("too many values")
		}

		valnum = uint16(len(values))
		for _, val := range values {
			size += 4
			if val != nil {
				size += len(val)
			}
		}
	}

	p, err := packCommon(m, size, Tatomic)
	if err != nil {
		return err
	}

	m.Atmop = op
	m.Key = key
	m.Vals = values
	p = hop.Pint16(op, p)
	p = hop.Pstr(key, p)
	p = hop.Pint16(valnum, p)
	for i := uint16(0); i < valnum; i++ {
		p = hop.Pblob(values[i], p)
	}

	return nil
}
