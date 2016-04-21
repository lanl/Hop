// Copyright 2013 The Hop Authors.  All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package rmt

import (
	"errors"
	"hop"
	"math"
)

func PackRerror(m *Msg, edescr string, ecode uint32) error {
	size := 4 + 2 + len(edescr) /* ecode[4] edescr[s] */
	p, err := packCommon(m, size, Rerror)
	if err != nil {
		return err
	}

	m.Edescr = edescr
	m.Ecode = ecode
	p = hop.Pint32(ecode, p)
	hop.Pstr(edescr, p)

	return nil
}

func PackRget(m *Msg, version uint64, value []byte) error {
	size := 8 + 4 /* version[8] value[n] */
	if value != nil {
		size += len(value)
	}

	p, err := packCommon(m, size, Rget)
	if err != nil {
		return err
	}

	m.Version = version
	m.Value = value
	p = hop.Pint64(version, p)
	hop.Pblob(value, p)

	return nil
}

func PackRset(m *Msg, version uint64) error {
	size := 8 /* version[8] */
	p, err := packCommon(m, size, Rset)
	if err != nil {
		return err
	}

	m.Version = version
	hop.Pint64(version, p)

	return nil
}

func PackRcreate(m *Msg, version uint64) error {
	size := 8 /* version[8] */
	p, err := packCommon(m, size, Rcreate)
	if err != nil {
		return err
	}

	m.Version = version
	hop.Pint64(version, p)

	return nil
}

func PackRremove(m *Msg) error {
	size := 0
	_, err := packCommon(m, size, Rremove)
	return err
}

func PackRtestset(m *Msg, version uint64, value []byte) error {
	size := 8 + 4 /* version[8] value[n] */
	if value != nil {
		size += len(value)
	}

	p, err := packCommon(m, size, Rtestset)
	if err != nil {
		return err
	}

	m.Version = version
	m.Value = value
	p = hop.Pint64(version, p)
	hop.Pblob(value, p)

	return nil
}

func PackRatomic(m *Msg, version uint64, values [][]byte) error {
	size := 8 + 2 /* version[8] valnum[2] */
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

	p, err := packCommon(m, size, Ratomic)
	if err != nil {
		return err
	}

	m.Version = version
	m.Vals = values
	p = hop.Pint64(version, p)
	p = hop.Pint16(valnum, p)
	for i := uint16(0); i < valnum; i++ {
		p = hop.Pblob(values[i], p)
	}

	return nil
}
