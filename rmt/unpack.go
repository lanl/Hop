// Copyright 2013 The Hop Authors.  All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package rmt

import (
	"fmt"
	"hop"
)

var Eshort = &Error{"buffer too short", EINVAL}

func unpackHeader(buf []byte) (size uint32, mtype uint16, tag uint16, err error) {
	if len(buf) < 8 {
		err = Eshort
		return
	}

	size, buf = hop.Gint32(buf)
	mtype, buf = hop.Gint16(buf)
	tag, buf = hop.Gint16(buf)

	return
}

// Creates a Msg value from the on-the-wire representation.
// Returns the unpacked message, error and how many bytes from the
// buffer were used by the message.
func Unpack(m *Msg, buf []byte) error {
	if len(buf) < 8 {
		return &Error{fmt.Sprintf("buffer too short: %d", len(buf)), EINVAL}
	}

	p := buf
	m.Size, p = hop.Gint32(p)
	m.Type, p = hop.Gint16(p)
	m.Tag, p = hop.Gint16(p)

	if int(m.Size) > len(buf) || m.Size < 8 {
		l := len(buf)
		if l > 8 {
			l = 8
		}

		return &Error{fmt.Sprintf("buffer too short: %d expected %d %v", len(buf), m.Size, buf[0:l]), EINVAL}
	}

	p = p[0 : m.Size-8]
	m.Pkt = buf[0:m.Size]
	if m.Type < Rerror || m.Type >= Tlast {
		return &Error{"invalid id", EINVAL}
	}

	sz := minMsgsize[m.Type-Rerror]
	if m.Size < sz {
		goto szerror
	}

	switch m.Type {
	default:
		return &Error{"invalid message id", EINVAL}

	case Rerror:
		m.Ecode, p = hop.Gint32(p)
		m.Edescr, p = hop.Gstr(p)

	case Tget:
		m.Key, p = hop.Gstr(p)
		if p != nil {
			m.Version, p = hop.Gint64(p)
		}

	case Rget:
		m.Version, p = hop.Gint64(p)
		m.Value, p = hop.Gblob(p)

	case Tset:
		m.Key, p = hop.Gstr(p)
		if p != nil {
			m.Value, p = hop.Gblob(p)
		}

	case Rset:
		m.Version, p = hop.Gint64(p)

	case Tcreate:
		m.Key, p = hop.Gstr(p)
		if p == nil {
			goto szerror
		}

		m.Flags, p = hop.Gstr(p)
		if p == nil {
			goto szerror
		}

		m.Value, p = hop.Gblob(p)

	case Rcreate:
		m.Version, p = hop.Gint64(p)

	case Tremove:
		m.Key, p = hop.Gstr(p)

	case Rremove:
		/* nothing */
	case Ttestset:
		m.Key, p = hop.Gstr(p)
		if p == nil {
			goto szerror
		}

		m.Version, p = hop.Gint64(p)
		if p == nil {
			goto szerror
		}

		m.Oldval, p = hop.Gblob(p)
		if p == nil {
			goto szerror
		}

		m.Value, p = hop.Gblob(p)

	case Rtestset:
		m.Version, p = hop.Gint64(p)
		m.Value, p = hop.Gblob(p)

	case Tatomic:
		var n uint16

		m.Atmop, p = hop.Gint16(p)
		m.Key, p = hop.Gstr(p)
		if p == nil || len(p) < 2 {
			goto szerror
		}

		if n, p = hop.Gint16(p); n > 0 {
			m.Vals = make([][]byte, n)
			for i := uint16(0); i < n; i++ {
				m.Vals[i], p = hop.Gblob(p)
				if p == nil {
					goto szerror
				}
			}
		}

	case Ratomic:
		var n uint16

		m.Version, p = hop.Gint64(p)
		if n, p = hop.Gint16(p); n > 0 {
			m.Vals = make([][]byte, n)
			for i := uint16(0); i < n; i++ {
				m.Vals[i], p = hop.Gblob(p)
				if p == nil {
					goto szerror
				}
			}
		}
	}

	if p == nil || len(p) > 0 {
		goto szerror
	}

	return nil

szerror:
	return &Error{"invalid size", EINVAL}
}
