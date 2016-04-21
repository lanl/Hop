// Copyright 2013 The Hop Authors.  All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package hop

func Gint8(buf []byte) (uint8, []byte) { return buf[0], buf[1:] }

func Gint16(buf []byte) (uint16, []byte) {
	return uint16(buf[0]) | (uint16(buf[1]) << 8), buf[2:]
}

func Gint32(buf []byte) (uint32, []byte) {
	return uint32(buf[0]) | (uint32(buf[1]) << 8) | (uint32(buf[2]) << 16) |
			(uint32(buf[3]) << 24),
		buf[4:]
}

func Gint64(buf []byte) (uint64, []byte) {
	return uint64(buf[0]) | (uint64(buf[1]) << 8) | (uint64(buf[2]) << 16) |
			(uint64(buf[3]) << 24) | (uint64(buf[4]) << 32) | (uint64(buf[5]) << 40) |
			(uint64(buf[6]) << 48) | (uint64(buf[7]) << 56),
		buf[8:]
}

func Gstr(buf []byte) (string, []byte) {
	var n uint16

	if buf == nil {
		return "", nil
	}

	n, buf = Gint16(buf)
	if int(n) > len(buf) {
		return "", nil
	}

	return string(buf[0:n]), buf[n:]
}

func Gblob(buf []byte) ([]byte, []byte) {
	var n uint32

	if buf == nil {
		return nil, nil
	}

	n, buf = Gint32(buf)
	if n == ^uint32(0) {
		return nil, buf
	}

	if int(n) > len(buf) {
		return nil, nil
	}

	return buf[0:n], buf[n:]
}

func Pint8(val uint8, buf []byte) []byte {
	buf[0] = val
	return buf[1:]
}

func Pint16(val uint16, buf []byte) []byte {
	buf[0] = uint8(val)
	buf[1] = uint8(val >> 8)
	return buf[2:]
}

func Pint32(val uint32, buf []byte) []byte {
	buf[0] = uint8(val)
	buf[1] = uint8(val >> 8)
	buf[2] = uint8(val >> 16)
	buf[3] = uint8(val >> 24)
	return buf[4:]
}

func Pint64(val uint64, buf []byte) []byte {
	buf[0] = uint8(val)
	buf[1] = uint8(val >> 8)
	buf[2] = uint8(val >> 16)
	buf[3] = uint8(val >> 24)
	buf[4] = uint8(val >> 32)
	buf[5] = uint8(val >> 40)
	buf[6] = uint8(val >> 48)
	buf[7] = uint8(val >> 56)
	return buf[8:]
}

func Pstr(val string, buf []byte) []byte {
	n := uint16(len(val))
	buf = Pint16(n, buf)
	b := []byte(val)
	copy(buf, b)
	return buf[n:]
}

func Pblob(val []byte, buf []byte) []byte {
	if val != nil {
		n := uint32(len(val))
		buf = Pint32(n, buf)
		copy(buf, val)
		buf = buf[n:]
	} else {
		buf = Pint32(^uint32(0), buf)
	}

	return buf
}

