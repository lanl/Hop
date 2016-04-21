// Copyright 2013 The Hop Authors.  All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package rmt

import (
	"fmt"
	"hop"
	"syscall"
)

// Hop message types
const (
	Rerror = 100 + iota
	Tget
	Rget
	Tset
	Rset
	Tcreate
	Rcreate
	Tremove
	Rremove
	Ttestset
	Rtestset
	Tatomic
	Ratomic
	Tlast
)

const (
	NOTAG uint16 = 0xFFFF     // no tag specified
	NOFID uint32 = 0xFFFFFFFF // no fid specified
	NOUID uint32 = 0xFFFFFFFF // no uid specified
)

// Error values
const (
	ECONNRESET = syscall.ECONNRESET
	EINVAL     = syscall.EINVAL
	EIO        = syscall.EIO
	ENOENT     = syscall.ENOENT
	ENOSYS     = syscall.ENOSYS
	EPERM      = syscall.EPERM
)

type RemoteHop interface {
	hop.Hop

	SetDebugLevel(int)
	SetLogger(*hop.Logger)
	Connection() Conn
	Close()
	Closed() bool
}

// Hopmsg represents a Hop message
type Msg struct {
	Size uint32 // size of the message
	Type uint16 // message type
	Tag  uint16 // message tag

	Key     string   // key
	Version uint64   // version of the key
	Value   []byte   // value
	Oldval  []byte   // old value
	Vals    [][]byte // list of values for the atomic operations
	Atmop   uint16   // atomic set operation
	Flags   string   // create flags
	Edescr  string   // error description
	Ecode   uint32   // error code

	Pkt []uint8 // raw packet data
	Buf []uint8 // buffer to put the raw data in
}

// Error represents a Hop error
type Error struct {
	Edescr string        // textual representation of the error
	Ecode  syscall.Errno // numeric representation of the error
}

// minimum size of a Hop message for a type
// all of them start with size[4] type[2] tag[2]
var minMsgsize = [...]uint32{
	14, /* Rerror code[4] error[s] */
	18, /* Tget key[s] version[8] */
	20, /* Rget version[8] value[n] */
	14, /* Tset key[s] value[n] */
	16, /* Rset version[8] */
	16, /* Tcreate key[s] flags[s] value[n] */
	16, /* Rcreate version[8] */
	10, /* Tremove key[s] */
	8,  /* Rremove */
	26, /* Ttestset key[s] oldval[n] oldversion[8] value[n] */
	20, /* Rtestset version[8] value[n] */
	14, /* Tatomic op[2] key[s] valnum[2] value[n] value[n] ... */
	10, /* Ratomic version[8] valnum[2] value[n] value[n] ... */
	18, /* Tgetnewer key[s] version[8] */
	20, /* Rgetnewer version[8] value[n] */
}

// Allocates a new Fcall.
func NewMsg() *Msg {
	m := new(Msg)

	return m
}

// Sets the tag of a Fcall.
func SetTag(m *Msg, tag uint16) {
	m.Tag = tag
	hop.Pint16(tag, m.Pkt[6:])
}

func packCommon(m *Msg, size int, id uint16) ([]byte, error) {
	size += 4 + 2 + 2 /* size[4] type[2] tag[2] */
	if m.Buf == nil || len(m.Buf) < int(size) {
		bsz := size
		if bsz < 8*1024 {
			bsz = 8 * 1024
		}

		m.Buf = make([]byte, bsz)
	}

	m.Size = uint32(size)
	m.Type = id
	m.Tag = NOTAG
	p := m.Buf
	p = hop.Pint32(uint32(size), p)
	p = hop.Pint16(id, p)
	p = hop.Pint16(NOTAG, p)
	m.Pkt = m.Buf[0:size]

	return p, nil
}

func (err *Error) Error() string {
	if err != nil {
		return fmt.Sprintf("%s: %d", err.Edescr, err.Ecode)
	}

	return ""
}
