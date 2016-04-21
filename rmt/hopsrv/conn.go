// Copyright 2013 The Hop Authors.  All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package hopsrv

import (
	"fmt"
	"hop"
	"hop/rmt"
	"log"
)

const (
	Msize = 8 * 1024 * 1024 // the default read buffer size
)

func (srv *Srv) NewConn(c rmt.Conn) {
	conn := new(Conn)
	conn.Srv = srv
	conn.Debuglevel = srv.Debuglevel
	conn.conn = c
	conn.ops, _ = srv.Ops.(hop.Hop)
	conn.done = make(chan bool)
	conn.prev = nil

	srv.Lock()
	conn.next = srv.connlist
	srv.connlist = conn
	srv.Unlock()

	conn.Id = c.RemoteAddr()
	c.SetRequestHandler(conn)
	if op, ok := (conn.Srv.Ops).(ConnOps); ok {
		op.ConnOpened(conn)
	}

	if sop, ok := (interface{}(conn)).(StatsOps); ok {
		sop.statsRegister()
	}
}

func (conn *Conn) ConnError(err error) {
	conn.Srv.Lock()
	if conn.prev != nil {
		conn.prev.next = conn.next
	} else {
		conn.Srv.connlist = conn.next
	}

	if conn.next != nil {
		conn.next.prev = conn.prev
	}
	conn.Srv.Unlock()

	if sop, ok := (interface{}(conn)).(StatsOps); ok {
		sop.statsUnregister()
	}

	if op, ok := (conn.Srv.Ops).(ConnOps); ok {
		op.ConnClosed(conn)
	}
}

func (conn *Conn) Incoming(m *rmt.Msg) {
	if conn.Debuglevel > 0 {
		conn.logMsg(m)
		if conn.Debuglevel&DbgPrintPackets != 0 {
			log.Println(">->", conn.Id, fmt.Sprint(m.Pkt))
		}

		if conn.Debuglevel&DbgPrintMsgs != 0 {
			log.Println(">>>", conn.Id, m.String())
		}
	}

	conn.Lock()
	conn.nreqs++
	conn.tsz += uint64(m.Size)
	conn.npend++
	if conn.npend > conn.maxpend {
		conn.maxpend = conn.npend
	}
	conn.Unlock()

	go conn.Process(m)
}

func (conn *Conn) RemoteAddr() string {
	return conn.conn.RemoteAddr()
}

func (conn *Conn) LocalAddr() string {
	return conn.conn.LocalAddr()
}

func (conn *Conn) SetOps(ops hop.Hop) {
	conn.ops = ops
}

func (conn *Conn) SetDebugLevel(n int) {
	conn.Debuglevel = n
}

func (conn *Conn) SetLogger(*hop.Logger) {
}

func (conn *Conn) Connection() rmt.Conn {
	return conn.conn
}

func (conn *Conn) Close() {
	conn.conn.Close()
}

func (conn *Conn) Closed() bool {
	return conn.conn == nil
}

func (conn *Conn) logMsg(m *rmt.Msg) {
	if conn.Debuglevel&DbgLogPackets != 0 {
		pkt := make([]byte, len(m.Pkt))
		copy(pkt, m.Pkt)
		conn.Srv.Log.Log(pkt, conn, DbgLogPackets)
	}

	if conn.Debuglevel&DbgLogMsgs != 0 {
		mm := new(rmt.Msg)
		*mm = *m
		mm.Pkt = nil
		conn.Srv.Log.Log(mm, conn, DbgLogMsgs)
	}
}

func (srv *Srv) NewConnection(c rmt.Conn) {
	srv.NewConn(c)
}
