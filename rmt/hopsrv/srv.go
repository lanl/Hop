// Copyright 2013 The Hop Authors.  All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// The srv package provides definitions and functions used to implement
// a remote Hop server.
package hopsrv

import (
	"fmt"
	"hop"
	"hop/rmt"
	"log"
	"sync"
)

// Debug flags
const (
	DbgPrintMsgs    = (1 << iota) // print all Hop messages on stderr
	DbgPrintPackets               // print the raw packets on stderr
	DbgLogMsgs                    // keep the last N Hop messages (can be accessed over http)
	DbgLogPackets                 // keep the last N Hop packets (can be accessed over http)
)

var Enotimpl error = &rmt.Error{"not implemented", rmt.EINVAL}

// Connection operations. These should be implemented if the file server
// needs to be called when a connection is opened or closed.
type ConnOps interface {
	ConnOpened(*Conn)
	ConnClosed(*Conn)
}

type StatsOps interface {
	statsRegister()
	statsUnregister()
}

// The Srv type contains the basic fields used to control the Hop
// remote server. Each file server implementation should create a value
// of Srv type, initialize the values it cares about and pass the
// struct to the (Srv *) srv.Start(ops) method together with the object
// that implements the file server operations.
type Srv struct {
	sync.Mutex
	Id          string    // Used for debugging and stats
	Debuglevel  int       // debug level
	Log         *hop.Logger

	Ops interface{} // operations

	connlist *Conn // List of connections
}

// The Conn type represents a connection from a client to the file server
type Conn struct {
	sync.Mutex
	Srv        *Srv
	Id         string // used for debugging and stats
	Debuglevel int

	conn rmt.Conn
	ops  hop.Hop         // operations

	done       chan bool
	prev, next *Conn

	// stats
	nreqs   int    // number of requests processed by the server
	tsz     uint64 // total size of the T messages received
	rsz     uint64 // total size of the R messages sent
	npend   int    // number of currently pending messages
	maxpend int    // maximum number of pending messages
	nreads  int    // number of reads
	nwrites int    // number of writes
}

// The Start method should be called once the file server implementor
// initializes the Srv struct with the preferred values. It sets default
// values to the fields that are not initialized and creates the goroutines
// required for the server's operation. The method receives an empty
// interface value, ops, that should implement the interfaces the file server is
// interested in. Ops must implement the ReqOps interface.
func (srv *Srv) Start(ops interface{}) bool {
	srv.Ops = ops
	if srv.Log == nil {
		srv.Log = hop.NewLogger(1024)
	}

	if sop, ok := (interface{}(srv)).(StatsOps); ok {
		sop.statsRegister()
	}

	return true
}

func (srv *Srv) String() string {
	return srv.Id
}

// Performs the default processing of a request. Calls the
// appropriate ReqOps operation for the message. The file server
// implementer should call it only if the Hop server implements
// the ReqProcessOps within the ReqProcess operation.
func (conn *Conn) Process(tc *rmt.Msg) {
	var ver uint64
	var val []byte
	var vals [][]byte
	var err error
	var rc *rmt.Msg

	ops := conn.ops
	c := conn.conn

	switch tc.Type {
	default:
		rc = c.GetOutbound()
		err = &rmt.Error{"unknown message type", rmt.ENOSYS}

	case rmt.Tcreate:
		ver, err = ops.Create(tc.Key, tc.Flags, tc.Value)
		rc = c.GetOutbound()
		if err == nil {
			err = rmt.PackRcreate(rc, ver)
		}

	case rmt.Tremove:
		err = ops.Remove(tc.Key)
		rc = c.GetOutbound()
		if err == nil {
			err = rmt.PackRremove(rc)
		}

	case rmt.Tget:
		ver, val, err = ops.Get(tc.Key, tc.Version)
		rc = c.GetOutbound()
		if err == nil {
			err = rmt.PackRget(rc, ver, val)
		}

	case rmt.Tset:
		ver, err = ops.Set(tc.Key, tc.Value)
		rc = c.GetOutbound()
		if err == nil {
			err = rmt.PackRset(rc, ver)
		}

	case rmt.Ttestset:
		ver, val, err = ops.TestSet(tc.Key, tc.Version, tc.Oldval, tc.Value)
		rc = c.GetOutbound()
		if err == nil {
			err = rmt.PackRtestset(rc, ver, val)
		}

	case rmt.Tatomic:
		ver, vals, err = ops.Atomic(tc.Key, tc.Atmop, tc.Vals)
		rc = c.GetOutbound()
		if err == nil {
			err = rmt.PackRatomic(rc, ver, vals)
		}
	}

	if err != nil {
		switch e := err.(type) {
		case *rmt.Error:
			rmt.PackRerror(rc, e.Error(), uint32(e.Ecode))
		case error:
			rmt.PackRerror(rc, e.Error(), uint32(rmt.EIO))
		default:
			rmt.PackRerror(rc, fmt.Sprintf("%v", e), uint32(rmt.EIO))
		}
	}

	rmt.SetTag(rc, tc.Tag)
	if conn.Debuglevel > 0 {
		conn.logMsg(rc)
		if conn.Debuglevel&DbgPrintPackets != 0 {
			log.Println(">->", conn.Id, fmt.Sprint(rc.Pkt))
		}

		if conn.Debuglevel&DbgPrintMsgs != 0 {
			log.Println("<<<", conn.Id, rc.String())
		}
	}

	conn.conn.Send(rc)
	conn.conn.ReleaseInbound(tc)
}

func (conn *Conn) String() string {
	return conn.Srv.Id + "/" + conn.Id
}
