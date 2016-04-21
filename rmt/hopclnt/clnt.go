// Copyright 2013 The Hop Authors.  All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// The clnt package provides definitions and functions used to implement
// a Hop client.
package hopclnt

import (
	"fmt"
	"hop"
	"hop/rmt"
	"log"
	"sync"
	"syscall"
)

// Debug flags
const (
	DbgPrintMsgs    = (1 << iota) // print all Hop messages on stderr
	DbgPrintPackets               // print the raw packets on stderr
	DbgLogMsgs                    // keep the last N Hop messages (can be accessed over http)
	DbgLogPackets                 // keep the last N Hop messages (can be accessed over http)
)

const (
	Msize = 1024 * 1024
)

type StatsOps interface {
	statsRegister()
	statsUnregister()
}

// The Clnt type represents a Hop client. The client is connected to
// a Hop server and its methods can be used to access and manipulate
// the keys exported by the server.
type Clnt struct {
	sync.Mutex
	Debuglevel int    // =0 don't print anything, >0 print Fcalls, >1 print raw packets
	Id         string // Used when printing debug messages
	Log        *hop.Logger

	conn     rmt.Conn
	tagpool  *pool
	reqfirst *Req
	reqlast  *Req
	err      error

	reqchan chan *Req
	tchan   chan *rmt.Msg

	next, prev *Clnt
}

type pool struct {
	sync.Mutex
	need  int
	nchan chan uint32
	maxid uint32
	imap  []byte
}

type Req struct {
	sync.Mutex
	Clnt       *Clnt
	Type       uint16
	Rc         *rmt.Msg
	Err        error
	Done       chan *Req
	tag        uint16
	donechan   chan *Req
	prev, next *Req
}

type ClntList struct {
	sync.Mutex
	clntList, clntLast *Clnt
}

var clnts *ClntList
var DefaultDebuglevel int
var DefaultLogger *hop.Logger

func (clnt *Clnt) Rpcnb(r *Req, tc *rmt.Msg) error {
	rmt.SetTag(tc, r.tag)
	r.Type = tc.Type
	clnt.Lock()
	if clnt.err != nil {
		clnt.Unlock()
		return clnt.err
	}

	if clnt.reqlast != nil {
		clnt.reqlast.next = r
	} else {
		clnt.reqfirst = r
	}

	r.prev = clnt.reqlast
	clnt.reqlast = r
	clnt.Unlock()

	if clnt.Debuglevel > 0 {
		clnt.logMsg(tc)
		if clnt.Debuglevel&DbgPrintPackets != 0 {
			log.Println("{-{", clnt.Id, fmt.Sprint(tc.Pkt))
		}

		if clnt.Debuglevel&DbgPrintMsgs != 0 {
			log.Println("{{{", clnt.Id, tc.String())
		}
	}
	clnt.conn.Send(tc)
	return nil
}

func (clnt *Clnt) Rpc(tc *rmt.Msg) (rc *rmt.Msg, err error) {
	r := clnt.ReqAlloc()
	err = clnt.Rpcnb(r, tc)
	if err != nil {
		return
	}

	<-r.Done
	rc = r.Rc
	err = r.Err
	r.Rc = nil // so the rc message is not released
	clnt.ReqFree(r)
	return
}

func (clnt *Clnt) Incoming(m *rmt.Msg) {
	if clnt.Debuglevel > 0 {
		clnt.logMsg(m)
		if clnt.Debuglevel&DbgPrintPackets != 0 {
			log.Println("}-}", clnt.Id, fmt.Sprint(m.Pkt))
		}

		if clnt.Debuglevel&DbgPrintMsgs != 0 {
			log.Println("}}}", clnt.Id, m.String())
		}
	}

	clnt.Lock()
	var r *Req = nil
	for r = clnt.reqfirst; r != nil; r = r.next {
		if r.tag == m.Tag {
			break
		}
	}

	if r == nil {
		clnt.err = &rmt.Error{"unexpected response", rmt.EINVAL}
		clnt.conn.Close()
		clnt.Unlock()
		return
	}

	r.Rc = m
	if r.prev != nil {
		r.prev.next = r.next
	} else {
		clnt.reqfirst = r.next
	}

	if r.next != nil {
		r.next.prev = r.prev
	} else {
		clnt.reqlast = r.prev
	}
	clnt.Unlock()

	if r.Type != r.Rc.Type-1 {
		if r.Rc.Type != rmt.Rerror {
			r.Err = &rmt.Error{"invalid response", rmt.EINVAL}
		} else {
			if r.Err == nil {
				r.Err = &rmt.Error{r.Rc.Edescr, syscall.Errno(r.Rc.Ecode)}
			}
		}
	}

	if r.Done != nil {
		r.Done <- r
	}
}

func (clnt *Clnt) ConnError(err error) {
	clnt.err = err

	/* send error to all pending requests */
	clnt.Lock()
	r := clnt.reqfirst
	clnt.reqfirst = nil
	clnt.reqlast = nil
	if err == nil {
		err = clnt.err
	}
	clnt.tagpool.close()
	clnt.Unlock()
	for ; r != nil; r = r.next {
		r.Err = err
		if r.Done != nil {
			r.Done <- r
		}
	}

	clnts.Lock()
	if clnt.prev != nil {
		clnt.prev.next = clnt.next
	} else {
		clnts.clntList = clnt.next
	}

	if clnt.next != nil {
		clnt.next.prev = clnt.prev
	} else {
		clnts.clntLast = clnt.prev
	}
	clnts.Unlock()

	if sop, ok := (interface{}(clnt)).(StatsOps); ok {
		sop.statsUnregister()
	}
}

// Creates and initializes a new Clnt object. Doesn't send any data
// on the wire.
func NewClient(c rmt.Conn) rmt.RemoteHop {
	clnt := new(Clnt)
	clnt.conn = c
	clnt.Debuglevel = DefaultDebuglevel
	clnt.Log = DefaultLogger
	clnt.Id = c.RemoteAddr() + ":"
	clnt.tagpool = newPool(uint32(rmt.NOTAG))
	clnt.reqchan = make(chan *Req, 16)
	clnt.tchan = make(chan *rmt.Msg, 16)

	clnts.Lock()
	if clnts.clntLast != nil {
		clnts.clntLast.next = clnt
	} else {
		clnts.clntList = clnt
	}

	clnt.prev = clnts.clntLast
	clnts.clntLast = clnt
	clnts.Unlock()

	if sop, ok := (interface{}(clnt)).(StatsOps); ok {
		sop.statsRegister()
	}

	c.SetResponseHandler(clnt)
	return clnt
}

func Connect(proto, addr string) (rmt.RemoteHop, error) {
	c, err := rmt.Connect(proto, addr)
	if err != nil {
		return nil, err
	}

	return NewClient(c), nil
}

func (clnt *Clnt) Close() {
	clnt.conn.Close()
}

func (clnt *Clnt) Closed() bool {
	return clnt.err != nil
}

func (clnt *Clnt) ReqAlloc() *Req {
	var req *Req
	select {
	case req = <-clnt.reqchan:
		break
	default:
		req = new(Req)
		req.Clnt = clnt
		req.tag = uint16(clnt.tagpool.getId())
		req.donechan = make(chan *Req)
	}

	req.Done = req.donechan
	return req
}

func (clnt *Clnt) ReqFree(req *Req) {
	if req.Rc != nil {
		clnt.conn.ReleaseInbound(req.Rc)
		req.Rc = nil
	}

	req.Done = nil
	req.Err = nil
	req.next = nil
	req.prev = nil

	select {
	case clnt.reqchan <- req:
		break
	default:
		clnt.tagpool.putId(uint32(req.tag))
	}
}

func (clnt *Clnt) logMsg(m *rmt.Msg) {
	if clnt.Debuglevel&DbgLogPackets != 0 {
		pkt := make([]byte, len(m.Pkt))
		copy(pkt, m.Pkt)
		clnt.Log.Log(pkt, clnt, DbgLogPackets)
	}

	if clnt.Debuglevel&DbgLogMsgs != 0 {
		mm := new(rmt.Msg)
		*mm = *m
		mm.Pkt = nil
		clnt.Log.Log(mm, clnt, DbgLogMsgs)
	}
}

func (clnt *Clnt) SetDebugLevel(n int) {
	clnt.Debuglevel = n
}

func (clnt *Clnt) SetLogger(l *hop.Logger) {
	clnt.Log = l
}

func (clnt *Clnt) Connection() rmt.Conn {
	return clnt.conn
}

func init() {
	clnts = new(ClntList)
	if sop, ok := (interface{}(clnts)).(StatsOps); ok {
		sop.statsRegister()
	}
}
