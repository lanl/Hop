// Copyright 2013 The Hop Authors.  All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package rmt

import (
	"errors"
	"fmt"
	"hop"
	"log"
	"net"
)

const (
	Msize = 1024 * 1024 // the default read buffer size
)

type Netconn struct {
	conn     net.Conn
	done     chan bool
	msgout   chan *Msg
	imsgchan chan *Msg
	omsgchan chan *Msg

	reqHandler MsgHandler
	rspHandler MsgHandler
}

func NewNetconn(conn net.Conn) *Netconn {
	c := new(Netconn)
	c.conn = conn
	c.msgout = make(chan *Msg)
	c.imsgchan = make(chan *Msg, 512)
	c.omsgchan = make(chan *Msg, 512)

	go c.recv()
	go c.send()

	return c
}

func (c *Netconn) SetRequestHandler(rr MsgHandler) {
	c.reqHandler = rr
}

func (c *Netconn) SetResponseHandler(rr MsgHandler) {
	c.rspHandler = rr
}

func (conn *Netconn) Send(m *Msg) error {
	if conn.conn == nil {
		return errors.New("connection closed")
	}

	conn.msgout <- m
	return nil
}

func (c *Netconn) GetOutbound() (m *Msg) {
	select {
	case m = <-c.omsgchan:
		// got message
	default:
		// allocate new message
		m = new(Msg)
		m.Buf = make([]byte, 8192)
	}

	return m
}

func (c *Netconn) ReleaseOutbound(m *Msg) {
	// make sure we don't keep stuff that should be garbage-collected
	m.Value = nil
	m.Oldval = nil
	m.Vals = nil
	m.Pkt = nil
	select {
	case c.omsgchan <- m:
	default:
	}
}

func (c *Netconn) GetInbound() (m *Msg) {
	select {
	case m = <-c.imsgchan:
		// got message
	default:
		// allocate new message
		m = new(Msg)
	}

	return m
}

func (c *Netconn) ReleaseInbound(m *Msg) {
	// make sure we don't keep stuff that should be garbage-collected
	m.Value = nil
	m.Oldval = nil
	m.Vals = nil
	m.Pkt = nil
	m.Buf = nil
	select {
	case c.omsgchan <- m:
	default:
	}
}

func (conn *Netconn) Close() {
	conn.conn.Close()
}

func (conn *Netconn) RemoteAddr() string {
	return conn.conn.RemoteAddr().String()
}

func (conn *Netconn) LocalAddr() string {
	return conn.conn.LocalAddr().String()
}

func (conn *Netconn) recv() {
	var err error
	var n int

	buf := make([]byte, Msize)
	pos := 0
	for {
		if len(buf) < int(64) {
			b := make([]byte, Msize)
			copy(b, buf[0:pos])
			buf = b
			b = nil
		}

		n, err = conn.conn.Read(buf[pos:])
		if err != nil || n == 0 {
			goto closed
		}

		pos += n
		for pos > 4 {
			sz, _ := hop.Gint32(buf)
			if pos < int(sz) {
				if len(buf) < int(sz) {
					nsz := sz
					if nsz < Msize {
						nsz = Msize
					}

					b := make([]byte, nsz)
					copy(b, buf[0:pos])
					buf = b
					b = nil
				}

				break
			}

			m := conn.GetInbound()
			err := Unpack(m, buf)
			if err != nil {
				log.Println(fmt.Sprintf("invalid packet : %v: %v %v", conn.RemoteAddr(), err, buf))
				conn.conn.Close()
				goto closed
			}

			//			log.Println("]]]", m.String())
			msize := m.Size
			if m.Type%2 == 0 {
				if conn.rspHandler != nil {
					conn.rspHandler.Incoming(m)
				} else {
					log.Println(fmt.Sprintf("invalid packet: %v: %v", conn.RemoteAddr(), m))
					conn.conn.Close()
					goto closed
				}
			} else {
				if conn.reqHandler != nil {
					conn.reqHandler.Incoming(m)
				} else {
					log.Println(fmt.Sprintf("invalid packet: %v: %v", conn.RemoteAddr(), m))
					conn.conn.Close()
					goto closed
				}
			}

			buf = buf[msize:]
			pos -= int(msize)
		}
	}

closed:
	conn.conn.Close()		// just in case...
	if err == nil {
		err = errors.New("connection closed")
	}

	// signal closed connection
	if conn.reqHandler != nil {
		conn.reqHandler.ConnError(err)
	}

	if conn.rspHandler != nil {
		conn.rspHandler.ConnError(err)
	}

	// signal send() goroutine
	conn.done <- true
}

func (conn *Netconn) send() {
	for {
		select {
		case <-conn.done:
			return

		case m := <-conn.msgout:
			for buf := m.Pkt; len(buf) > 0; {
				n, err := conn.conn.Write(buf)
				if err != nil {
					/* just close the socket, will get signal on conn.done */
					log.Println(fmt.Sprintf("error while writing: %v", err))
					conn.conn.Close()
					break
				}

				buf = buf[n:]
			}

			//			log.Println("[[[", m.String())
			//			conn.ReleaseOutbound(m)
		}
	}
}

func StartNetListener(ntype, addr string, listener Listener) (err error) {
	l, err := net.Listen(ntype, addr)
	if err != nil {
		return &Error{err.Error(), EIO}
	}

	go func() {
		for {
			if c, err := l.Accept(); err != nil {
				log.Println(err)
			} else {
				listener.NewConnection(NewNetconn(c))
			}
		}
	}()

	return nil
}

type netprototype int

var netproto netprototype

func (netprototype) Connect(proto, addr string) (Conn, error) {
	c, err := net.Dial(proto, addr)
	if err != nil {
		return nil, err
	}

	return NewNetconn(c), nil
}

func (netprototype) Listen(proto, addr string, lstn Listener) (string, error) {
	err := StartNetListener(proto, addr, lstn)
	return addr, err
}

func init() {
	if err := AddProtocol("tcp", netproto); err != nil {
		fmt.Printf("Error: %v\n", err)
	}
}
