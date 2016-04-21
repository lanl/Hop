// Copyright 2013 The Hop Authors.  All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// +build linux ib

package rmt

/*
#cgo LDFLAGS: -libverbs -lrdmacm

#include <sys/types.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <netdb.h>
#include <stdlib.h>
#include <unistd.h>
#include <string.h>
#include <stdio.h>
#include <infiniband/verbs.h>
#include <rdma/rdma_cma.h>
#include <rdma/rdma_verbs.h>

//static void *toptr(uint64_t v) {
//	return (void *) v;
//}

//static struct sockaddr *tosockaddr(struct sockaddr_in *s) {
//	return (struct sockaddr *) s;
//}

static int post_send(struct rdma_cm_id *id, uintptr_t sidx, void *addr, size_t length, struct ibv_mr *mr, int flags) {
	return rdma_post_send(id, (void *) sidx, addr, length, mr, flags);
}

static int post_recv(struct rdma_cm_id *id, uintptr_t sidx, void *addr, size_t length, struct ibv_mr *mr) {
	return rdma_post_recv(id, (void *) sidx, addr, length, mr);
}

*/
import "C"

import (
	"errors"
	"fmt"
	"hop"
_	"log"
	"net"
	_ "runtime"
	"sync"
	"unsafe"
)

const (
	IBbufsize = 8192
)

const (
	Textra = Tlast + 1
)

type IBdev struct {
	sync.RWMutex
	name     string
	conns    map[uint32]*IBconn
	sidxchan chan int    // indices of available send buffers
	bufchan  chan []byte // pool of buffers
	rcvbuf   []byte      // buffer for receiving messages (IBmsize*IBdepth bytes of size)
	sndbuf   []byte      // buffer for sending messages (IBmsize*IBdepth bytes of size)

	imsgchan chan *Msg
	omsgchan chan *Msg
	xmsgs    map[uint64]*IBXmsg // pending extra-long messages

	srqid   *C.struct_rdma_cm_id
	ctx     *C.struct_ibv_context
	pd      *C.struct_ibv_pd
	rcvmr   *C.struct_ibv_mr
	sndmr   *C.struct_ibv_mr
	srq     *C.struct_ibv_srq
	ch      *C.struct_ibv_comp_channel
	cq      *C.struct_ibv_cq
	cqevnum uint
}

type IBconn struct {
	sync.Mutex
	err        error
	dev        *IBdev
	connected  bool
	addr       string
	done       chan bool
	msgout     chan *Msg
	reqHandler MsgHandler
	rspHandler MsgHandler
	recvchan   chan *Msg
	errchan    chan error

	// RDMA fields
	cmid  *C.struct_rdma_cm_id
	qp    *C.struct_ibv_qp
	qpnum uint32
}

type IBXmsg struct {
	m   *Msg
	num uint32
}

var IBmsize = 128 * 1024
var IBdepth = 12 * 1024
var iblock sync.RWMutex
var ibdevmap map[string]*IBdev
var eventChannel *C.struct_rdma_event_channel

func getAddrInfo(addr string) (*C.struct_addrinfo, error) {
	var hints C.struct_addrinfo
	var res *C.struct_addrinfo

	host, port, err := net.SplitHostPort(addr)
	if err != nil {
		return nil, err
	}

	hints.ai_socktype = C.SOCK_STREAM
	hints.ai_protocol = C.IPPROTO_TCP
	hints.ai_family = C.AF_INET

	h := C.CString(host)
	defer C.free(unsafe.Pointer(h))
	p := C.CString(port)
	defer C.free(unsafe.Pointer(p))
	gerrno, err := C.getaddrinfo(h, p, &hints, &res)
	if gerrno != 0 {
		var str string

		if gerrno == C.EAI_NONAME {
			str = "no such host"
		} else if gerrno == C.EAI_SYSTEM {
			str = err.Error()
		} else {
			str = C.GoString(C.gai_strerror(gerrno))
		}

		return nil, errors.New(str)
	}

	return res, nil
}

func NewIBconn(addr string) (conn *IBconn, err error) {
	var cmid *C.struct_rdma_cm_id

	if n, e := C.rdma_create_id(nil, &cmid, nil, C.RDMA_PS_TCP); n < 0 {
		return nil, e
	}

	ai, e := getAddrInfo(addr)
	if e != nil {
		err = e
		goto error
	}
	defer C.freeaddrinfo(ai)

	if n, e := C.rdma_resolve_addr(cmid, nil, ai.ai_addr, C.int(30000)); n < 0 {
		err = e
		goto error
	}

	if n, e := C.rdma_resolve_route(cmid, C.int(30000)); n < 0 {
		err = e
		goto error
	}

	conn, err = newIBconn(cmid)
	if err != nil {
		return nil, err
	}

	conn.addr = addr
	err = conn.connect()
	if err != nil {
		conn.Close()
		return nil, err
	}

	if n, e := C.rdma_migrate_id(cmid, eventChannel); n < 0 {
		return nil, e
	}

	return

error:
	if cmid != nil {
		C.rdma_destroy_id(cmid)
	}

	return
}

func newIBconn(cmid *C.struct_rdma_cm_id) (conn *IBconn, err error) {
	var qpattr C.struct_ibv_qp_init_attr

	conn = new(IBconn)
	conn.done = make(chan bool)
	conn.msgout = make(chan *Msg)
	conn.recvchan = make(chan *Msg)
	conn.errchan = make(chan error)

	conn.cmid = cmid
	//	conn.context = conn.cmid.verbs
	cmid.context = unsafe.Pointer(conn)

	dev, err := getIBdev(cmid)
	if err != nil {
		return nil, err
	}

	conn.dev = dev
	qpattr.qp_context = unsafe.Pointer(conn)
	qpattr.send_cq = dev.cq
	qpattr.recv_cq = dev.cq
	qpattr.srq = dev.srq
	qpattr.cap.max_send_wr = C.uint32_t(IBdepth)
	qpattr.cap.max_recv_wr = C.uint32_t(IBdepth)
	qpattr.cap.max_send_sge = 1
	qpattr.cap.max_recv_sge = 1
	qpattr.cap.max_inline_data = 0
	qpattr.sq_sig_all = 0
	qpattr.qp_type = C.IBV_QPT_RC

	if n, e := C.rdma_create_qp(conn.cmid, dev.pd, &qpattr); n < 0 {
		err = errors.New(fmt.Sprintf("can't create QP: %v", e))
		goto error
	}
	conn.qp = conn.cmid.qp
	conn.qpnum = uint32(conn.qp.qp_num)
	conn.cmid.srq = dev.srq

	go conn.recvproc()
	go conn.sendproc()

	dev.addConn(conn)
	return conn, nil

error:
	conn.Close()
	return nil, err
}

func (conn *IBconn) connect() error {
	var cparam C.struct_rdma_conn_param

	cparam.retry_count = 5
	cparam.rnr_retry_count = 5
	if n, e := C.rdma_connect(conn.cmid, &cparam); n < 0 {
		return e
	}

	conn.connected = true
	return nil
}

func (conn *IBconn) accept() error {
	var cparam C.struct_rdma_conn_param

	cparam.responder_resources = 1
	cparam.initiator_depth = 1
	cparam.retry_count = 5
	cparam.rnr_retry_count = 5
	if n, e := C.rdma_accept(conn.cmid, &cparam); n < 0 {
		return e
	}

	conn.connected = true
	return nil
}

func (conn *IBconn) Close() {
	conn.Lock()
	if conn.dev != nil {
		conn.dev.removeConn(conn)
		conn.dev = nil
	}

	if conn.connected {
		conn.connected = false
		C.rdma_disconnect(conn.cmid)
	}

	if conn.qp != nil {
		var attr C.struct_ibv_qp_attr

		qp := conn.qp
		conn.qp = nil
		attr.qp_state = C.IBV_QPS_ERR
		C.ibv_modify_qp(qp, &attr, C.IBV_QP_STATE)
		C.ibv_destroy_qp(qp)
	}

	if conn.cmid != nil {
		cmid := conn.cmid
		conn.cmid = nil
		C.rdma_destroy_id(cmid)
	}
	conn.Unlock()
}

func (c *IBconn) SetRequestHandler(rr MsgHandler) {
	c.reqHandler = rr
}

func (c *IBconn) SetResponseHandler(rr MsgHandler) {
	c.rspHandler = rr
}

func (conn *IBconn) Send(m *Msg) error {
	if !conn.connected {
		return errors.New("connection closed")
	} else if conn.err != nil {
		return conn.err
	}

	conn.msgout <- m
	return nil
}

func (c *IBconn) GetOutbound() (m *Msg) {
	var ch chan *Msg

	if c.dev != nil {
		ch = c.dev.omsgchan
	}

	select {
	case m = <-ch:
		// got message
	default:
		// allocate new message
		m = new(Msg)
		m.Buf = make([]byte, 8192)
	}

	return m
}

func (c *IBconn) ReleaseOutbound(m *Msg) {
	if c.dev == nil {
		return
	}

	// make sure we don't keep stuff that should be garbage-collected
	m.Value = nil
	m.Oldval = nil
	m.Vals = nil
	m.Pkt = nil
	select {
	case c.dev.omsgchan <- m:
	default:
	}
}

func (c *IBconn) ReleaseInbound(m *Msg) {
	if c.dev == nil {
		return
	}

	// make sure we don't keep stuff that should be garbage-collected
	m.Value = nil
	m.Oldval = nil
	m.Vals = nil
	m.Pkt = nil
	select {
	case c.dev.imsgchan <- m:
	default:
	}
}

func (conn *IBconn) RemoteAddr() string {
	return conn.addr
}

func (conn *IBconn) LocalAddr() string {
	return ""
}

func (conn *IBconn) recvproc() {
	for conn.err == nil {
		select {
		case m := <-conn.recvchan:
			if e := Unpack(m, m.Pkt); e != nil {
				fmt.Printf("Error while unpacking: %v\n", e)
				conn.err = e
				break
			}

			if m.Type%2 == 0 {
				if conn.rspHandler != nil {
					conn.rspHandler.Incoming(m)
				} else {
					conn.err = errors.New(fmt.Sprintf("invalid packet: %v", m))
					break
				}
			} else {
				if conn.reqHandler != nil {
					conn.reqHandler.Incoming(m)
				} else {
					conn.err = errors.New(fmt.Sprintf("invalid packet: %v", m))
					break
				}
			}
		case e := <-conn.errchan:
			conn.err = e
		}
	}

	if conn.err != nil {
		// signal closed connection
		if conn.reqHandler != nil {
			conn.reqHandler.ConnError(conn.err)
		}

		if conn.rspHandler != nil {
			conn.rspHandler.ConnError(conn.err)
		}

		conn.Close()

		// signal send() goroutine
		conn.done <- true
	}
}

func (conn *IBconn) sendproc() {
	dev := conn.dev

	for {
		select {
		case <-conn.done:
			return

		case m := <-conn.msgout:
			//			fmt.Printf("IBconn.sendproc: got message %v\n", m)
			plen := IBmsize - 16
//			if len(m.Pkt) > plen {
//				fmt.Printf("IBconn.sendproc: long message tag %d id %d size %d\n", m.Tag, m.Type, m.Size)
//			}

			for buf, offset := m.Pkt, 0; len(buf) > 0; {
				sidx := <-dev.sidxchan
				sbuf := dev.sndbuf[sidx*IBmsize : (sidx+1)*IBmsize]
				if plen > len(buf) {
					plen = len(buf)
				}

				p := sbuf
				sz := 0
				if offset > 0 {
					p = hop.Pint32(m.Size, p)
					p = hop.Pint16(Textra, p)
					p = hop.Pint16(m.Tag, p)
					p = hop.Pint32(uint32(offset), p)
					sz += 12
//					fmt.Printf("IBconn.sendproc: long message offset %d len %d\n", offset, plen)
				}

				copy(p, buf[0:plen])
				sz += plen
				if n, e := C.post_send(conn.cmid, C.uintptr_t(sidx),
					unsafe.Pointer(&dev.sndbuf[sidx*IBmsize]),
					C.size_t(sz), dev.sndmr, C.IBV_SEND_SIGNALED); n < 0 {
					conn.err = e
					conn.Close()
					fmt.Printf("error while writing: %v", e)
				}
				offset += plen
				buf = buf[plen:]
			}

			conn.ReleaseOutbound(m)
		}
	}
}

func StartIBListener(addr string, listener Listener) (err error) {
	var listenid *C.struct_rdma_cm_id
	var evch *C.struct_rdma_event_channel

	if evch, err = C.rdma_create_event_channel(); evch == nil {
		return
	}

	if n, e := C.rdma_create_id(evch, &listenid, nil, C.RDMA_PS_TCP); n < 0 {
		err = e
		return
	}

	ai, e := getAddrInfo(addr)
	if e != nil {
		err = e
		return
	}
	defer C.freeaddrinfo(ai)

	if n, e := C.rdma_bind_addr(listenid, ai.ai_addr); n < 0 {
		err = e
		return
	}

	if n, e := C.rdma_listen(listenid, 1); n < 0 {
		err = e
		return
	}

	go doListen(listenid, listener, evch)
	return
}

func doListen(listenid *C.struct_rdma_cm_id, listener Listener, evch *C.struct_rdma_event_channel) {
	var err error
	var ev *C.struct_rdma_cm_event

	// TODO: how to shutdown?
	for {
		if n, e := C.rdma_get_cm_event(evch, &ev); n < 0 {
			err = e
			goto done
		}

		event := ev.event
		cmid := ev.id
		C.rdma_ack_cm_event(ev)
		switch event {
		case C.RDMA_CM_EVENT_CONNECT_REQUEST:
//			log.Println("Connection request")
			conn, err := newIBconn(cmid)
			if err != nil {
				fmt.Printf("Error: %v\n", err)
			} else if err := conn.accept(); err != nil {
				conn.err = err
				fmt.Printf("Error: %v\n", err)
				conn.Close()
			} else {
				listener.NewConnection(conn)
			}

		case C.RDMA_CM_EVENT_ESTABLISHED:
//			log.Println("Connection established")

		case C.RDMA_CM_EVENT_DISCONNECTED:
//			log.Println("Connection disconnected")
			conn := (*IBconn)(unsafe.Pointer(cmid.context))
			conn.Close()
		default:
//			log.Println(fmt.Sprintf("Event %d received", ev.event))
		}
	}
done:
	if err != nil {
		fmt.Printf("Error: %v\n", err)
	}
}

func getIBdev(cmid *C.struct_rdma_cm_id) (d *IBdev, err error) {
	devname := C.GoString(&cmid.verbs.device.name[0])

	iblock.RLock()
	d = ibdevmap[devname]
	iblock.RUnlock()

	if d != nil {
		return
	}

	// device not found, create it
	iblock.Lock()
	defer iblock.Unlock()
	d = ibdevmap[devname]
	if d != nil {
		// device might have been created while waiting for the write lock
		return
	}

	d, err = newIBdev(devname, C.rdma_get_local_addr(cmid))
	if err == nil {
		ibdevmap[devname] = d
	}

	return
}

func newIBdev(devname string, saddr *C.struct_sockaddr) (d *IBdev, err error) {
	var srqa C.struct_ibv_srq_init_attr

	d = new(IBdev)
	d.name = devname
	d.conns = make(map[uint32]*IBconn)
	d.sidxchan = make(chan int, IBdepth)
	d.bufchan = make(chan []byte, IBdepth)
	d.rcvbuf = make([]byte, IBmsize*IBdepth)
	d.sndbuf = make([]byte, IBmsize*IBdepth)
	d.imsgchan = make(chan *Msg, 1024)
	d.omsgchan = make(chan *Msg, 1024)
	d.xmsgs = make(map[uint64]*IBXmsg)

	if n, e := C.rdma_create_id(nil, &d.srqid, nil, C.RDMA_PS_TCP); n < 0 {
		return nil, e
	}

	if n, e := C.rdma_resolve_addr(d.srqid, nil, saddr, C.int(30000)); n < 0 {
		return nil, e
	}

	d.ctx = d.srqid.verbs
	if d.pd = C.ibv_alloc_pd(d.ctx); d.pd == nil {
		err = errors.New("can't allocate protection domain")
		goto error
	}

	d.rcvmr = C.ibv_reg_mr(d.pd, unsafe.Pointer(&d.rcvbuf[0]), C.size_t(len(d.rcvbuf)), C.IBV_ACCESS_LOCAL_WRITE)
	if d.rcvmr == nil {
		err = errors.New("can't register receive memory region")
		goto error
	}

	d.sndmr = C.ibv_reg_mr(d.pd, unsafe.Pointer(&d.sndbuf[0]), C.size_t(len(d.sndbuf)), C.IBV_ACCESS_LOCAL_WRITE)
	if d.sndmr == nil {
		err = errors.New("can't register send memory region")
		goto error
	}

	srqa.attr.max_wr = 2 * C.uint32_t(IBdepth)
	srqa.attr.max_sge = 1
	if n, e := C.rdma_create_srq(d.srqid, d.pd, &srqa); n < 0 {
		err = e
		goto error
	}

	d.srq = d.srqid.srq
	for i := 0; i < IBdepth; i++ {
		p := unsafe.Pointer(&d.rcvbuf[i*IBmsize])
		if n, e := C.post_recv(d.srqid, C.uintptr_t(i), p, C.size_t(IBmsize), d.rcvmr); n < 0 {
			err = errors.New(fmt.Sprintf("can't post recv buffer: %v", e))
			goto error
		}
	}

	for i := 0; i < IBdepth; i++ {
		d.sidxchan <- i
	}

	d.ch = C.ibv_create_comp_channel(d.ctx)
	if d.ch == nil {
		err = errors.New("can't create completion event channel")
		goto error
	}

	d.cq = C.ibv_create_cq(d.ctx, C.int(2*IBdepth), unsafe.Pointer(d), d.ch, 0)
	if d.cq == nil {
		err = errors.New("can't create completion queue")
		goto error
	}

	if n, e := C.ibv_req_notify_cq(d.cq, 0); n < 0 {
		err = errors.New(fmt.Sprintf("can't register notify CQ: %v", e))
		goto error
	}

	go d.pollproc()

	return d, nil

error:
	if d.cq != nil {
		C.ibv_destroy_cq(d.cq)
	}

	if d.ch != nil {
		C.ibv_destroy_comp_channel(d.ch)
	}

	if d.sndmr != nil {
		C.ibv_dereg_mr(d.sndmr)
	}

	if d.rcvmr != nil {
		C.ibv_dereg_mr(d.rcvmr)
	}

	if d.srq != nil {
		C.rdma_destroy_srq(d.srqid)
	}

	if d.pd != nil {
		C.ibv_dealloc_pd(d.pd)
	}

	if d.srqid != nil {
		C.rdma_destroy_id(d.srqid)
	}

	return nil, err
}

func (d *IBdev) addConn(c *IBconn) {
	d.Lock()
	d.conns[c.qpnum] = c
	d.Unlock()
	//	fmt.Printf("IBdev.addConn %d\n", c.qpnum)
}

func (d *IBdev) removeConn(c *IBconn) {
	d.Lock()
	delete(d.conns, c.qpnum)
	d.Unlock()
	//	fmt.Printf("IBdev.removeConn %d\n", c.qpnum)
}

func (d *IBdev) GetInbound(sz uint32) (m *Msg) {
	select {
	case m = <-d.imsgchan:
	default:
		m = new(Msg)
		m.Buf = make([]byte, IBbufsize)
	}

	if sz < uint32(len(m.Buf)) {
		m.Pkt = m.Buf[0:sz]
	} else {
		m.Pkt = make([]byte, sz)
	}

	return m
}

func (d *IBdev) connError(qpnum uint32, err error) {
	d.RLock()
	c := d.conns[qpnum]
	d.RUnlock()

	if c == nil {
		fmt.Printf("Error: unknown connection with qp_num %d\n", qpnum)
		return
	}

	c.errchan <- err
}

func (d *IBdev) handleExtra(qpnum, sz uint32, id, tag uint16, buf []byte) (*Msg, error) {
	qptag := uint64(qpnum)<<32 | uint64(tag)
	xm := d.xmsgs[qptag]
	if xm == nil {
		xm = new(IBXmsg)
		xm.m = d.GetInbound(sz)

		// calculate in how many buffers the message is going to arrive
		// all messages, except eventually the last
		// will have the same payload length, equal to the one of the
		// first payload (which is len(buf) - 8)
		plen := uint32(IBmsize - 16)
		xm.num = sz / plen
		if sz%plen != 0 {
			xm.num++
		}

		d.xmsgs[qptag] = xm
//		fmt.Printf("IBdev.handleExtra first fragment tag %d id %d\n", tag, id)
	}

	offset := uint32(0)
	b := buf
	if id == Textra {
		if len(buf) < 12 {
			return nil, Eshort
		}

		offset, b = hop.Gint32(buf[8:])
//		fmt.Printf("IBdev.handleExtra: tag %d %d fragment len %d\n", tag, offset, len(b))
	}

	copy(xm.m.Pkt[offset:], b)
	xm.num--
	if xm.num == 0 {
		delete(d.xmsgs, qptag)
//		fmt.Printf("IBdev.handleExtra: done with tag %d\n", tag)
		return xm.m, nil
	}

	return nil, nil
}

func (d *IBdev) pollproc() {
	var cq *C.struct_ibv_cq
	var wca [512]C.struct_ibv_wc
	var context unsafe.Pointer

	for {
		if n, err := C.ibv_get_cq_event(d.ch, &cq, &context); n < 0 {
			fmt.Printf("Error ibv_get_cq_event: %v\n", err)
			//			conn.err = err
			break
		}

		d.cqevnum++ // safe, nobody but this proc changes it
		if d.cqevnum > 10000 {
			C.ibv_ack_cq_events(cq, C.uint(d.cqevnum))
			d.cqevnum = 0
		}

		if n, err := C.ibv_req_notify_cq(cq, 0); n < 0 {
			fmt.Printf("Error ibv_req_notify_cq: %v\n", err)
			//			conn.err = err;
			break
		}

		for {
			//			fmt.Printf("cq %p wc %p conn.err %v\n", cq, &wc, conn.err)
			n, err := C.ibv_poll_cq(cq, C.int(len(wca)), &wca[0])
			if n <= 0 {
				if n < 0 {
					fmt.Printf("ibv_poll_cq error %v\n", err)
					//					conn.err = err
				}

				break
			}

			for i := 0; i < int(n); i++ {
				wc := &wca[i]

				qpnum := uint32(wc.qp_num)
				if wc.status != C.IBV_WC_SUCCESS {
					d.connError(qpnum, errors.New(fmt.Sprintf("requiest failed: %d", wc.status)))
					continue
				}

				switch wc.opcode {
				case C.IBV_WC_RECV:
					var m *Msg

					ridx := int(wc.wr_id)
					rcvbuf := d.rcvbuf[ridx*IBmsize : ridx*IBmsize+int(wc.byte_len)]
					sz, id, tag, err := unpackHeader(rcvbuf)
//					if sz == 0 {
//						fmt.Printf("IBdev.pollproc: size 0 %v\n", rcvbuf[0:8])
//					}

					if err != nil {
						d.connError(qpnum, err)
						continue
					}

					if id == Textra || sz > uint32(len(rcvbuf)) {
						m, err = d.handleExtra(qpnum, sz, id, tag, rcvbuf)
						if err != nil {
							d.connError(qpnum, err)
						}
					} else {
						m = d.GetInbound(sz)
						copy(m.Pkt, rcvbuf)
					}

					// post the buffer back to the pool
					if n, e := C.rdma_post_recv(d.srqid, unsafe.Pointer(uintptr(ridx)),
						unsafe.Pointer(&d.rcvbuf[ridx*IBmsize]), C.size_t(IBmsize), d.rcvmr); n < 0 {
						fmt.Printf("Error while posting buffer: %v\n", e)
						break
					}

					if m != nil {
						// get the connection the message belongs to and pass it to it
						d.RLock()
						c := d.conns[qpnum]
						d.RUnlock()

						if c == nil {
							fmt.Printf("Error: unknown connection with qp_num %d\n", wc.qp_num)
						} else {
							c.recvchan <- m
						}
					}

				case C.IBV_WC_SEND:
					// put the send buffer back to the pool
					//					fmt.Printf("send buf %d\n", int(wc.wr_id))
					d.sidxchan <- int(wc.wr_id)
				}
			}
		}
	}
}

type ibprototype int

var ibproto ibprototype

func (ibprototype) Connect(proto, addr string) (Conn, error) {
	return NewIBconn(addr)
}

func (ibprototype) Listen(proto, addr string, lstn Listener) (string, error) {
	err := StartIBListener(addr, lstn)
	return addr, err
}

func eventproc() {
	var ev *C.struct_rdma_cm_event

	for {
		if n, e := C.rdma_get_cm_event(eventChannel, &ev); n < 0 {
			fmt.Printf("eventproc: can't get event: %v\n", e)
			return
		}

		event := ev.event
		cmid := ev.id
		C.rdma_ack_cm_event(ev)
		switch event {
		case C.RDMA_CM_EVENT_CONNECT_REQUEST:
			//			fmt.Printf("Connection request: %p\n", cmid)

		case C.RDMA_CM_EVENT_ESTABLISHED:
			//			fmt.Printf("Connection established: %p\n", cmid)

		case C.RDMA_CM_EVENT_DISCONNECTED:
			//			fmt.Printf("Connection disconnected: %p\n", cmid)
			conn := (*IBconn)(unsafe.Pointer(cmid.context))
			if conn != nil {
				select {
				case conn.errchan <- errors.New("connection disconnected"):
				}
			}

		case C.RDMA_CM_EVENT_REJECTED:
			//			fmt.Printf("Connection rejected: %p\n", cmid)
			conn := (*IBconn)(unsafe.Pointer(cmid.context))
			if conn != nil {
				select {
				case conn.errchan <- errors.New("connection rejected"):
				}
			}

		default:
			//			fmt.Printf("Event %d received\n", ev.event)
		}
	}
}

func init() {
	var err error

	ibdevmap = make(map[string]*IBdev)
	if err := AddProtocol("ib", ibproto); err != nil {
		fmt.Printf("Error: %v\n", err)
	}

	eventChannel, err = C.rdma_create_event_channel()
	if eventChannel == nil {
		fmt.Printf("Error: can't create event channel: %v\n", err)
	} else {
		go eventproc()
	}
}
