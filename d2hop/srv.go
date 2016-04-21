// Copyright 2013 The Hop Authors.  All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package d2hop

import (
	"errors"
	"fmt"
	"hop"
	"hop/rmt"
	"hop/rmt/hopclnt"
	"hop/rmt/hopsrv"
	"hop/shop"
	"math"
	"strings"
	"sync"
	"time"
)

// D2Hop can be used as both server and a client. If D2Hop is created with addr
// being empty string, D2Hop will act only as a client. If addr is passed, it
// will listen on that address and act as both a server and a client.

type D2Hop struct {
	sync.RWMutex

	proto  string
	addr   string
	hop    hop.Hop
	srv    *hopsrv.Srv // if nil, client, i.e. not serving any keys
	closed bool

	master   rmt.RemoteHop
	keyhash  *KeyHash
	conf     *Conf
	srvmap   map[string]*Conn
	routes   RangeList
	cmap     map[rmt.Conn]string // used to establish duplex connections
	selfconn *Conn

	// local entries
	lents      *shop.SHop
	confentry  ConfEntry
	keysentry  KeysEntry
	khashentry *hop.Entry
	stackentry	StackEntry
}

// Represents client connection, both from a client, or another
// server. If from client, the clnt field is nil.
type Conn struct {
	srv   *D2Hop
	conn  *hopsrv.Conn
	clnt  hop.Hop
	alive time.Time // last time we heard from the server
}

var DefaultKeyHash = "fnv1a"

func NewD2Hop(proto, listenaddr, masteraddr string, hop hop.Hop, hashes []Hash) (s *D2Hop, err error) {
	s = new(D2Hop)
	s.proto = proto
	s.addr = listenaddr
	s.hop = hop
	s.srvmap = make(map[string]*Conn)
	s.cmap = make(map[rmt.Conn]string)

	if s.isServer() {
		s.startServer()

		// add ourselves to the list of servers
		s.selfconn = new(Conn)
		s.selfconn.srv = s
		s.selfconn.clnt = s.hop
		s.srvmap[s.addr] = s.selfconn
	}

	if masteraddr != "" && masteraddr != listenaddr {
		err = s.initCommon(masteraddr)
	} else {
		err = s.initMaster()
	}

	if err != nil {
		return nil, err
	}

	go s.heartbeatproc()
	return s, nil
}

func Connect(proto, addr string) (*D2Hop, error) {
	return NewD2Hop(proto, "", addr, nil)
}

func (s *D2Hop) startServer() error {
	s.srv = new(hopsrv.Srv)

	_, hopid, err := s.hop.Get("#/id", hop.Any)
	if err != nil {
		hopid = nil
	}

	// add some local entries
	s.lents = shop.NewSHop()
	id := "D2Hop"
	if hopid != nil {
		id += " (" + string(hopid) + ")"
	}

	s.lents.RemoveEntry("#/id")
	s.lents.AddEntry("#/id", []byte(id), nil)
	s.lents.AddEntry("#/ctl", []byte("D2Hop"), nil)
	s.khashentry, _ = s.lents.AddEntry("#/keyhash", []byte(DefaultKeyHash), nil)

	s.confentry.s = s
	s.lents.AddEntry("#/conf", nil, &s.confentry)

	s.keysentry.s = s
	s.lents.AddEntry("#/keys", nil, &s.keysentry)

	s.stackentry.s = s
	s.lents.AddEntry("#/stack", nil, &s.stackentry)

	if !s.srv.Start(s) {
		return errors.New("Error: can't start the server")
	}

	if _, err := rmt.Listen(s.proto, s.addr, s.srv); err != nil {
		return err
	}

	return nil
}

// initializes the structures shared by the client and non-master server
func (s *D2Hop) initCommon(masteraddr string) (err error) {
	var khash, confval []byte
	var confvals [][]byte
	var confver uint64
	var conf *Conf

	s.keysentry.s = s
	if s.master, err = hopclnt.Connect(s.proto, masteraddr); err != nil {
		return
	}

	confver, confval, err = s.master.Get("#/conf", hop.Any)
	if err != nil {
		return
	}

	conf, err = parseConf(confval)
	if err != nil {
		return
	}

	if conf.maddr != masteraddr {
		s.master.(rmt.RemoteHop).Close()
		if s.master, err = hopclnt.Connect(s.proto, conf.maddr); err != nil {
			return
		}

		if !s.isServer() {
			if confver, confval, err = s.master.Get("#/conf", hop.Any); err != nil {
				return
			}
		}
	}

	// find out what hash function is being used
	_, khash, err = s.master.Get("#/keyhash", hop.Any)
	if err != nil {
		return
	}

	s.keyhash = GetKeyHash(string(khash))
	if s.keyhash == nil {
		return errors.New("unknown key hash function")
	}

	if s.isServer() {
		// set the local #/keyhash to the correct value
		s.khashentry.SetValue(khash)

		// setup the connection to the master
		s.cmap[s.master.Connection()] = masteraddr

		// accept requests from the master
		s.srv.NewConnection(s.master.Connection())

		// add the itself to the list of servers
		if confver, confvals, err = s.master.Atomic("#/conf", hop.Append, [][]byte{[]byte(s.addr)}); err != nil {
			return
		}
		confval = confvals[0]

		// tell the master that it can use the connection in the other direction
		_, err = s.master.Set("#/ctl", []byte(fmt.Sprintf("server %s", s.addr)))
		if err != nil {
			return
		}
	}

	// setup the connection to the master
	conn := new(Conn)
	conn.clnt = s.master
	s.srvmap[masteraddr] = conn

	conf, err = parseConf(confval)
	if err != nil {
		return
	}

	err = s.updateConf(conf)
	if err != nil {
		return
	}

	if s.isServer() {
		s.confentry.SetEntry(confver, confval)
	}

	go s.confproc(confver)
	return
}

func (s *D2Hop) initMaster() (err error) {
	s.keyhash = GetKeyHash(DefaultKeyHash)

	s.conf = new(Conf)
	s.conf.maddr = s.addr
	s.conf.srvnum = 1
	s.conf.srvaddrs = append(s.conf.srvaddrs, s.addr)
	s.conf.routes = RangeList(make([]Range, 1))
	s.conf.routes[0].addr = s.addr
	s.conf.routes[0].conn = s.selfconn

	confstr := s.masterUpdateConf()
	s.confentry.SetValue([]byte(confstr))
	return
}

func (s *D2Hop) isServer() bool {
	return s.addr != ""
}

func (s *D2Hop) isMaster() bool {
	return s.master == nil
}

func (s *D2Hop) getServer(key string) *Conn {
	hash := s.keyhash.Hash(key)

	s.RLock()
	defer s.RUnlock()
	r := s.routes.Search(hash)

	return r.conn
}

func (s *D2Hop) masterAddServer(addr string) {
	var r Range

	s.Lock()
	r.addr = addr
	s.conf.routes = append(s.conf.routes, r)
	s.conf.srvaddrs = append(s.conf.srvaddrs, addr)
	s.conf.srvnum++
	confstr := s.masterUpdateConf()
	s.Unlock()

	// tell everybody waiting that there is new configuration
	s.confentry.SetLocked([]byte(confstr))
}

func (s *D2Hop) masterRemoveServer(addr string) error {
	var routes RangeList

	s.Lock()
	h := s.srvmap[addr]
	if h == nil {
		s.Unlock()
		//		fmt.Printf("masterRemoveServer: address %s not found\n", addr)
		return nil
	}

	for i, ss := range s.conf.srvaddrs {
		if ss == addr {
			copy(s.conf.srvaddrs[i:], s.conf.srvaddrs[i+1:])
			s.conf.srvaddrs = s.conf.srvaddrs[0 : len(s.conf.srvaddrs)-1]
			break
		}
	}

	for i, _ := range s.conf.routes {
		r := &s.conf.routes[i]
		if r.addr != addr {
			routes = append(routes, *r)
		}
	}

	s.conf.routes = routes
	s.conf.srvnum--
	delete(s.srvmap, addr)
	confstr := s.masterUpdateConf()
	s.Unlock()

	// tell everybody waiting that there is new configuration
	s.confentry.SetLocked([]byte(confstr))
	if rhop, ok := h.clnt.(rmt.RemoteHop); ok {
		rhop.Close()
	}

	return nil
}

// called with s lock held
func (s *D2Hop) masterUpdateConf() string {
	conf := s.conf
	n := len(conf.routes)
	rsz := math.MaxUint32 / n
	for i, _ := range conf.routes {
		conf.routes[i].start = uint32(i * rsz)
		conf.routes[i].end = uint32((i + 1) * rsz)
	}

	conf.routes[len(conf.routes)-1].end = math.MaxUint32
	s.routes = conf.routes

	c := fmt.Sprintf("%s %d\n", s.addr, n)
	for _, r := range conf.routes {
		c += fmt.Sprintf("%s %d:%d\n", r.addr, r.start, r.end)
	}

	return c
}

// run by the normal servers/clients to update their knowledge of the rest of the servers
func (s *D2Hop) readUpdateConf(version uint64) (ver uint64, err error) {
	var val []byte
	var conf *Conf

	ver, val, err = s.master.Get("#/conf", version)
	if err != nil {
		//		fmt.Printf("Error: %v\n", err)
		return
	}

	conf, err = parseConf(val)
	if err != nil {
		return 0, err
	}

	err = s.updateConf(conf)
	if err != nil {
		return 0, err
	}

	s.confentry.SetEntry(ver, val)
	return ver, nil
}

// run by the normal servers/clients to update their knowledge of the rest of the servers
func (s *D2Hop) updateConf(conf *Conf) (err error) {
	var cmap map[rmt.Conn]string

	smap := make(map[string]*Conn)
	if s.isServer() {
		cmap = make(map[rmt.Conn]string)
	}

	s.RLock()
	// Connect to the other servers
	// Each server connects only to servers that appear before it in the
	// configuration list. The rest of the connections will be established
	// by the other server
	idx := 0
	for ; idx < len(conf.srvaddrs); idx++ {
		saddr := conf.srvaddrs[idx]
		if saddr == s.addr {
			// we don't need connection to ourselves
			break
		}

		if srv, ok := s.srvmap[saddr]; ok {
			smap[saddr] = srv
		} else if clnt, e := hopclnt.Connect(s.proto, saddr); e == nil {
			if s.isServer() {
				_, err = clnt.Set("#/ctl", []byte(fmt.Sprintf("server %s", s.addr)))
				if err != nil {
					s.RUnlock()
					return err
				}
			}

			conn := new(Conn)
			conn.clnt = clnt
			smap[saddr] = conn
			if s.isServer() {
				cmap[clnt.Connection()] = saddr
			}
		} else {
			err = e
			s.RUnlock()
			return
		}

		// else, the other side will connect to our server
		// and we are going to pick it up when it sends
		// 'server <addr>' command to our #/ctl key
	}

	for idx++; idx < len(conf.srvaddrs); idx++ {
		saddr := conf.srvaddrs[idx]
		if srv, ok := s.srvmap[saddr]; ok {
			smap[saddr] = srv
		}
	}

	// update the routes to point to the servers
	for i, _ := range conf.routes {
		r := &conf.routes[i]
		if c, ok := smap[r.addr]; ok {
			r.conn = c
		} else if r.addr == s.addr {
			r.conn = s.selfconn
		}
	}
	s.RUnlock()

	s.Lock()
	s.conf = conf
	s.master = smap[conf.srvaddrs[0]].clnt.(rmt.RemoteHop)
	s.srvmap = smap
	s.routes = conf.routes
	for c, a := range cmap {
		s.cmap[c] = a
	}
	s.Unlock()

	// make sure that we serve requests on the newly created connections
	for c, _ := range cmap {
		s.srv.NewConnection(c)
	}

	// TODO: close old connections
	return
}

func (s *D2Hop) confproc(version uint64) {
	var err error

	for {
		version, err = s.readUpdateConf(version + 1)
		if err != nil {
			return
		}
	}
}

func (s *D2Hop) heartbeatproc() {
	var pc []*Conn

	tick := time.Tick(time.Minute)
	for !s.closed {
		<-tick
		t := time.Now()

		//		fmt.Printf("Tick: %v\n", t)
		s.RLock()
		pc = nil
		for a, c := range s.srvmap {
			if a == s.addr || (!s.isMaster() && c.clnt != s.master) {
				continue
			}

			//			fmt.Printf("connection '%s' %d\n", a, t.Sub(c.alive))
			if t.Sub(c.alive) > time.Minute*2 && c.clnt != nil {
				pc = append(pc, c)
			}
		}
		s.RUnlock()

		for _, c := range pc {
			if _, _, err := c.clnt.Get("#/id", hop.Any); err != nil {
				//				fmt.Printf("heartbeat failed: %v\n", err)
				if rhop, ok := c.clnt.(rmt.RemoteHop); ok {
					rhop.Close()
				}
			}
		}
	}
}

func (s *D2Hop) ConnOpened(c *hopsrv.Conn) {
	var conn *Conn

//	fmt.Printf("ConnOpened: %v\n", c)

	s.Lock()
	if addr, ok := s.cmap[c.Connection()]; ok {
		delete(s.cmap, c.Connection())
		conn = s.srvmap[addr]
	}
	s.Unlock()

	if conn == nil {
		conn = new(Conn)
	}

	conn.srv = s
	conn.conn = c
	conn.alive = time.Now()
	c.SetOps(conn)
}

func (s *D2Hop) ConnClosed(c *hopsrv.Conn) {
	var addr string

//	fmt.Printf("ConnClosed: %v\n", c)
	s.RLock()
	for a, cc := range s.srvmap {
		if c == cc.conn {
			addr = a
			break
		}
	}
	s.RUnlock()

	if addr != "" {
		s.masterRemoveServer(addr)
	}

}

func (s *D2Hop) ctl(c *Conn, cmd string) error {
	if strings.HasPrefix(cmd, "server ") {
		addr := cmd[7:]
		c.clnt = hopclnt.NewClient(c.conn.Connection())

		s.Lock()
		s.srvmap[addr] = c
		for i, _ := range s.routes {
			r := &s.routes[i]

			if r.addr == addr {
				r.conn = c
			}
		}
		s.Unlock()

		return nil
	} else {
		return errors.New("unknown command")
	}
}

// make D2Hop implement the RemoteHop interface
func (s *D2Hop) SetDebugLevel(n int) {
	if s.srv != nil {
		s.srv.Debuglevel = n
	}
}

func (s *D2Hop) SetLogger(l *hop.Logger) {
	if s.srv != nil {
		s.srv.Log = l
	}
}

func (s *D2Hop) Connection() rmt.Conn {
	return nil
}

func (s *D2Hop) Close() {
	s.Lock()
	for _, c := range s.srvmap {
		if rhop, ok := c.clnt.(rmt.RemoteHop); ok {
			rhop.Close()
		}
	}
	s.closed = true
	s.Unlock()

	// TODO: stop listening?
}
