// Copyright 2013 The Hop Authors.  All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package chord

import (
	"errors"
	"fmt"
	"hop"
	"hop/rmt"
	"hop/rmt/hopclnt"
	"hop/rmt/hopsrv"
	"hop/shop"
	"math"
_	"math/rand"
	"strconv"
	"strings"
	"sync"
	"time"
)

const (
	// Set (conditionally) the predecessor to value[0] and return
	// the previous predecessor as val[0]
	PredAndNotify = hop.Replace + 1
)

// Local Entries
//
// #/chord/id
//	Node's Chord ID
//
// #/chord/successor[:XXXX]
//	Returns the successor of the specified hash. If XXXX is not specified
//	returns the successor of the current node. The format of the value is:
//		<successor_id> <successor_addr> <on_cycle>
//
// #/chord/predecessor
//	Returns the predecessor of the node. The AtomicSet operation
//	PredAndNotify returns the predecessor and tells the node that the
//	specified value describes a node that might be the node's predecessor
//
// #/chord/finger
//	Returns the finger table of the node. For debugging only, not used
//	to implement Chord
//
// #/chord/ring
//	Returns the list of nodes on the ring. If a node is in the finger list
//	its indices are listed. For debugging only, not used to implement Chord
//

type Chord struct {
	sync.RWMutex
	proto		string
	addr		string
	hop		hop.Hop
	srv		*hopsrv.Srv	// if nil, client
	keyhash		*KeyHash
	self		Node
	closed		bool

	// chord data
	finger		[]*Node
	predecessor	*Node
	nodecache	map[string] *Node

	// local entries
	lents		*shop.SHop
	identry		IdEntry
	khashentry	*hop.Entry
	succentry	SuccEntry
	predentry	PredEntry
	figentry	FingerEntry
	ringentry	RingEntry
	stackentry	StackEntry

	// stabilization data
	modchan		chan bool

	// strong stabilization data
	successor1	*Node
}

type Node struct {
	sync.Mutex
	id		uint64
	addr		string
	ref		int32
	oncycle		bool
	srv		*Chord
	clnt		hop.Hop
}

var DefaultKeyHash = "sha1"
var Edisconnect = errors.New("disconnected")

func NewChord(proto, listenaddr, nodeaddr string, chop hop.Hop) (s *Chord, err error) {
	var clnt rmt.RemoteHop

	s = new(Chord)
	s.proto = proto
	s.addr = listenaddr
	s.hop = chop
	s.finger = make([]*Node, 64)
	s.nodecache = make(map[string] *Node)
	s.modchan = make(chan bool, 4)

	if s.isServer() {
		s.startServer()

	}

	if nodeaddr == "" || nodeaddr == listenaddr {
		s.keyhash = GetKeyHash(DefaultKeyHash)
		s.self.oncycle = true
	} else {
		var khash []byte

		if clnt, err = hopclnt.Connect(s.proto, nodeaddr); err != nil {
			return nil, err
		}

		// find out what hash function is being used
		_, khash, err = clnt.Get("#/keyhash", hop.Any)
		if err != nil {
			return
		}

		s.keyhash = GetKeyHash(string(khash))
		if s.keyhash == nil {
			return nil, errors.New("unknown key hash function")
		}
	}

	s.self.id = s.keyhash.Hash(s.addr)
	s.self.addr = s.addr
	s.self.clnt = chop
	s.self.ref = 1
	s.self.srv = s
	s.nodecache[s.addr] = &s.self

	if clnt != nil {
		err = s.join(clnt)
		clnt.Close()
	}

	if err != nil {
		return nil, err
	}

	go s.stabilizeproc()

	return s, nil
}

func Connect(proto, addr string) (*Chord, error) {
	return NewChord(proto, "", addr, nil)
}

func (s *Chord) isServer() bool {
	return s.addr != ""
}

func (s *Chord) startServer() error {
	s.srv = new(hopsrv.Srv)

	_, hopid, err := s.hop.Get("#/id", hop.Any)
	if err != nil {
		hopid = nil
	}

	// add some local entries
	s.lents = shop.NewSHop()
	id := "Chord"
	if hopid != nil {
		id += " (" + string(hopid) + ")"
	}

	s.lents.RemoveEntry("#/id")
	s.lents.AddEntry("#/id", []byte(id), nil)
	s.khashentry, _ = s.lents.AddEntry("#/keyhash", []byte(DefaultKeyHash), nil)

//	s.keysentry.s = s
//	s.lents.AddEntry("#/keys", nil, &s.keysentry)

	// chord entries
	s.identry.s = s
	s.lents.AddEntry("#/chord/id", nil, &s.identry)
	s.succentry.s = s
	s.lents.AddEntry("#/chord/successor", nil, &s.succentry)
	s.predentry.s = s
	s.lents.AddEntry("#/chord/predecessor", nil, &s.predentry)
	s.figentry.s = s
	s.lents.AddEntry("#/chord/finger", nil, &s.figentry)
	s.ringentry.s = s
	s.lents.AddEntry("#/chord/ring", nil, &s.ringentry)
	s.stackentry.s = s
	s.lents.AddEntry("#/chord/stack", nil, &s.stackentry)

	if !s.srv.Start(s) {
		return errors.New("Error: can't start the server")
	}

	if _, err := rmt.Listen(s.proto, s.addr, s.srv); err != nil {
		return err
	}

	return nil
}

func (s *Chord) start(k uint) uint64 {
	return s.self.id + (1<<k)
}

func (s *Chord) getServer(key string) hop.Hop {
	nd, _ := s.findLocalSuccessor(s.keyhash.Hash(key))
	return nd.clnt
}

func (s *Chord) getNode(key string) *Node {
	nd, _ := s.findLocalSuccessor(s.keyhash.Hash(key))
	return nd
}

// From the Chord paper:
// n.find_successor(id)
// 	if (key belongs to (n, n.successor])
//		return n.successor
//	else
//		n1 = closest_preceding_node(id)
//		return n1.find_successor(id)
//
// n.closest_preceding_node(id)
//	for i = m downto 1
//		if (finger[i] belongs-to (n, id))
//			return finger[i]
//	return n
//
func (s *Chord) findLocalSuccessor(id uint64) (nd *Node, exact bool) {
	s.RLock()
	defer s.RUnlock()

	if s.isServer() && s.predecessor!=nil && between(id, s.predecessor.id, s.self.id) {
		return &s.self, true
	}

	if s.finger[0] == nil {
		// we are the only node in the ring
//		fmt.Printf("Chord.findLocalSuccessor(%016x): self\n", id)
		return &s.self, true
	}

//	fmt.Printf("Chord.findLocalSuccessor(%016x) check successor: %016x\n", id, s.finger[0].id)
	if between(id, s.self.id, s.finger[0].id) {
//		fmt.Printf("Chord.findLocalSuccessor(%016x): successor\n", id)
		return s.finger[0], true
	}

	for i := len(s.finger) - 1; i >= 0; i-- {
//		if s.finger[i]!=nil {
//			fmt.Printf("Chord.findLocalSuccessor(%016x) check finger %d: %016x\n", id, i, s.finger[i].id)
//		}

		if s.finger[i]!=nil && between(s.finger[i].id, s.self.id, id-1) {
//			fmt.Printf("Chord.findLocalSuccessor(%016x): finger %d\n", id, i)
			return s.finger[i], false
		}
	}

//	fmt.Printf("Chord.findLocalSuccessor(%016x): self 2\n", id)
	return &s.self, true
}

func (s *Chord) findSuccessor(id uint64) (nd *Node, err error) {
	lnd, exact := s.findLocalSuccessor(id)
//	fmt.Printf("Chord.findSuccessor: local successor(%016x): %016x:%s exact %v\n", id, lnd.id, lnd.addr, exact)
	if exact {
		return lnd, nil
	}

	nd, err = s.getSuccessor(lnd.clnt, id)
	if err != nil {
		s.checkClosed(lnd)
	}
	return
}

// From the Chord paper:
// Weak stabilization:
// n.join(n1)
//	predecessor = nil
//	s = n1.find_successor(n)
//	build_fingers(s)
//	successor = s
//
// n.build_fingers(n1)
//	i0 = log(successor - n) + 1
//	for each i >= i0 index into finger[]
//		finger[i] = n1.find_successor(n+2^(i-1))
//
// Strong stabilization:
// n.join(n1)
//	on_cycle = false
//	predecessor = nil
//	s = n1.find_successor(n)
//	while (not s.on_cycle) do
//		s = s.find_successor(n1)
//	successor[0] = s
//	successor[1] = s
//
func (s *Chord) join(clnt hop.Hop) error {
	s.predecessor = nil
	succ, err := s.getSuccessor(clnt, s.self.id)
	if err != nil {
		return err
	}

	err = succ.Connect()
	if err != nil {
		return err
	}

	for !succ.oncycle {
		succ, err = s.getSuccessor(succ.clnt, s.self.id)
	}

	i0 := uint(math.Log2(float64(succ.id - s.self.id)))
	if i0==0 {
		fmt.Printf("i0 == 0\n")
	}

	for i := i0; i < uint(len(s.finger)); i++ {
		s.finger[i], err = s.getSuccessor(succ.clnt, s.start(uint(i)))
		if err != nil {
			s.checkClosed(succ)
			return err
		}

		err = s.finger[i].Connect()
		if err != nil {
			return err
		}

//		fmt.Printf("new finger[%d] start:%016x new %016x:%s\n", i, s.start(uint(i)), s.finger[i].id, s.finger[i].addr)
		
	}

	s.finger[0] = succ
	s.successor1 = succ
//	fmt.Printf("successor1 %v\n", s.successor1)
	return nil
}

// From the Chord paper:
// Weak stabilization:
//// periodically verify n's immediate successor andtell the successor about n
// n.stabilize()
//	x = successor.predecessor
//	if (x belongs-to (n, successor))
//		successor = x
//	successor.notify(n)
//
//// n1 thinks it might be our predecessor
// n.notify(n1)
//	if (predecessor is nil or n1 belongs-to (predecessor, n))
//		predecessor = n1
//
// Strong stabilization:
//
// n.stabilize()
//	u = successor[0].find_successor(n)
//	on_cycle = (u == n)
//	if (successor[0] == successor[1] and u belongs-to (n, successor[1])
//		successor[1] = u
//	for(i=0, 1)
//		update_and_notify(i)
//
// n.update_and_notify(i)
//	s = successor[i]
//	x = s.predecessor
//	if (x belongs-to (n, s))
//		successor[i] = x
//	s.notify(n)
//
func (s *Chord) stabilize() (modified bool) {
	var err error
	var nd, newpred, newsucc0, newsucc1 *Node
	var doagain bool

again:
	s.RLock()
	succ0 := s.finger[0]
	succ1 := s.successor1
	pred := s.predecessor
	s.RUnlock()
	if succ0 == nil {
		return
	}

	if succ1 == nil {
		succ1 = succ0
	}

	nd, err = s.getSuccessor(succ0.clnt, s.self.id)
	if err != nil {
		return true
	}

	oncycle := nd.id == s.self.id
	if succ0 == succ1 && between(nd.id, s.self.id, succ1.id-1) {
		err = nd.Connect()
		if err != nil {
			return true
		}
		succ1 = nd
	}

	newpred, newsucc0, doagain = s.updateNotify(pred, succ0)
	if doagain {
		goto again
	}

	_, newsucc1, doagain = s.updateNotify(pred, succ1)
	if doagain {
		goto again
	}

	s.Lock()
	s.self.oncycle = oncycle
	if s.successor1 != succ1 {
		s.successor1.DisconnectLocked()
		s.successor1 = succ1
		modified = true
	}

	if newsucc1 != nil {
		s.successor1.DisconnectLocked()
		s.successor1 = newsucc1
		modified = true
	}

	if newsucc0 != nil {
		s.finger[0].DisconnectLocked()
		s.finger[0] = newsucc0
		modified = true
	}

	if newpred != nil {
		s.predecessor.DisconnectLocked()
		s.predecessor = newpred
		modified = true
	}
	s.Unlock()

	return
}

func (s *Chord) updateNotify(pred, succ *Node) (newpred, newsucc *Node, again bool) {
	var ndval string
	var err error
	var nd *Node

	if s.isServer() {
		_, vals, err := succ.clnt.Atomic("#/chord/predecessor", PredAndNotify,
			[][]byte{[]byte(s.self.String())})

		if err == nil {
			if vals==nil || len(vals[0]) == 0 {
				return
			}

			ndval = string(vals[0])
		}
	} else {
		var val []byte

		_, val, err = succ.clnt.Get("#/chord/predecessor", hop.Any)
		if err == nil {
			ndval = string(val)
		}
	}

	if err != nil {
		fmt.Printf("Chord.updateNotify error %v\n", err)
		s.checkClosed(succ)
		again = true
		return
	}

	if ndval=="" {
		return
	}

	nd, err = s.newNode(ndval)
	if err != nil {
		return
	}

	if between(nd.id, s.self.id, succ.id-1) && nd.addr != succ.addr {
		err = nd.Connect()
		if err == nil {
			newsucc = nd
		}
	} else if !s.isServer() && (pred == nil || pred.addr != nd.addr) {
		// the client nodes are not really part of the ring, so they should
		// actively keep track of their predecessor
		err = nd.Connect()
		if err == nil {
			newpred = nd
		}
	}

	return
}

// From the Chord paper:
// n.fix_fingers
//	i = random 1..m
//	finger[i] = find_successor(n + 2^(i-1))
//
func (s *Chord) fixFinger(n int) (modified bool) {
//	n := rand.Int31n(int32(len(s.finger) - 1))
//	fmt.Printf("fixing finger %d\n", n)
	nd := s.finger[n]
	if nd == nil {
		for i := n; i >= 0; i-- {
			if s.finger[i] != nil {
				nd = s.finger[i]
				break
			}
		}

		if nd == nil {
			s.Lock()
			s.tryFindSuccessor()
			s.Unlock()
			nd = s.finger[0]
			if nd == nil {
				// nobody to talk to
				return
			}
		}
	}

	nd1, err := s.getSuccessor(nd.clnt, s.start(uint(n)))
	if err != nil {
//		fmt.Printf("find successor asking finger %d(%s) error: %v\n", n, nd.addr, err)
		s.checkClosed(nd)
		modified = true
		return
	}

	s.Lock()
	fn := s.finger[n]
	if fn==nil || fn.addr != nd1.addr || fn.id != nd1.id {
		err = nd1.ConnectLocked()
		if err != nil {
//			fmt.Printf("can't connect to new %016x:%s finger: %v\n", n, nd1.addr, err)
			modified = true
			s.Unlock()
			return
		}

//		if fn==nil {
//			fmt.Printf("new finger[%d] start:%016x new %016x:%s\n", n, s.start(uint(n)), nd1.id, nd1.addr)
//		} else {
//			fmt.Printf("new finger[%d] start:%016x new %016x:%s old %016x:%s\n", n, s.start(uint(n)), nd1.id, nd1.addr, fn.id, fn.addr)
//		}

		fn.DisconnectLocked()
		s.finger[n] = nd1
		modified = true
	}
	s.Unlock()
	return
}

func (s *Chord) stabilizeproc() {
//	period := time.Second		// ticker period
	period := 20 * time.Millisecond
//	tick := time.Tick(period)
	modcnt := 0			// number of sequential modifications
	runcnt := 0			// number of sequential runs without mods
	nfinger := 1

	for !s.closed {
		modified := false
		select {
		case <-time.After(period):

		case <-s.modchan:
			modified = true
		}	

		// check for closed connections
		s.Lock()
		pred := s.predecessor
		if pred != nil {
			if rh, ok := pred.clnt.(rmt.RemoteHop); ok && rh.Closed() {
//				fmt.Printf("new predecessor: nil\n")
				s.predecessor.DisconnectLocked()
				s.predecessor = nil
				modified = true
			}
		}
		s.Unlock()

		modified = modified || s.stabilize()
		modified = modified || s.fixFinger(nfinger)
		nfinger++
		if nfinger >= len(s.finger) {
			nfinger = 0
		}

		nperiod := period
		if modified {
			runcnt = 0
			modcnt++

			// if there were modification in both last runs,
			// decrease the time between runs
			if modcnt >= 1 {
				nperiod /= 2
			}

			if nperiod > time.Second {
				nperiod = time.Second
			}
		} else {
			runcnt++
			modcnt = 0

			// if the proc run three times without making any
			// modifications, increase the time between runs
			if runcnt > 7 {
				nperiod += nperiod/3
			}
		}

		if nperiod != period && nperiod > 10*time.Millisecond && nperiod < 1*time.Second {
			period = nperiod
//			tick = time.Tick(period)
		}
	}
}

func (s *Chord) checkClosed(nd *Node) {
	if rh, ok := nd.clnt.(rmt.RemoteHop); !ok || !rh.Closed() {
		return
	}

	// the node's connection is closed, remove all references to it
	// from the finger and predecessor
	s.Lock()
	if s.predecessor == nd {
//		fmt.Printf("new predecessor: nil\n")
		s.predecessor = nil
		nd.DisconnectLocked()
	}

	for i := 0; i < len(s.finger); i++ {
		if s.finger[i] == nd {
//			fmt.Printf("new finger[%d]: nil\n", i)
			s.finger[i] = nil
			nd.DisconnectLocked()
		}
	}

	if s.finger[0] == nil {
		s.tryFindSuccessor()
	}
	s.Unlock()
	s.ringModified()
}

// called with s lock held
func (s *Chord) tryFindSuccessor() {
	for i := 1; i < len(s.finger); i++ {
		if s.finger[i] != nil && s.finger[i] != &s.self {
			s.finger[0] = s.finger[i]
			s.finger[0].ConnectLocked()
			break
		}
	}

	if s.finger[0] == nil {
		// no luck in the finger table, let's try
		// the predecessor
		s.finger[0] = s.predecessor
		if s.finger[0] != nil {
			s.finger[0].ConnectLocked()
		}
	}

//	if s.finger[0] == nil {
//		fmt.Printf("tryFindSuccessor: failed\n")
//	} else {
//		fmt.Printf("tryFindSuccessor: %016x:%s\n", s.finger[0].id, s.finger[0].addr)
//	}
}

func (s *Chord) ringModified() {
	select {
		case s.modchan <- true:
	}
}

// make Chord implement the RemoteHop interface
func (s *Chord) SetDebugLevel(n int) {
	if s.srv != nil {
		s.srv.Debuglevel = n
	}
}

func (s *Chord) SetLogger(l *hop.Logger) {
	if s.srv != nil {
		s.srv.Log = l
	}
}

func (s *Chord) Connection() rmt.Conn {
	return nil
}

func (s *Chord) Close() {
	s.Lock()
	for _, c := range s.finger {
		if rhop, ok := c.clnt.(rmt.RemoteHop); ok {
			rhop.Close()
		}
	}
	s.closed = true
	s.Unlock()

	// TODO: stop listening?
}

func (s *Chord) Closed() bool {
	return s.closed
}

func (s *Chord) getSuccessor(clnt hop.Hop, id uint64) (nd *Node, err error) {
	var val []byte

	_, val, err = clnt.Get(fmt.Sprintf("#/chord/successor:%016x", id), hop.Any)
	if err != nil {
		return nil, err
	}

	nd, err = s.newNode(string(val))
	if err != nil {
		return nil, err
	}

	return
}

func (s *Chord) newNode(spec string) (nd *Node, err error) {
	var n uint64

	ss := strings.Split(spec, " ")
	if len(ss) < 2 || len(ss) > 3 {
		return nil, errors.New(fmt.Sprintf("invalid node spec: %s", spec))
	}

	addr := ss[1]
	n, err = strconv.ParseUint(ss[0], 16, 64)
	if err != nil {
		return nil, err
	}

	oncycle := len(ss) == 3 && ss[2] == "true"

	// TODO: clean the cache of stale nodes
	s.Lock()
	if nd = s.nodecache[addr]; nd == nil {
		nd = new(Node)
		nd.addr = addr
		nd.id = n
		nd.srv = s
		s.nodecache[addr] = nd
	}

	if oncycle {
		nd.oncycle = true
	}
	s.Unlock()
	return
}

func (nd *Node) Connect() (err error) {
	nd.Lock()
	nd.ref++
	if nd.ref == 1 {
		fmt.Printf("Node.Connect %s\n", nd.addr)
		nd.clnt, err = hopclnt.Connect(nd.srv.proto, nd.addr)
		if err != nil {
			nd.ref--
		} else {
			s := nd.srv
			s.Lock()
			s.nodecache[nd.addr] = nd
			s.Unlock()
		}
	}

	if rh, ok := nd.clnt.(rmt.RemoteHop); ok && rh.Closed() {
		err = Edisconnect
	}
	nd.Unlock()
	return
}

// same as connect, but nd.srv lock is already held
func (nd *Node) ConnectLocked() (err error) {
	nd.Lock()
	nd.ref++
	if nd.ref == 1 {
		fmt.Printf("Node.ConnectLocked %s\n", nd.addr)
		nd.clnt, err = hopclnt.Connect(nd.srv.proto, nd.addr)
		if err != nil {
			nd.ref--
		} else {
			nd.srv.nodecache[nd.addr] = nd
		}
	}

	if rh, ok := nd.clnt.(rmt.RemoteHop); ok && rh.Closed() {
		err = Edisconnect
	}
	nd.Unlock()
	return
}

func (nd *Node) Disconnect() {
	if nd == nil {
		return
	}

	nd.Lock()
	nd.ref--
	if nd.ref == 0 {
		s := nd.srv
		s.Lock()
		delete(s.nodecache, nd.addr)
		s.Unlock()

		fmt.Printf("Node.Disconnect %s\n", nd.addr)
		nd.clnt.(rmt.RemoteHop).Close()
	}
	nd.Unlock()
}

// same as Disconnect, but nd.srv lock is already held
func (nd *Node) DisconnectLocked() {
	if nd == nil {
		return
	}

	nd.Lock()
	nd.ref--
	if nd.ref == 0 {
		delete(nd.srv.nodecache, nd.addr)
		fmt.Printf("Node.DisconnectLocked %s\n", nd.addr)
		nd.clnt.(rmt.RemoteHop).Close()
	}
	nd.Unlock()
}

func (nd *Node) String() (ret string) {
	if nd != nil {
		ret = fmt.Sprintf("%016x %s %v", nd.id, nd.addr, nd.oncycle)
	}

	return
}

func between(n, low, high uint64) (ret bool) {
	if low > high {
		ret = n > low || n <= high
	} else {
		ret = n > low && n <= high
	}

//	fmt.Printf("between %016x (%016x, %016x]: %v\n", n, low, high, ret)
	return
}
