// Copyright 2013 The Hop Authors.  All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package hop

import (
	"errors"
	"fmt"
)

// MHop is a Hop implementation that allows redirection to other Hop
// implmenentations based on the key prefix.

type MHop struct {
	dflt  interface{}
	root  *mnode
	minid int
	maxid int
}

type mnode struct {
	prefix  string
	id      int
	hop     interface{}
	exact   bool
	cutpref bool
	sub     []*mnode
	prev    *mnode
}

func NewMHop(dflt interface{}) *MHop {
	m := new(MHop)
	m.dflt = dflt

	return m
}

func (m *MHop) SetDefault(dflt interface{}) {
	m.dflt = dflt
}

func (m *MHop) AddBefore(pattern string, exact bool, cutprefix bool, hop interface{}) error {
	m.minid--
	return m.add(m.minid, pattern, exact, cutprefix, hop)
}

func (m *MHop) AddAfter(pattern string, exact bool, cutprefix bool, hop interface{}) error {
	m.maxid++
	return m.add(m.maxid, pattern, exact, cutprefix, hop)
}

func (m *MHop) Create(key, flags string, value []byte) (ver uint64, err error) {
	hop, nkey := m.find(key)

	if chop, ok := hop.(CreatorHop); ok {
		return chop.Create(nkey, flags, value)
	} else {
		return 0, Eperm
	}
}

func (m *MHop) Remove(key string) (err error) {
	hop, nkey := m.find(key)

	if chop, ok := hop.(CreatorHop); ok {
		return chop.Remove(nkey)
	} else {
		return Eperm
	}
}

func (m *MHop) Get(key string, version uint64) (ver uint64, val []byte, err error) {
	hop, nkey := m.find(key)

	if ghop, ok := hop.(GetterHop); ok {
		return ghop.Get(nkey, version)
	} else {
		return 0, nil, Eperm
	}
}

func (m *MHop) Set(key string, value []byte) (ver uint64, err error) {
	hop, nkey := m.find(key)

	if shop, ok := hop.(SetterHop); ok {
		return shop.Set(nkey, value)
	} else {
		return 0, Eperm
	}
}

func (m *MHop) TestSet(key string, oldversion uint64, oldvalue, value []byte) (ver uint64, val []byte, err error) {
	hop, nkey := m.find(key)

	if shop, ok := hop.(TestSetterHop); ok {
		return shop.TestSet(nkey, oldversion, oldvalue, value)
	} else {
		return 0, nil, Eperm
	}
}

func (m *MHop) Atomic(key string, op uint16, values [][]byte) (ver uint64, vals [][]byte, err error) {
	hop, nkey := m.find(key)

	if shop, ok := hop.(AtomicHop); ok {
		return shop.Atomic(nkey, op, values)
	} else {
		return 0, nil, Eperm
	}
}

func (m *MHop) find(key string) (hop interface{}, newkey string) {
	ndprev, _, n, _ := m.match(key)

	if ndprev == nil {
		return m.dflt, key
	}

	// All nodes from ndprev up to root match the key
	// Find the matching node (that has Hop attached) with the highest id
	nd := ndprev
	for nd1 := nd; nd1 != nil; nd1 = nd1.prev {
		if nd.hop == nil {
			nd = nd1
		} else if nd1.hop != nil && nd.id < nd1.id {
			nd = nd1
		}
	}

	//	fmt.Printf("nd.exact %v\n", nd.exact)
	if nd.hop == nil || (nd.exact && n < len(key)) {
		return m.dflt, key
	}

	if nd.cutpref {
		newkey = key[n:]
	} else {
		newkey = key
	}

	//	fmt.Printf("nd.hop %T newkey %s\n", nd.hop, newkey)
	return nd.hop, newkey
}

func (m *MHop) add(id int, pattern string, exact bool, cutprefix bool, h interface{}) error {
	ndprev, nd, n, i := m.match(pattern)
	//	fmt.Printf("ndprev %v nd %v n %d i %d\n", ndprev, nd, n, i)
	if nd == nil {
		if n == len(pattern) {
			return errors.New("pattern already in the list")
		}

		nd1 := new(mnode)
		nd1.prefix = pattern[n:]
		nd1.id = id
		nd1.exact = exact
		nd1.cutpref = cutprefix
		nd1.hop = h
		if ndprev != nil {
			ndprev.sub = append(ndprev.sub, nd1)
			nd1.prev = ndprev
		} else {
			m.root = nd1
		}

		return nil
	} else {
		// split the node
		nd1 := new(mnode)
		nd1.prefix = nd.prefix[0:i]
		nd1.prev = nd.prev

		if n+i == len(pattern) {
			// we can set the new pattern at the intermediate node
			nd1.id = id
			nd1.exact = exact
			nd1.cutpref = cutprefix
			nd1.hop = h
		} else {
			nd2 := new(mnode)
			nd2.prefix = pattern[n+i:]
			nd2.id = id
			nd2.exact = exact
			nd2.cutpref = cutprefix
			nd2.hop = h

			nd1.sub = append(nd1.sub, nd2)
			nd2.prev = nd1
		}

		// move the current intermediate node down
		nd.prefix = nd.prefix[i:]
		nd1.sub = append(nd1.sub, nd)
		nd.prev = nd1

		if ndprev == nil {
			m.root = nd1
		} else {
			for m, nnd := range ndprev.sub {
				if nnd == nd {
					ndprev.sub[m] = nd1
					break
				}
			}
		}
	}

	return nil
}

// Tries to match the specified pattern.
// If nd is nil, ndprev matches the pattern up to n-th character and
// no further matching is possible. If nd is non-nil, it matches the
// pattern up to the n+i-th character, where i is the i-th character
// of its prefix
func (m *MHop) match(pattern string) (ndprev *mnode, nd *mnode, n int, i int) {
	nd = m.root
	n = 0
	pidx := 0
	plen := len(pattern)
	//	fmt.Printf("MHop.match '%s' nd %v\n", pattern, nd)
L:
	for (nd != nil) && n < plen {
		m := len(nd.prefix)
		if m > plen-n {
			m = plen - n
		}
		/*
			if len(nd.prefix) > (plen - n) {
				// the prefix is longer than the rest of the pattern,
				// can't match it
				if ndprev != nil && pidx < len(ndprev.sub) {
					pidx++
					nd = ndprev.sub[pidx]
				} else {
					nd = nil
				}
				continue
			}
		*/

		for i = 0; i < len(nd.prefix); i++ {
			if nd.prefix[i] != pattern[i+n] {
				break
			}
		}

		//		fmt.Printf("MHop.match i %d n %d plen %d\n", i, n, plen)
		switch i {
		case 0:
			// no match at all, next sub
			nd = nil
			if ndprev != nil && pidx < len(ndprev.sub) {
				pidx++
				if pidx < len(ndprev.sub) {
					nd = ndprev.sub[pidx]
				}
			}
			continue

		case len(nd.prefix):
			// matched the whole prefix, descend (if possible)
			ndprev = nd
			pidx = 0
			n += i
			i = 0
			if ndprev.sub != nil && len(ndprev.sub) != 0 {
				nd = ndprev.sub[0]
			} else {
				nd = nil
			}
			continue

		default:
			// partial match
			break L
		}
	}

	// At this point, if nd is nil, ndprev matches the pattern
	// up to n-th character and no further matching is possible.
	// If nd is non-nil, it matches the pattern up to the n+i-th
	// character, where i is the i-th character of its prefix
	//	fmt.Printf("MHop.match ndprev %p %p %d %d\n", ndprev, nd, n, i)
	return ndprev, nd, n, i
}

func (m *MHop) String() string {
	if m.root == nil {
		return "()"
	} else {
		return m.root.String()
	}
}

func (nd *mnode) String() string {
	s := fmt.Sprintf("(\"%s\" %p [", nd.prefix, nd.hop)
	for _, nd1 := range nd.sub {
		s += nd1.String() + " "
	}
	s += "])"

	return s
}
