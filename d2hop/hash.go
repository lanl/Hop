// Copyright 2013 The Hop Authors.  All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package d2hop

import (
	"hash"
	"hash/adler32"
	"hash/fnv"
)

type Hash interface {
	New() hash.Hash32
	Name() string
}

type KeyHash struct {
	hash     Hash
	hashchan chan hash.Hash32
}

type adler32type int
type fnv1type int
type fnv1atype int

var keyhashMap map[string]*KeyHash

func GetKeyHash(hashname string) *KeyHash {
	return keyhashMap[hashname]
}

func (kh *KeyHash) Hash(key string) (hcode uint32) {
	var h hash.Hash32

	// get the hash function
	select {
	case h = <-kh.hashchan:
		// got one from the chan
	default:
		h = kh.hash.New()
	}

	h.Reset()
	h.Write([]byte(key))
	hcode = h.Sum32()

	// release the hash function
	select {
	case kh.hashchan <- h:
	default:
	}

	return
}

func (kh *KeyHash) Name() string {
	return kh.hash.Name()
}

func newKeyHash(h Hash) *KeyHash {
	kh := new(KeyHash)
	kh.hash = h
	kh.hashchan = make(chan hash.Hash32, 64)

	return kh
}

func (*adler32type) New() hash.Hash32 {
	return adler32.New()
}

func (*adler32type) Name() string {
	return "adler32"
}

func (*fnv1type) New() hash.Hash32 {
	return fnv.New32()
}

func (*fnv1type) Name() string {
	return "fnv1"
}

func (*fnv1atype) New() hash.Hash32 {
	return fnv.New32a()
}

func (*fnv1atype) Name() string {
	return "fnv1a"
}

func init() {
	keyhashMap = make(map[string]*KeyHash)

	keyhashMap["adler32"] = newKeyHash(new(adler32type))
	keyhashMap["fnv1"] = newKeyHash(new(fnv1type))
	keyhashMap["fnv1a"] = newKeyHash(new(fnv1atype))
}
