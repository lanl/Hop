// Copyright 2013 The Hop Authors.  All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package chord

import (
	"crypto/sha1"
	"hash"
	"hash/fnv"
	"hop"
)

type Hash interface {
	New() hash.Hash64
	Name() string
}

type KeyHash struct {
	hash     Hash
	hashchan chan hash.Hash64
}

type sha1Hash struct {
	hash.Hash
}

type fnv1type int
type fnv1atype int
type sha1type int

var keyhashMap map[string]*KeyHash

func GetKeyHash(hashname string) *KeyHash {
	return keyhashMap[hashname]
}

func (kh *KeyHash) Hash(key string) (hcode uint64) {
	var h hash.Hash64

	// get the hash function
	select {
	case h = <-kh.hashchan:
		// got one from the chan
	default:
		h = kh.hash.New()
	}

	h.Reset()
	h.Write([]byte(key))
	hcode = h.Sum64()

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
	kh.hashchan = make(chan hash.Hash64, 64)

	return kh
}

func (*fnv1type) New() hash.Hash64 {
	return fnv.New64()
}

func (*fnv1type) Name() string {
	return "fnv1"
}

func (*fnv1atype) New() hash.Hash64 {
	return fnv.New64a()
}

func (*fnv1atype) Name() string {
	return "fnv1a"
}

func (*sha1type) New() hash.Hash64 {
	h := new(sha1Hash)
	h.Hash = sha1.New()

	return h
}

func (*sha1type) Name() string {
	return "sha1"
}

func (h *sha1Hash) Sum64() uint64 {
	n, _ := hop.Gint64(h.Sum(nil))
	return n
}

func init() {
	keyhashMap = make(map[string]*KeyHash)

	keyhashMap["fnv1"] = newKeyHash(new(fnv1type))
	keyhashMap["fnv1a"] = newKeyHash(new(fnv1atype))
	keyhashMap["sha1"] = newKeyHash(new(sha1type))
}
