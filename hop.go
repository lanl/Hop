// Copyright 2013 The Hop Authors.  All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package hop

import "errors"

type Hop interface {
	// Create add a new entry to the key-value store. The content of the
	// flags parameter is implementation dependent
	Create(key, flags string, value []byte) (ver uint64, err error)

	// Removes an entry from the key-value store.
	Remove(key string) (err error)

	// Retrieves the value for the specified key and version. If necessary,
	// the call waits until the entry's version becomes greater than the
	// specified. Version 0 (Any) returns immediately any value available.
	// Version 2^31-1 (Newest) tries to return the most up-to-date value.
	// Version 2^32-1 (PastNewest) waits until a new value is set and
	// returns it. Returns the version and the value.
	Get(key string, version uint64) (ver uint64, val []byte, err error)

	// Stores new value for the specified key. Returns the new version.
	Set(key string, value []byte) (ver uint64, err error)

	// Checks if the entry's current version and value match the specified
	// oldversion and oldvalue. If true, stores the new value and returns
	// the new version. If oldversion is Any, doesn't compare the versions.
	// If oldvalue is nil, doesn't compare the values.
	// TestSet(key, Any, nil, val) is equivalent to Set(key, val)
	TestSet(key string, oldversion uint64, oldvalue, value []byte) (ver uint64, val []byte, err error)

	// Atomically executes the specified operation on the entry's value.
	// Returns the new version and list of values. Number of specified
	// values, as well as the number of returned ones depends on the 
	// operation type. If any values are returned, the first value in the
	// list should be the new value of the entry
	Atomic(key string, op uint16, values [][]byte) (ver uint64, vals [][]byte, err error)
}

// Separate interfaces for each operation
type CreatorHop interface {
	Create(key, flags string, value []byte) (ver uint64, err error)
	Remove(key string) (err error)
}

type GetterHop interface {
	Get(key string, version uint64) (ver uint64, val []byte, err error)
}

type SetterHop interface {
	Set(key string, value []byte) (ver uint64, err error)
}

type TestSetterHop interface {
	TestSet(key string, oldversion uint64, oldvalue, value []byte) (ver uint64, val []byte, err error)
}

type AtomicHop interface {
	Atomic(key string, op uint16, values [][]byte) (ver uint64, vals [][]byte, err error)
}

// Version values
const (
	Any        = 0
	Lowest     = 1
	Highest    = Newest - 1
	Newest     = 0x7FFFFFFFFFFFFFFF
	Removed	   = 0x8000000000000000
	PastNewest = 0xFFFFFFFFFFFFFFFF
)

// Atomic Set operations
const (
	// Atomically add the specified value to the current value.
	// The current value and the specified one need to be the same length.
	// Supports byte array lengths of 1, 2, 4, and 8, assumes little-endian
	// order, and converts them to the appropriate unsigned integer.
	Add = iota

	// Atomically subtracts the specified value from the current value.
	// Same requirements as AtomicAdd.
	Sub

	// If the specified value is nil, atomically set/clear one bit in the
	// current value that was zero before. Returns two byte arrays: the
	// new value of the entry, and the 'address' of the bit set/cleared as
	// a 32-bit integer, represented as 4-byte array.
	BitSet
	BitClear

	// Atomically append the specified value to the end of the current value
	Append

	// Atomically remove all matches of the specified value from the current
	// value. If there are no matches, the entry's value and version are
	// not modified
	Remove

	// Atomically replace all matches of the first specified value with the
	// second specified value. If there are no matches, the entry's value
	// and version are not modified
	Replace
)

var Eperm = errors.New("permission denied")
var Eexist = errors.New("key exists")
var Enoent = errors.New("key doesn't exist")
var Eremoved = errors.New("key removed")
