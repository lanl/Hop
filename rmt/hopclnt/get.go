// Copyright 2013 The Hop Authors.  All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package hopclnt

import (
	"hop/rmt"
)

func (clnt *Clnt) Get(key string, version uint64) (ver uint64, val []byte, err error) {
	var rc *rmt.Msg

	tc := clnt.conn.GetOutbound()
	err = rmt.PackTget(tc, key, version)
	if err != nil {
		clnt.conn.ReleaseOutbound(tc)
		return
	}

	rc, err = clnt.Rpc(tc)
	if err == nil {
		ver = rc.Version
		val = rc.Value
	}

	if rc != nil {
		clnt.conn.ReleaseInbound(rc)
	}

	return
}
