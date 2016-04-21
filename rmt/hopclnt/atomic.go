// Copyright 2013 The Hop Authors.  All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package hopclnt

import (
	"hop/rmt"
)

func (clnt *Clnt) Atomic(key string, op uint16, vals [][]byte) (version uint64, values [][]byte, err error) {
	var rc *rmt.Msg

	tc := clnt.conn.GetOutbound()
	err = rmt.PackTatomic(tc, op, key, vals)
	if err != nil {
		clnt.conn.ReleaseOutbound(tc)
		return
	}

	rc, err = clnt.Rpc(tc)
	if err == nil {
		version = rc.Version
		values = rc.Vals
	}

	if rc != nil {
		clnt.conn.ReleaseInbound(rc)
	}

	return
}
