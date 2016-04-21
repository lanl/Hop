// Copyright 2013 The Hop Authors.  All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package hopclnt

import (
	"hop/rmt"
)

func (clnt *Clnt) Set(key string, value []byte) (ver uint64, err error) {
	var rc *rmt.Msg

	tc := clnt.conn.GetOutbound()
	err = rmt.PackTset(tc, key, value)
	if err != nil {
		clnt.conn.ReleaseOutbound(tc)
		return
	}

	rc, err = clnt.Rpc(tc)
	if err == nil {
		ver = rc.Version
	}

	if rc != nil {
		clnt.conn.ReleaseInbound(rc)
	}

	return
}
