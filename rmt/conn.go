// Copyright 2013 The Hop Authors.  All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package rmt

import (
	"errors"
	"fmt"
)

type MsgHandler interface {
	Incoming(m *Msg)
	ConnError(error)
}

type Conn interface {
	// Returns a message to be used for sending.
	GetOutbound() *Msg

	// Releases a message allocated for sending.
	// Send will automatically call ReleaseOutbound once the message
	// sent.
	ReleaseOutbound(*Msg)

	// Releases a message that contains incoming data.
	// The Msg should be one passed to the Incoming method of the
	// MsgHandler interface assigned to the connection
	ReleaseInbound(*Msg)

	// Assigns a handler to receive incoming request (T messages)
	SetRequestHandler(h MsgHandler)

	// Assigns a handler to receive incoming responses (R messages)
	SetResponseHandler(h MsgHandler)

	// Sends a message. The message should one of the messages
	// previously retrieved by calling GetOutbound.
	// Automatically calls ReleaseOutbound once the message is sent.
	Send(m *Msg) error

	Close()
	RemoteAddr() string
	LocalAddr() string
}

type Listener interface {
	NewConnection(c Conn)
}

type Protocol interface {
	Connect(proto, addr string) (Conn, error)
	Listen(proto, addr string, lstn Listener) (string, error)
}

var protos map[string]Protocol
var Eproto = errors.New("unknown protocol")

func init() {
	protos = make(map[string]Protocol)
}

func AddProtocol(proto string, p Protocol) error {
	if protos[proto] != nil {
		return errors.New(fmt.Sprintf("protocol %s already registered", proto))
	}

	protos[proto] = p
	return nil
}

func getProtocol(proto string) Protocol {
	return protos[proto]
}

func Connect(proto, addr string) (Conn, error) {
	p := getProtocol(proto)
	if p == nil {
		return nil, Eproto
	}

	return p.Connect(proto, addr)
}

func Listen(proto, addr string, lstn Listener) (string, error) {
	p := getProtocol(proto)
	if p == nil {
		return "", Eproto
	}

	return p.Listen(proto, addr, lstn)
}
