// Copyright 2013 The Hop Authors.  All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package main

// An interactive client for Hop servers.

import (
	"bufio"
	"bytes"
	"flag"
	"fmt"
	"hop"
	"os"
	"strconv"
	"strings"
)

var prompt = flag.String("prompt", "hop> ", "prompt for interactive client")

type Cmd struct {
	fun   func(c hop.Hop, s []string)
	nargs int
	help  string
}

var cmds map[string]*Cmd

func init() {
	cmds = make(map[string]*Cmd)
	cmds["create"] = &Cmd{cmdcreate, 2, "create key flags [value]\t«create a new entry with key and assign the value»"}
	cmds["remove"] = &Cmd{cmdremove, 1, "remove key\t«removes an entry with the specified key»"}
	cmds["get"] = &Cmd{cmdget, 1, "get key [version]\t«gets the value for the specified key»"}
	cmds["getn"] = &Cmd{cmdgetn, 1, "getn key [version]\t«gets the numeric value for the specified key»"}
	cmds["gets"] = &Cmd{cmdgets, 1, "gets key [version]\t«gets the string value for the specified key»"}
	cmds["set"] = &Cmd{cmdset, 1, "set key [value]\t«sets the value for a key, nil if the value is not specified»"}
	cmds["tas"] = &Cmd{cmdtas, 1, "tas key version [ oldvalue | value ]\t«if the version/oldvalue match, set to the new value»"}
	cmds["add8"] = &Cmd{cmdadd, 2, "add key value\t«atomic addition to 8-bit number»"}
	cmds["add16"] = &Cmd{cmdadd, 2, "add key value\t«atomic addition to 16-bit number»"}
	cmds["add32"] = &Cmd{cmdadd, 2, "add key value\t«atomic addition to 32-bit number»"}
	cmds["add64"] = &Cmd{cmdadd, 2, "add key value\t«atomic addition to 64-bit number»"}
	cmds["sub8"] = &Cmd{cmdsub, 2, "sub key value\t«atomic subtraction to 8-bit number»"}
	cmds["sub16"] = &Cmd{cmdsub, 2, "sub key value\t«atomic subtraction to 16-bit number»"}
	cmds["sub32"] = &Cmd{cmdsub, 2, "sub key value\t«atomic subtraction to 32-bit number»"}
	cmds["sub64"] = &Cmd{cmdsub, 2, "sub key value\t«atomic subtraction to 64-bit number»"}
	cmds["bitset"] = &Cmd{cmdbset, 1, "bitset key\t«atomic bit set»"}
	cmds["bitclr"] = &Cmd{cmdbclr, 1, "bitclr key\t«atomic bit clear»"}
	cmds["sappend"] = &Cmd{cmdsappend, 2, "sappend key value\t«atomically append the specified string to the value for the key»"}
	cmds["sremove"] = &Cmd{cmdsremove, 2, "sremove key value\t«atomically remove the specified string from the value of the key»"}
	cmds["ls"] = &Cmd{cmdls, 0, "ls [regexp]\t«list all keys that match the specified regular expresion (get #/keys:regexp)»"}
	cmds["help"] = &Cmd{cmdhelp, 0, "help [cmd]\t«print available commands or help on cmd»"}
	cmds["quit"] = &Cmd{cmdquit, 0, "quit\t«exit»"}
	cmds["exit"] = &Cmd{cmdquit, 0, "exit\t«quit»"}
}

func makevalue(args []string) []byte {
	if len(args) == 1 {
		s := args[0]
		if strings.HasPrefix(s, "\"") {
			if !strings.HasSuffix(s, "\"") {
				fmt.Fprintf(os.Stderr, "invalid value: %s\n", s)
				return nil
			}

			return []byte(s[1 : len(s)-1])
		} else {
			v := make([]byte, 8)
			if n, e := strconv.ParseUint(s, 0, 8); e == nil {
				hop.Pint8(uint8(n), v)
				v = v[0:1]
			} else if n, e := strconv.ParseUint(s, 0, 16); e == nil {
				hop.Pint16(uint16(n), v)
				v = v[0:2]
			} else if n, e := strconv.ParseUint(s, 0, 32); e == nil {
				hop.Pint32(uint32(n), v)
				v = v[0:4]
			} else if n, e := strconv.ParseUint(s, 0, 64); e == nil {
				hop.Pint64(uint64(n), v)
			} else {
				fmt.Fprintf(os.Stderr, "invalid value: %s\n", s)
				return nil
			}

			return v
		}
	}

	v := make([]byte, len(args))
	for i, s := range args {
		n, e := strconv.ParseUint(s, 0, 8)
		if e != nil {
			fmt.Fprintf(os.Stderr, "invalid value: %d: %v\n", n, e)
			return nil
		}

		v[i] = uint8(n)
	}

	return v
}

func cmdcreate(c hop.Hop, s []string) {
	var v []byte

	if len(s) > 3 {
		v = makevalue(s[3:])
		if v == nil {
			return
		}
	}

	version, err := c.Create(s[1], s[2], v)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
	} else {
		fmt.Printf("%d\n", version)
	}
}

func cmdremove(c hop.Hop, s []string) {
	err := c.Remove(s[1])
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
	}
}

func get(c hop.Hop, s []string) (uint64, []byte, error) {
	var e error
	var v []byte

	key := s[1]
	version := uint64(0)

	if len(s) > 2 {
		version, e = strconv.ParseUint(s[2], 0, 64)
		if e != nil {
			return 0, nil, e
		}
	}

	version, v, e = c.Get(key, version)
	return version, v, e
}

func cmdget(c hop.Hop, s []string) {
	version, val, err := get(c, s)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		return
	}

	fmt.Printf("%d: %s\n", version, barray(val))
}

func makenumber(val []byte) uint64 {
	n := uint64(0)

	switch len(val) {
	default:
		fmt.Fprintf(os.Stderr, "Error: cannot convert to number\n")
		return 0
	case 1:
		b, _ := hop.Gint8(val)
		n = uint64(b)
	case 2:
		b, _ := hop.Gint16(val)
		n = uint64(b)
	case 4:
		b, _ :=hop.Gint32(val)
		n = uint64(b)
	case 8:
		n, _ = hop.Gint64(val)
	}

	return n
}

func barray(v []byte) string {
	if v == nil {
		return "nil"
	}

	s := ""
	for _, n := range v {
		s += fmt.Sprintf("%02x ", n)
	}

	return s
}

func cmdgetn(c hop.Hop, s []string) {
	version, val, err := get(c, s)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		return
	}

	if val == nil {
		fmt.Printf("%d: nil\n", version)
		return
	}

	fmt.Printf("%d: %d\n", version, makenumber(val))
}

func cmdgets(c hop.Hop, s []string) {
	version, val, err := get(c, s)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		return
	}

	if val == nil {
		fmt.Printf("%d: nil\n", version)
		return
	}

	fmt.Printf("%d: \"%s\"\n", version, string(val))
}

func cmdset(c hop.Hop, s []string) {
	var val []byte

	if len(s) > 2 {
		val = makevalue(s[2:])
		if val == nil {
			return
		}
	}

	version, err := c.Set(s[1], val)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
	} else {
		fmt.Printf("%d\n", version)
	}
}

func cmdtas(c hop.Hop, s []string) {
	var nv, ov []string
	var newval, oldval []byte

	if len(s) > 3 {
		ov := s[3:]
		for i, ss := range ov {
			if ss == "|" {
				nv = ov[i+1:]
				ov = ov[0:i]
				break
			}
		}
	}

	if ov != nil {
		oldval = makevalue(ov)
		if oldval == nil {
			return
		}
	}

	if nv != nil {
		newval = makevalue(nv)
		if oldval == nil {
			return
		}
	}

	ver, e := strconv.ParseUint(s[2], 0, 64)
	if e != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", e)
		return
	}

	version, val, err := c.TestSet(s[1], ver, oldval, newval)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		return
	}

	fmt.Printf("%d: %s", version, barray(val))
}

func makenvalue(args []string) []byte {
	var n uint64
	var v []byte
	var e error

	switch args[0][len(args[0])-1] {
	case '8':
		if n, e = strconv.ParseUint(args[2], 0, 8); e == nil {
			v = make([]byte, 1)
			hop.Pint8(uint8(n), v)
		}
	case '6':
		if n, e = strconv.ParseUint(args[2], 0, 16); e == nil {
			v = make([]byte, 2)
			hop.Pint16(uint16(n), v)
		}
	case '2':
		if n, e = strconv.ParseUint(args[2], 0, 32); e == nil {
			v = make([]byte, 4)
			hop.Pint32(uint32(n), v)
		}
	case '4':
		if n, e = strconv.ParseUint(args[2], 0, 64); e == nil {
			v = make([]byte, 8)
			hop.Pint64(uint64(n), v)
		}
	}

	if e != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", e)
		return nil
	}

	return v
}

func cmdadd(c hop.Hop, s []string) {
	v := makenvalue(s)
	if v == nil {
		return
	}

	version, vals, err := c.Atomic(s[1], hop.Add, [][]byte{v})
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		return
	}

	fmt.Printf("%d: %d\n", version, makenumber(vals[0]))
}

func cmdsub(c hop.Hop, s []string) {
	v := makenvalue(s)
	if v == nil {
		return
	}

	version, vals, err := c.Atomic(s[1], hop.Sub, [][]byte{v})
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		return
	}

	fmt.Printf("%d: %d\n", version, makenumber(vals[0]))
}

func cmdbset(c hop.Hop, s []string) {
	version, vals, err := c.Atomic(s[1], hop.BitSet, nil)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		return
	}

	bitnum, _ := hop.Gint32(vals[1])
	fmt.Printf("%d: %d", version, bitnum)
}

func cmdbclr(c hop.Hop, s []string) {
	version, vals, err := c.Atomic(s[1], hop.BitClear, nil)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		return
	}

	bitnum, _ := hop.Gint32(vals[1])
	fmt.Printf("%d: %d", version, bitnum)
}

func cmdsappend(c hop.Hop, s []string) {
	v := makevalue(s[2:])
	if v == nil {
		return
	}

	version, vals, err := c.Atomic(s[1], hop.Append, [][]byte{v})
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		return
	}

	fmt.Printf("%d: %s", version, barray(vals[0]))
}

func cmdsremove(c hop.Hop, s []string) {
	v := makevalue(s[2:])
	if v == nil {
		return
	}

	version, vals, err := c.Atomic(s[1], hop.Remove, [][]byte{v})
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		return
	}

	fmt.Printf("%d: %s", version, barray(vals[0]))
}

func cmdls(c hop.Hop, s []string) {
	re := ".*"
	if len(s) > 1 {
		re = s[1]
	}

	_, value, err := c.Get("#/keys:"+re, 0)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		return
	}

	keys := bytes.Split(value, []byte{0})
	for _, k := range keys {
		fmt.Printf("%s\n", string(k))
	}
}

// Print available commands
func cmdhelp(c hop.Hop, s []string) {
	cmdstr := ""
	if len(s) > 1 {
		for _, h := range s[1:] {
			v, ok := cmds[h]
			if ok {
				cmdstr = cmdstr + v.help + "\n"
			} else {
				cmdstr = cmdstr + "unknown command: " + h + "\n"
			}
		}
	} else {
		cmdstr = "available commands: "
		for k := range cmds {
			cmdstr = cmdstr + " " + k
		}
		cmdstr = cmdstr + "\n"
	}
	fmt.Fprintf(os.Stdout, "%s", cmdstr)
}

func cmdquit(c hop.Hop, s []string) { os.Exit(0) }

func cmd(c hop.Hop, cmd string) {
	ncmd := strings.Fields(cmd)
	if len(ncmd) <= 0 {
		return
	}
	v, ok := cmds[ncmd[0]]
	if ok == false {
		fmt.Fprintf(os.Stderr, "unknown command: %s\n", ncmd[0])
		return
	}
	v.fun(c, ncmd)
	return
}

func interactive(c hop.Hop) {
	reader := bufio.NewReaderSize(os.Stdin, 8192)
	for {
		fmt.Print(*prompt)
		line, err := reader.ReadSlice('\n')
		if err != nil {
			break
		}
		str := strings.TrimSpace(string(line))
		// TODO: handle larger input lines by doubling buffer
		in := strings.Split(str, "\n")
		for i := range in {
			if len(in[i]) > 0 {
				cmd(c, in[i])
			}
		}
	}
}

func main() {
	flag.Parse()

	c, e := Connect()
	if e != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", e)
		os.Exit(1)
	}

	if flag.NArg() > 0 {
		flags := flag.Args()
		for _, uc := range flags {
			cmd(c, uc)
		}
	} else {
		interactive(c)
	}

	return
}
