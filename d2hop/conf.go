// Copyright 2013 The Hop Authors.  All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package d2hop

import (
	"errors"
	"fmt"
	"math"
	"sort"
	"strconv"
	"strings"
)

// This file contains data for parsing the content of a #/conf file
// Used by both the client and the servers

type Range struct {
	addr  string
	start uint32
	end   uint32
	conn  *Conn // connection to the server (not set by this code)
}

type RangeList []Range

type Conf struct {
	maddr    string    // master's address
	srvnum   int       // total number of servers
	srvaddrs []string  // servers we route to
	routes   RangeList // list of routes
}

// Parses the value of the #/conf file. The format of the file is:
// <master's-address> <number-of-servers>
// <server's-address> <start-keyhas>-<end-keyhash>[ <start-keyhash>-<end-keyhash> ...]
// ...
// <server's-address> <start-keyhas>-<end-keyhash>[ <start-keyhash>-<end-keyhash> ...]
//
func parseConf(confVal []byte) (c *Conf, err error) {
	c = new(Conf)
	confstr := string(confVal)
	lines := strings.Split(confstr, "\n")

	//	fmt.Printf("parseConf: %s\n", confstr)
	if len(lines) < 2 {
		return nil, errors.New("invalid conf")
	}

	// parse the first line (<master's-address> <number-of-servers>)
	c.maddr = lines[0]
	if n := strings.Index(lines[0], " "); n >= 0 {
		c.maddr = c.maddr[0:n]
		//		fmt.Printf("parseConf: first line %s n %d\n", lines[0], n)
		m, err := strconv.ParseUint(lines[0][n+1:], 0, 32)
		if err != nil {
			return nil, err
		}
		c.srvnum = int(m)
	}

	lines = lines[1:]
	c.srvaddrs = nil
	for _, s := range lines {
		if s == "" {
			continue
		}

		//		fmt.Printf("parseConf: line %s\n", s)
		sd := strings.Split(s, " ")
		if len(sd) < 2 {
			return nil, errors.New("invalid route description")
		}

		c.srvaddrs = append(c.srvaddrs, sd[0])
		c.routes, err = parseRanges(c.routes, sd[0], sd[1:])
		if err != nil {
			return nil, err
		}
	}

	// sort the routes list
	sort.Sort(c.routes)

	// check if the routes cover the whole space
	start := uint32(0)
	for _, r := range c.routes {
		if r.start != start {
			return nil, errors.New(fmt.Sprintf("uncovered range: %d to %d\n", start, r.start))
		}

		start = r.end
	}

	if start < math.MaxUint32 {
		return nil, errors.New(fmt.Sprintf("uncovered range: %d to %d\n", start, math.MaxUint32))
	}

	return c, nil
}

func parseRanges(kr RangeList, addr string, ranges []string) (RangeList, error) {
	for _, r := range ranges {
		start := r
		end := ""
		if n := strings.Index(start, ":"); n >= 0 {
			end = start[n+1:]
			start = start[0:n]
		} else {
			return nil, errors.New("invalid range")
		}

		s, err := strconv.ParseUint(start, 0, 32)
		if err != nil {
			return nil, errors.New(fmt.Sprintf("start range error: '%s': %v", start, err))
		}

		e, err := strconv.ParseUint(end, 0, 32)
		if err != nil {
			return nil, errors.New(fmt.Sprintf("end range error: '%s': %v", end, err))
		}

		kr = append(kr, Range{addr, uint32(s), uint32(e), nil})
	}

	return kr, nil
}

func (rl RangeList) Len() int {
	return len(rl)
}

func (rl RangeList) Swap(i, j int) {
	rl[i], rl[j] = rl[j], rl[i]
}

func (rl RangeList) Less(i, j int) bool {
	return rl[i].start < rl[j].start
}

func (rl RangeList) Search(hash uint32) *Range {
	n := sort.Search(len(rl), func(i int) bool { return rl[i].end >= hash })

	if n >= len(rl) {
		err := fmt.Sprintf("looking for hash %x in {\n", hash);
		for _, r := range rl {
			err = fmt.Sprintf("%s %s %x %x\n", err, r.addr, r.start, r.end)
		}
		err += "}\n"

		panic(err)
	}

	return &rl[n]
}
