/* Copyright (c) 2009-2014 Stanford University
 *
 * Permission to use, copy, modify, and distribute this software for any
 * purpose with or without fee is hereby granted, provided that the above
 * copyright notice and this permission notice appear in all copies.
 *
 * THE SOFTWARE IS PROVIDED "AS IS" AND THE AUTHOR(S) DISCLAIM ALL WARRANTIES
 * WITH REGARD TO THIS SOFTWARE INCLUDING ALL IMPLIED WARRANTIES OF
 * MERCHANTABILITY AND FITNESS. IN NO EVENT SHALL AUTHORS BE LIABLE FOR
 * ANY SPECIAL, DIRECT, INDIRECT, OR CONSEQUENTIAL DAMAGES OR ANY DAMAGES
 * WHATSOEVER RESULTING FROM LOSS OF USE, DATA OR PROFITS, WHETHER IN AN
 * ACTION OF CONTRACT, NEGLIGENCE OR OTHER TORTIOUS ACTION, ARISING OUT OF
 * OR IN CONNECTION WITH THE USE OR PERFORMANCE OF THIS SOFTWARE.
 */

#include <stdio.h>
#include <string.h>
#include <getopt.h>
#include <assert.h>

#include "ClusterMetrics.h"
#include "Cycles.h"
#include "ShortMacros.h"
#include "Crc32C.h"
#include "ObjectFinder.h"
#include "OptionParser.h"
#include "RamCloud.h"
#include "Tub.h"

using namespace RAMCloud;

enum {
	Opget = 1,
	Opset = 2,
	Opcreate = 3,
	Opremove = 4,
};

// percent of operations, has to add up to 100
enum {
	Opgetnum = 60,
	Opsetnum = 30,
	Opcreatenum = 5,
	Opremovenum = 5,
};

typedef struct Tdata {
	pthread_t	tid;
	RamCloud*	rc;
	unsigned short	randstate[3];

	char*		key;		// current operation key
	int		vlen;		// current operation value length
	char*		val;		// current operation value

	// stats
	uint64_t	datasent;
	uint64_t	datarecv;
	int		reqnum;
	int		errnum;
} Tdata;

// benchmark flags
uint64_t vminlen;		// minimum value length
uint64_t vmaxlen;		// maximum value length
uint64_t keynum;		// maximum number of keys to create
int numop;			// total number of operations per thread
int64_t seed;			// seed for the random number generator
int threadnum;			// number of op threads
int sleepn;			// time to sleep before starting the tests

// other stuff
uint64_t tableId;
int rvalsz;
char *rval;
int ops[100];

static int tdinit(Tdata *t, int id, RamCloud *rc)
{
	int n;

	t->rc = rc;

	n = id + seed;
	t->randstate[0] = n;
	t->randstate[1] = n>>16;
	t->randstate[2] = 0x330e;
	t->key = (char*) malloc(7);
	t->key[6] = '\0';
	t->vlen = 0;
	t->val = (char*) malloc(vmaxlen);
	t->datasent = 0;
	t->datarecv = 0;
	t->reqnum = 0;
	t->errnum = 0;

	return 0;
}

static void genkey(Tdata *t)
{
	long n;

	n = nrand48(t->randstate) % keynum;
	t->key[0] = (n & 0x3f) + '0';
	t->key[1] = ((n >> 6) & 0x3f) + '0';
	t->key[2] = ((n >> 12) & 0x3f) + '0';
	t->key[3] = ((n >> 18) & 0x3f) + '0';
	t->key[4] = ((n >> 24) & 0x3f) + '0';
	t->key[5] = ((n >> 32) & 0x3f) + '0';
}

static void genval(Tdata *t)
{
	int start;

	t->vlen = (nrand48(t->randstate) % (vmaxlen - vminlen)) + vminlen;
	start = nrand48(t->randstate) % (rvalsz - t->vlen - 1);
	memmove(t->val, &rval[start], t->vlen);
}

static void testget(Tdata *t)
{
	Buffer val;

	genkey(t);
//	printf("testget '%s'\n", t->key);
	try {
		t->rc->read(tableId, t->key, strlen(t->key), &val);
		t->datarecv += val.size();
	} catch (ClientException &e) {
		t->errnum++;
	}

	t->reqnum++;
	t->datasent += strlen(t->key);
}

static void testset(Tdata *t)
{
	genkey(t);
	genval(t);
//	printf("testset '%s' vlen %d\n", t->key, t->vlen);

	try {
		t->rc->write(tableId, t->key, strlen(t->key), t->val, t->vlen);
		t->datasent += strlen(t->key) + t->vlen;
	} catch (ClientException &e) {
		t->errnum++;
	}

	t->reqnum++;
}

static void testcreate(Tdata *t)
{
	testset(t);
}

static void testremove(Tdata *t)
{
	genkey(t);
//	printf("testremove '%s'\n", t->key);
	try {
		t->rc->remove(tableId, t->key, strlen(t->key));
	} catch (ClientException &e) {
		t->errnum++;
	}

	t->reqnum++;
	t->datasent += strlen(t->key);
}

static void *testloop(void *a)
{
	int n, op;
	Tdata *t;

	t = (Tdata *) a;
	while (t->reqnum < numop) {
		n = nrand48(t->randstate) % 100;
		op = ops[n];
		switch (op) {
		case Opget:
			testget(t);
			break;

		case Opset:
			testset(t);
			break;

		case Opcreate:
			testcreate(t);
			break;

		case Opremove:
			testremove(t);
			break;
		}
	}

	return NULL;	
}

int
main(int argc, char *argv[])
try
{
	int i;
	unsigned long long datasent, datarecv, reqnum, errnum, st, et;
	Tdata *tds;
	struct timeval stv, etv;
	void *p;

	vminlen = 512;
	vmaxlen = 512*1024;
	keynum = 16*1024*1024;
	numop = 16*1024*1024;
	seed = 1;
	threadnum = 1;
	sleepn = 0;

	// need external context to set log levels with OptionParser
	Context context(false);

	OptionsDescription clientOptions("Client");
	clientOptions.add_options()

        ("vmin",
         ProgramOptions::value<uint64_t>(&vminlen),
         "Minimum value length.")
        ("vmax",
         ProgramOptions::value<uint64_t>(&vmaxlen),
         "Maximum value length.")
        ("knum",
         ProgramOptions::value<uint64_t>(&keynum),
         "Maximum number of keys to create.")
        ("N",
         ProgramOptions::value<int>(&numop),
         "Total number of operations per thread.")
        ("S",
         ProgramOptions::value<int64_t>(&seed),
         "Seed for the random number generator.")
        ("threadnum",
         ProgramOptions::value<int>(&threadnum),
         "Number of op threads.")
        ("T",
         ProgramOptions::value<int>(&sleepn),
         "Time to sleep before starting tests.");

	OptionParser optionParser(clientOptions, argc, argv);
	context.transportManager->setSessionTimeout(optionParser.options.getSessionTimeout());

	LOG(NOTICE, "client: Connecting to %s", optionParser.options.getCoordinatorLocator().c_str());

	string locator = optionParser.options.getExternalStorageLocator();
	if (locator.size() == 0) {
		locator = optionParser.options.getCoordinatorLocator();
	}

	RamCloud rc(&context, locator.c_str(), optionParser.options.getClusterName().c_str());
	rc.createTable("test");
	tableId = rc.getTableId("test");

	// initialize the global stuff
	rvalsz = vmaxlen*2;
	rval = (char *) malloc(rvalsz);
	srand(seed);
	for(i = 0; i < rvalsz; i++)
		rval[i] = rand();

	for(i = 0; i < Opgetnum; i++)
		ops[i] = Opget;

	for(; i < Opgetnum + Opsetnum; i++)
		ops[i] = Opset;

	for(; i < Opgetnum + Opsetnum + Opcreatenum; i++)
		ops[i] = Opcreate;

	for(; i < Opgetnum + Opsetnum + Opcreatenum + Opremovenum; i++)
		ops[i] = Opremove;

	// initialize the threads
	tds = (Tdata *) calloc(threadnum, sizeof(Tdata));
	for(i = 0; i < threadnum; i++) {
		RamCloud *trc = new RamCloud(&context, locator.c_str(), optionParser.options.getClusterName().c_str());

		if (tdinit(&tds[i], i, trc) < 0) {
			return -1;
		}
	}

	sleep(sleepn);
	gettimeofday(&stv, NULL);
	for(i = 0; i < threadnum; i++) {
		if (pthread_create(&tds[i].tid, NULL, testloop, &tds[i])) {
			fprintf(stderr, "Can't create thread\n");
			return -1;
		}
	}

	datasent = 0;
	datarecv = 0;
	reqnum = 0;
	errnum = 0;
	for(i = 0; i < threadnum; i++) {
		Tdata *td;

		td = &tds[i];
		pthread_join(td->tid, &p);
		datasent += td->datasent;
		datarecv += td->datarecv;
		reqnum += td->reqnum;
		errnum += td->errnum;
		delete td->rc;
	}
	gettimeofday(&etv, NULL);

	st = ((unsigned long long) stv.tv_sec) * 1000000 + stv.tv_usec/1000;
	et = ((unsigned long long) etv.tv_sec) * 1000000 + etv.tv_usec/1000;
	printf("Time: %lld us\n", et - st);
	printf("Data sent: %lld bytes\n", datasent);
	printf("Data received: %lld bytes\n", datarecv);
	printf("Number of requests: %lld\n", reqnum);
	printf("Number of errors: %lld\n", errnum);
	printf("\n\n");
	printf("Bandwidth: %.2f MB/s\n", ((double)(datasent+datarecv)*1000000.0)/((double)(et - st)*1024.0*1024.0));
	printf("Rate: %.2f requests/s\n", ((double)(reqnum)*1000000.0)/((double)(et - st)));
	printf("ReqSize: %.2f bytes\n", ((double)(datasent+datarecv)) / ((double)reqnum));

	rc.dropTable("test");

	return 0;
} catch (RAMCloud::ClientException& e) {
    fprintf(stderr, "RAMCloud exception: %s\n", e.str().c_str());
    return 1;
} catch (RAMCloud::Exception& e) {
    fprintf(stderr, "RAMCloud exception: %s\n", e.str().c_str());
    return 1;
}
