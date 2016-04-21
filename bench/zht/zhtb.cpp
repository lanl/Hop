#include <unistd.h>
#include <stdio.h>
#include <string.h>
#include <sys/time.h>
#include <pthread.h>
#include <stdlib.h>

#include "cpp_zhtclient.h"
#include "Util.h"
#include "Const.h"

using namespace iit::datasys::zht::dm;

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

// zht flags
char *zhtconf;
char *neighbors;

// other stuff
int rvalsz;
char *rval;
int ops[100];
ZHTClient *zht;

void usage()
{
	fprintf(stderr, "zhtb -m vminlen -x vmaxlen -k keynum -N numop -S seed -t threadnum -T sleepsec -z zht.conf -n neighbors.conf\n");
	exit(1);
}

static int tdinit(Tdata *t, int id)
{
	int n;

	n = id + seed;
	t->randstate[0] = n;
	t->randstate[1] = n>>16;
	t->randstate[2] = 0x330e;
	t->key = (char *) malloc(7);
	t->key[6] = '\0';
	t->vlen = 0;
	t->val = (char *) malloc(vmaxlen + 1);
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
	t->val[t->vlen] = '\0';
}

static void testget(Tdata *t)
{
	int rc;
	size_t n;
	uint32_t flags;
	char *val;

	genkey(t);
//	printf("testget '%s'\n", t->key);
	rc = zht->lookup(t->key, t->val);
	if (rc == Const::ZSI_REC_SUCC) {
		t->datarecv += strlen(t->val);
	} else
		t->errnum++;

	t->reqnum++;
	t->datasent += strlen(t->key);
}

static void testset(Tdata *t)
{
	int rc;

	genkey(t);
	genval(t);
//	printf("testset '%s' vlen %d\n", t->key, t->vlen);
	rc = zht->insert(t->key, t->val);
	t->datasent += strlen(t->key) + t->vlen;
	t->reqnum++;
	if (rc != Const::ZSI_REC_SUCC)
		t->errnum++;
}

static void testcreate(Tdata *t)
{
	testset(t);
}

static void testremove(Tdata *t)
{
	int rc;

	genkey(t);
//	printf("testremove '%s'\n", t->key);
	rc = zht->remove(t->key);
	if (rc != Const::ZSI_REC_SUCC)
		t->errnum++;

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

int main(int argc, char **argv)
{
	int i, c;
	char *s, buf[64];
	unsigned long long datasent, datarecv, reqnum, errnum, st, et;
	Tdata *tds;
	struct timeval stv, etv;
	void *p;
	char *zhtconf, *neighbors;

	vminlen = 512;
	vmaxlen = 512*1024;
	keynum = 16*1024*1024;
	numop = 16*1024*1024;
	seed = 1;
	threadnum = 1;
	sleepn = 0;

	while ((c = getopt(argc, argv, "m:x:k:N:S:t:T:s:z:n:")) != -1) {
		switch (c) {
		default:
			usage();
			break;
		case 'm':
			vminlen = strtoll(optarg, &s, 10);
			if (*s != '\0')
				usage();
			break;
		case 'x':
			vmaxlen = strtoll(optarg, &s, 10);
			if (*s != '\0')
				usage();
			break;
		case 'k':
			keynum = strtoll(optarg, &s, 10);
			if (*s != '\0')
				usage();
			break;
		case 'N':
			numop = strtol(optarg, &s, 10);
			if (*s != '\0')
				usage();
			break;
		case 'S':
			seed = strtoll(optarg, &s, 10);
			if (*s != '\0')
				usage();
			break;
		case 't':
			threadnum = strtol(optarg, &s, 10);
			if (*s != '\0')
				usage();
			break;
		case 'T':
			sleepn = strtol(optarg, &s, 10);
			if (*s != '\0')
				usage();
			break;
		case 'z':
			zhtconf = strdup(optarg);
			break;
		case 'n':
			neighbors = strdup(optarg);
			break;
		}
	}

	zht = new ZHTClient(zhtconf, neighbors);

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
		if (tdinit(&tds[i], i) < 0) {
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
	zht->teardown();

	return 0;
}
