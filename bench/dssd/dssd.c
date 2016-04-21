#include <stdio.h>
#include <string.h>
#include <sys/time.h>
#include <stdlib.h>
#include <unistd.h>
#include <errno.h>
#include <pthread.h>
#include <flood.h>

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

// dssd flags
char *dbname;

// other stuff
int rvalsz;
char *rval;
int ops[100];
flood_ctx_t *ctx;
flood_obj_t *obj;

void usage()
{
	fprintf(stderr, "dssd -m vminlen -x vmaxlen -k keynum -N numop -S seed -t threadnum -T sleepsec -c dbname\n");
	exit(1);
}

void perr(void *a, const flood_err_t *b, const flood_iou_t *c)
{
}

static int tdinit(Tdata *t, int id)
{
	int n;

	n = id + seed;
	t->randstate[0] = n;
	t->randstate[1] = n>>16;
	t->randstate[2] = 0x330e;
	t->key = malloc(7);
	t->key[6] = '\0';
	t->vlen = 0;
	t->val = malloc(vmaxlen);
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
	size_t n;
//	uint32_t flags;
//	char *val;
	flood_key_t *fkey;
        flood_iou_t fiou;
	flood_err_t ferr;

	genkey(t);
//	printf("testget '%s'\n", t->key);
	fkey = flood_key_alloc_str(ctx, t->key);
	n = flood_lookup(obj, &fiou, perr, &ferr, fkey, t->val, vmaxlen);
	if (n == 0)
		t->datarecv += fiou.fi_key.uk_len;
	else if (n != ENOENT)
		t->errnum++;

	flood_key_free(fkey);

	t->reqnum++;
	t->datasent += strlen(t->key);
}

static void testset(Tdata *t)
{
	int err;
	flood_key_t *fkey;
        flood_iou_t fiou;
	flood_err_t ferr;

	genkey(t);
	genval(t);
//	printf("testset '%s' vlen %d\n", t->key, t->vlen);

	fkey = flood_key_alloc_str(ctx, t->key);
	err = flood_insert(obj, &fiou, perr, &ferr, FM_ANY, fkey, t->val, t->vlen);
	if (err != 0)
		t->errnum++;

	flood_key_free(fkey);
	t->datasent += strlen(t->key) + t->vlen;
	t->reqnum++;
}

static void testcreate(Tdata *t)
{
	testset(t);
}

static void testremove(Tdata *t)
{
	int err;
	flood_key_t *fkey;
        flood_iou_t fiou;
	flood_err_t ferr;

	genkey(t);
//	printf("testremove '%s'\n", t->key);
	fkey = flood_key_alloc_str(ctx, t->key);
	err = flood_delete(obj, &fiou, perr, &ferr, fkey);
	if (err != 0)
		t->errnum++;

	flood_key_free(fkey);

	t->reqnum++;
	t->datasent += strlen(t->key);
}

static void *testloop(void *a)
{
	int n, op;
	Tdata *t;

	t = a;
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
	char *s;
	unsigned long long datasent, datarecv, reqnum, errnum, st, et;
	Tdata *tds;
	struct timeval stv, etv;
	void *p;
	flood_iou_t fiou;
	flood_err_t ferr;

	vminlen = 512;
	vmaxlen = 512*1024;
	keynum = 16*1024*1024;
	numop = 16*1024*1024;
	seed = 1;
	threadnum = 1;
	sleepn = 0;

	while ((c = getopt(argc, argv, "m:x:k:N:S:t:T:s:c:")) != -1) {
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
		case 'c':
			dbname = strdup(optarg);
			break;
		}
	}

	ctx = flood_init(FL_VERSION, NULL, NULL);
	if (ctx == NULL) {
		fprintf(stderr, "Error connecting\n");
		exit(1);
	}

	i = flood_xopen(flood_root(ctx), &fiou, perr, &ferr, FM_OLD, FT_KV, 0, dbname, NULL);
        if (i != 0) {
		i = flood_xopen(flood_root(ctx), &fiou, perr, &ferr, FM_NEW, FT_KV, 0, dbname, NULL);
		if (i != 0) {
			fprintf(stderr, "Error opening: %d\n", ferr.fe_err);
        	        exit(1);
		}
	}
	obj = fiou.fi_obj.uo_obj;

	// initialize the global stuff
	rvalsz = vmaxlen*2;
	rval = malloc(rvalsz);
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
	tds = calloc(threadnum, sizeof(Tdata));
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
	flood_fini(ctx);

	return 0;
}
