#include <unistd.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/time.h>
#include <pthread.h>
#include <stdint.h>
#include <cassandra.h>

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

	CassStatement*	stmtcreate;
	CassStatement*	stmtremove;
	CassStatement*	stmtget;
	CassStatement*	stmtset;

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
uint64_t seed;			// seed for the random number generator
int threadnum;			// number of op threads
int sleepn;			// time to sleep before starting the tests

// cassandra stuff
char *cseed;
CassCluster *ccluster;
CassSession *cs;

// other stuff
int rvalsz;
char *rval;
int ops[100];

void usage()
{
	fprintf(stderr, "csbench -m vminlen -x vmaxlen -k keynum -N numop -S seed -t threadnum -T sleepsec -c maddr -s\n");
	exit(1);
}

static void printerr(char *op, CassFuture *cf)
{
	const char *msg;
	size_t mlen;

	cass_future_error_message(cf, &msg, &mlen);
	fprintf(stderr, "Error:%s: %.*s\n", op, (int) mlen, msg);
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
	t->val = (char *) malloc(vmaxlen);
	t->datasent = 0;
	t->datarecv = 0;
	t->reqnum = 0;
	t->errnum = 0;

	t->stmtcreate = cass_statement_new("INSERT INTO bench.tbl (key, val) VALUES (?, ?);", 2);
	t->stmtremove = cass_statement_new("DELETE FROM bench.tbl WHERE key = ?;", 1);
	t->stmtget = cass_statement_new("SELECT val FROM bench.tbl WHERE key = ?;", 1);
	t->stmtset = cass_statement_new("UPDATE bench.tbl SET val = ? where key = ?;", 2);

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
	CassFuture *cf;

	genkey(t);
	cass_statement_bind_string(t->stmtget, 0, t->key);
	cf = cass_session_execute(cs, t->stmtget);
	cass_future_wait(cf);
	if (cass_future_error_code(cf) == CASS_OK) {
		const CassResult* result;
		CassIterator *it;

		result = cass_future_get_result(cf);
		it = cass_iterator_from_result(result);
		if (cass_iterator_next(it)) {
			const CassRow *row;
			const cass_byte_t *val;

			row = cass_iterator_get_row(it);
			cass_value_get_bytes(cass_row_get_column(row, 0), &val, &n);
			t->datarecv += n;
		}
		cass_result_free(result);
		cass_iterator_free(it);
	} else {
		printerr("get", cf);
		t->errnum++;
	}

	cass_future_free(cf);
	t->reqnum++;
	t->datasent += strlen(t->key);
}

static void testset(Tdata *t)
{
	CassFuture *cf;

	genkey(t);
	genval(t);

//	fprintf(stderr, "testset '%s'\n", t->key);
	cass_statement_bind_string(t->stmtset, 1, t->key);
	cass_statement_bind_bytes(t->stmtset, 0, (const unsigned char *) t->val, t->vlen);
	cf = cass_session_execute(cs, t->stmtset);
	cass_future_wait(cf);
	if (cass_future_error_code(cf) != CASS_OK) {
//		printerr("set", cf);
		t->errnum++;
	}
	cass_future_free(cf);

	t->reqnum++;
	t->datasent += strlen(t->key) + t->vlen;
}

static void testcreate(Tdata *t)
{
	CassFuture *cf;

	genkey(t);
	genval(t);

//	printf("testcreate '%s'\n", t->key);
	cass_statement_bind_string(t->stmtcreate, 0, t->key);
	cass_statement_bind_bytes(t->stmtcreate, 1, (const unsigned char *) t->val, t->vlen);
	cf = cass_session_execute(cs, t->stmtcreate);
	cass_future_wait(cf);
	if (cass_future_error_code(cf) != CASS_OK) {
//		printerr("create", cf);
		t->errnum++;
	}
	cass_future_free(cf);

	t->reqnum++;
	t->datasent += strlen(t->key) + t->vlen;
}

static void testremove(Tdata *t)
{
	CassFuture *cf;

	genkey(t);
//	printf("testremove '%s'\n", t->key);

	cass_statement_bind_string(t->stmtremove, 0, t->key);
	cf = cass_session_execute(cs, t->stmtremove);
	cass_future_wait(cf);
	if (cass_future_error_code(cf) != CASS_OK) {
//		printerr("remove", cf);
		t->errnum++;
	}

	cass_future_free(cf);
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
	int i, c, setup;
	char *s;
	unsigned long long datasent, datarecv, reqnum, errnum, st, et;
	Tdata *tds;
	struct timeval stv, etv;
	void *p;
	CassFuture *cf;
	CassError rc;
	CassStatement *stmt;

	vminlen = 512;
	vmaxlen = 512*1024;
	keynum = 16*1024*1024;
	numop = 16*1024*1024;
	seed = 1;
	threadnum = 1;
	sleepn = 0;
	cseed = NULL;
	setup = 0;

	while ((c = getopt(argc, argv, "m:x:k:N:S:t:T:sc:")) != -1) {
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
			cseed = strdup(optarg);
			break;
		case 's':
			setup++;
			break;
		}
	}

	if (!cseed)
		usage();

	// initialize the Cassandra connection
	cass_log_set_level(CASS_LOG_DISABLED);
	rc = CASS_OK;
	CassCluster *ccluster = cass_cluster_new();
	cass_cluster_set_contact_points(ccluster, cseed);
	cass_cluster_set_num_threads_io(ccluster, threadnum + 1);
	cs = cass_session_new();
	cf = cass_session_connect(cs, ccluster);
	cass_future_wait(cf);
	rc = cass_future_error_code(cf);
	if (rc != CASS_OK)
		goto error;

	cass_future_free(cf);

	if (setup) {
		stmt = cass_statement_new("DROP KEYSPACE IF EXISTS bench;", 0);
		cf = cass_session_execute(cs, stmt);
		cass_future_wait(cf);
		rc = cass_future_error_code(cf);
		if (rc != CASS_OK)
			goto error;
		cass_future_free(cf);
		cass_statement_free(stmt);

		stmt = cass_statement_new("CREATE KEYSPACE bench WITH replication = { 'class': 'SimpleStrategy', 'replication_factor': '1' };", 0);
		cf = cass_session_execute(cs, stmt);
		cass_future_wait(cf);
		rc = cass_future_error_code(cf);
		if (rc != CASS_OK)
			goto error;
		cass_future_free(cf);
		cass_statement_free(stmt);

		stmt = cass_statement_new("CREATE TABLE bench.tbl (key text, val blob, PRIMARY KEY (key));", 0);
		cf = cass_session_execute(cs, stmt);
		cass_future_wait(cf);
		rc = cass_future_error_code(cf);
		if (rc != CASS_OK)
			goto error;
		cass_future_free(cf);
		cass_statement_free(stmt);
		exit(0);
	}
	
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

	return 0;

error:
	{
		const char *msg;
		size_t mlen;
		cass_future_error_message(cf, &msg, &mlen);
		fprintf(stderr, "Error: %.*s\n", (int) mlen, msg);
		return -1;
	}

}
