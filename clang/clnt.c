#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <pthread.h>
#include <errno.h>
#include <assert.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <sys/time.h>
#include <netdb.h>
#include <time.h>
#include "hop.h"
#include "rmt.h"

typedef struct Hcreq Hcreq;
typedef struct Hcpool Hcpool;
typedef struct Hcrpc Hcrpc;

struct Hcreq {
	HopClnt*	clnt;
	uint16_t	tag;
	HopMsg*		tc;
	HopMsg*		rc;
	HopError*	err;

	void		(*cb)(Hcreq *, void *);
	void*		cba;
	Hcreq*		next;
};

struct Hcpool {
	pthread_mutex_t	lock;
	pthread_cond_t	cond;
	uint32_t	maxid;
	int		msize;
	uint8_t*	map;
};

struct HopClnt {
	pthread_mutex_t	lock;
	pthread_cond_t	cond;
	int		fd;
	int		debuglevel;

	Hcpool*		tagpool;

	Hcreq*		unsentfirst;
	Hcreq*		unsentlast;
	Hcreq*		pendfirst;

	pthread_t	readproc;
	pthread_t	writeproc;
};

struct Hcrpc {
	pthread_mutex_t	lock;
	pthread_cond_t	cond;
	HopMsg*		tc;
	HopMsg*		rc;
	HopError*	err;
};

static Hcreq *reqalloc(HopClnt *);
static void reqfree(Hcreq *req);
static void hrpccb(Hcreq *req, void *cba);
static void *clntwproc(void *a);
static void *clntrproc(void *a);

static Hcpool *poolcreate(uint32_t maxid);
static void pooldestroy(Hcpool *p);
static uint32_t poolgetid(Hcpool *p);
static void poolputid(Hcpool *p, uint32_t id);

HopClnt *
hclntcreate(char *addr, int port, int debuglevel)
{
	int fd;
	HopClnt *clnt;
	struct sockaddr_in saddr;
	struct hostent *hostinfo;

/*
	struct addrinfo *addrs;

	snprintf(p, sizeof(p), "%d", port);
	if (getaddrinfo(addr, p, NULL, &addrs) < 0)
		return NULL;

	fd = socket(addrs->ai_family, addrs->ai_socktype, 0);
	if (fd < 0)
		return NULL;

	if (connect(fd, addrs->ai_addr, sizeof(*addrs->ai_addr)) < 0) {
		perror("connect");
		close(fd);
		return NULL;
	}
	freeaddrinfo(addrs);
*/

	fd = socket(PF_INET, SOCK_STREAM, 0);
	if (fd < 0) {
		return NULL;
	}

	hostinfo = gethostbyname(addr);
	if (!hostinfo) {
		return NULL;
	}

	saddr.sin_family = AF_INET;
	saddr.sin_port = htons(port);
	saddr.sin_addr = *(struct in_addr *) hostinfo->h_addr;

	if (connect(fd, (struct sockaddr *) &saddr, sizeof(saddr)) < 0) {
		return NULL;
	}

	clnt = malloc(sizeof(*clnt));
	pthread_mutex_init(&clnt->lock, NULL);
	pthread_cond_init(&clnt->cond, NULL);
	clnt->fd = fd;
	clnt->unsentfirst = NULL;
	clnt->unsentlast = NULL;
	clnt->pendfirst = NULL;
	clnt->writeproc = 0;
	clnt->readproc = 0;
	clnt->tagpool = poolcreate(255);
	pthread_create(&clnt->readproc, NULL, clntrproc, clnt);
	pthread_create(&clnt->writeproc, NULL, clntwproc, clnt);
	clnt->debuglevel = debuglevel;

	return clnt;
}

void
hclntdisconnect(HopClnt *clnt)
{
	void *v;
	pthread_t rproc, wproc;

	pthread_mutex_lock(&clnt->lock);
	if (clnt->fd >= 0) {
		shutdown(clnt->fd, 2);
		close(clnt->fd);
		clnt->fd = -1;
	}
	rproc = clnt->readproc;
	clnt->readproc = 0;
	wproc = clnt->writeproc;
	clnt->writeproc = 0;
	pthread_cond_broadcast(&clnt->cond);
	pthread_mutex_unlock(&clnt->lock);

	if (rproc)
		pthread_join(rproc, &v);

	if (wproc)
		pthread_join(wproc, &v);
}

void
hclntdestroy(HopClnt *clnt)
{
	pthread_mutex_lock(&clnt->lock);
	if (clnt->tagpool) {
		pooldestroy(clnt->tagpool);
		clnt->tagpool = NULL;
	}
	pthread_mutex_unlock(&clnt->lock);
	free(clnt);
}

static void *
clntrproc(void *a)
{
	int i, n, size, fd;
	HopClnt *clnt;
	HopMsg *hc, *hc1;
	HopError *err;
	Hcreq *req, *req1, *unsent, *pend, *preq;

	clnt = a;
	hc = msg_alloc();
	hc->size = hc->buflen;
	hc->pkt = hc->buf;
	n = 0;
	fd = clnt->fd;
	while ((i = read(fd, hc->pkt + n, hc->size - n)) > 0) {
//		if (i == 0)
//			continue;

		n += i;

again:
		if (n < 2)
			continue;

		size = (hc->pkt[1] | (hc->pkt[0]<<8)) + 2;
		if (hc->size < size) {
			hc->pkt = malloc(size);
			hc->size = size;
			memmove(hc->pkt, hc->buf, n);
		}

		if (n < size)
			continue;

		err = unpack(hc, hc->pkt, size);
		if (err) {
			fprintf(stderr, "Error %s:%d\n", err->error, err->errnum);
			close(fd);
			break;
		}

		if (clnt->debuglevel) {
			fprintf(stderr, "<<< ");
			printhmsg(stderr, hc);
			fprintf(stderr, "\n");
		}

		hc1 = msg_alloc();
		hc1->size = hc->buflen;
		hc1->pkt = hc->buf;
		if (n > size)
			memmove(hc1->pkt, hc->pkt + size, n - size);
		n -= size;

		pthread_mutex_lock(&clnt->lock);
//		printf("- tag %d %d\n", vc->tag, vc->size);
		for(preq = NULL, req = clnt->pendfirst; req != NULL; preq = req, req = req->next) {
			if (req->tag == hc->tag) {
				if (preq)
					preq->next = req->next;
				else
					clnt->pendfirst = req->next;

				pthread_mutex_unlock(&clnt->lock);
				req->rc = hc;
				(*req->cb)(req, req->cba);
				reqfree(req);
				break;
			}
		}

		pthread_mutex_unlock(&clnt->lock);
		if (!req) {
			fprintf(stderr, "unmatched response: ");
			printhmsg(stderr, hc);
			fprintf(stderr, "\n");
			err = hop_error_new(EINVAL, "unmatched response");
			close(fd);
			break;
		}

		hc = hc1;
		if (n > 0)
			goto again;
	}

	msg_free(hc);
	pthread_mutex_lock(&clnt->lock);
	unsent = clnt->unsentfirst;
	clnt->unsentfirst = NULL;
	clnt->unsentlast = NULL;
	pend = clnt->pendfirst;
	clnt->pendfirst = NULL;
	pthread_mutex_unlock(&clnt->lock);

	if (!err)
		err = hop_error_new(EPIPE, "closed");

	req = unsent;
	while (req) {
		req1 = req->next;
		req->err = hop_error_clone(err);
		(*req->cb)(req, req->cba);
		reqfree(req);
		req = req1;
	}

	req = pend;
	while (req) {
		req1 = req->next;
		req->err = hop_error_clone(err);
		(*req->cb)(req, req->cba);
		reqfree(req);
		req = req1;
	}

	hop_error_free(err);
	return NULL;
}

static void *
clntwproc(void *a)
{
	int i, n, sz;
	uint8_t *p;
	Hcreq *req;
	HopClnt *clnt;

	clnt = a;
	pthread_mutex_lock(&clnt->lock);
	while (clnt->fd >= 0) {
		req = clnt->unsentfirst;
		if (!req) {
			pthread_cond_wait(&clnt->cond, &clnt->lock);
			continue;
		}

		clnt->unsentfirst = req->next;
		if (!clnt->unsentfirst)
			clnt->unsentlast = NULL;

		req->next = clnt->pendfirst;
		clnt->pendfirst = req;
		if (clnt->fd < 0)
			break;

		pthread_mutex_unlock(&clnt->lock);

		if (clnt->debuglevel) {
			fprintf(stderr, "<<< ");
			printhmsg(stderr, req->tc);
			fprintf(stderr, "\n");
		}

		n = 0;
		sz = req->tc->size;
		p = req->tc->pkt;
		while (n < sz) {
			i = write(clnt->fd, p + n, sz - n);
//			printf("+ %p tag %d %d %d\n", req, req->tc->tag, n, req->tc->size);
			if (i <= 0)
				break;
			n += i;
		}
		pthread_mutex_lock(&clnt->lock);
		if (i < 0) {
			if (clnt->fd>=0) {
				shutdown(clnt->fd, 2);
				close(clnt->fd);
			}
			break;
		}
	}

	pthread_mutex_unlock(&clnt->lock);
	return NULL;
}

HopError *hrpcnb(HopClnt *clnt, HopMsg *tc, void (*cb)(Hcreq *, void *), void *cba)
{
	Hcreq *req;

	req = reqalloc(clnt);
	req->tc = tc;
	req->cb = cb;
	req->cba = cba;
	set_tag(tc, req->tag);

	pthread_mutex_lock(&clnt->lock);
	if (clnt->fd < 0) {
		pthread_mutex_unlock(&clnt->lock);
		reqfree(req);
		return hop_error_new(EPIPE, "no connection");
	}

	if (clnt->unsentlast)
		clnt->unsentlast->next = req;
	else
		clnt->unsentfirst = req;

	clnt->unsentlast = req;
	pthread_mutex_unlock(&clnt->lock);
	pthread_cond_signal(&clnt->cond);

	return 0;
}

static void hrpccb(Hcreq *req, void *cba)
{
	Hcrpc *r;

	r = cba;
	pthread_mutex_lock(&r->lock);
	r->rc = req->rc;
	r->err = req->err;
	pthread_mutex_unlock(&r->lock);
	pthread_cond_signal(&r->cond);
}

HopError *hrpc(HopClnt *clnt, HopMsg *tc, HopMsg **rc)
{
	Hcrpc r;

	if (rc)
		*rc = NULL;

	r.tc = tc;
	r.rc = NULL;
	r.err = NULL;
	pthread_mutex_init(&r.lock, NULL);
	pthread_cond_init(&r.cond, NULL);
	if (!hrpcnb(clnt, tc, hrpccb, &r)) {
		pthread_mutex_lock(&r.lock);
		while (!r.rc && !r.err) {
			pthread_cond_wait(&r.cond, &r.lock);
		}

		pthread_mutex_unlock(&r.lock);
	}

	if (rc)
		*rc = r.rc;
	else
		free(r.rc);

	return r.err;
}

static Hcreq *
reqalloc(HopClnt *clnt)
{
	Hcreq *req;

	req = calloc(1, sizeof(*req));
	req->clnt = clnt;
	req->tag = poolgetid(clnt->tagpool);

	return req;
}

static void
reqfree(Hcreq *req)
{
	poolputid(req->clnt->tagpool, req->tag);
	free(req);
}

static uint8_t m2id[] = {
	0, 1, 0, 2, 0, 1, 0, 3, 
	0, 1, 0, 2, 0, 1, 0, 4, 
	0, 1, 0, 2, 0, 1, 0, 3, 
	0, 1, 0, 2, 0, 1, 0, 5, 
	0, 1, 0, 2, 0, 1, 0, 3, 
	0, 1, 0, 2, 0, 1, 0, 4, 
	0, 1, 0, 2, 0, 1, 0, 3, 
	0, 1, 0, 2, 0, 1, 0, 6, 
	0, 1, 0, 2, 0, 1, 0, 3, 
	0, 1, 0, 2, 0, 1, 0, 4, 
	0, 1, 0, 2, 0, 1, 0, 3, 
	0, 1, 0, 2, 0, 1, 0, 5, 
	0, 1, 0, 2, 0, 1, 0, 3, 
	0, 1, 0, 2, 0, 1, 0, 4, 
	0, 1, 0, 2, 0, 1, 0, 3, 
	0, 1, 0, 2, 0, 1, 0, 7, 
	0, 1, 0, 2, 0, 1, 0, 3, 
	0, 1, 0, 2, 0, 1, 0, 4, 
	0, 1, 0, 2, 0, 1, 0, 3, 
	0, 1, 0, 2, 0, 1, 0, 5, 
	0, 1, 0, 2, 0, 1, 0, 3, 
	0, 1, 0, 2, 0, 1, 0, 4, 
	0, 1, 0, 2, 0, 1, 0, 3, 
	0, 1, 0, 2, 0, 1, 0, 6, 
	0, 1, 0, 2, 0, 1, 0, 3, 
	0, 1, 0, 2, 0, 1, 0, 4, 
	0, 1, 0, 2, 0, 1, 0, 3, 
	0, 1, 0, 2, 0, 1, 0, 5, 
	0, 1, 0, 2, 0, 1, 0, 3, 
	0, 1, 0, 2, 0, 1, 0, 4, 
	0, 1, 0, 2, 0, 1, 0, 3, 
	0, 1, 0, 2, 0, 1, 0, 0,
};

static Hcpool *
poolcreate(uint32_t maxid)
{
	Hcpool *p;

	p = malloc(sizeof(*p));
	p->maxid = maxid;
	pthread_mutex_init(&p->lock, NULL);
	pthread_cond_init(&p->cond, NULL);
	p->msize = 32;
	p->map = calloc(p->msize, 1);

	return p;
}

static void
pooldestroy(Hcpool *p)
{
	free(p->map);
	free(p);
}

static uint32_t
poolgetid(Hcpool *p)
{
	int i, n;
	uint32_t ret;
	uint8_t *pt;

	pthread_mutex_lock(&p->lock);
again:
	for(i = 0; i < p->msize; i++)
		if (p->map[i] != 0xFF)
			break;

	if (i>=p->msize && p->msize*8<p->maxid) {
		n = p->msize + 32;
		if (n*8 > p->maxid)
			n = p->maxid/8 + 1;

		pt = realloc(p->map, n);
		if (pt) {
			memset(pt + p->msize, 0, n - p->msize);
			p->map = pt;
			i = p->msize;
			p->msize = n;
		}
	}

	if (i >= p->msize) {
		pthread_cond_wait(&p->cond, &p->lock);
		goto again;
	}

	ret = m2id[p->map[i]];
	p->map[i] |= 1 << ret;
	ret += i * 8;
	pthread_mutex_unlock(&p->lock);

	return ret;
}

static void
poolputid(Hcpool *p, uint32_t id)
{
	pthread_mutex_lock(&p->lock);
	if (id < p->msize*8)
		p->map[id / 8] &= ~(1 << (id % 8));
	pthread_mutex_unlock(&p->lock);
	pthread_cond_broadcast(&p->cond);
}
