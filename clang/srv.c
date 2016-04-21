#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <pthread.h>
#include <errno.h>
#include <assert.h>
#include "hop.h"
#include "rmt.h"

static void wthreadcreate(Vsrv *srv);
static void *wthreadproc(void *a);
static void processreq(Vreq *req);

Vsrv *
srvcreate(int nwthread)
{
	int i;
	Vsrv *srv;

	srv = calloc(1, sizeof(*srv));
	pthread_mutex_init(&srv->lock, NULL);
	pthread_cond_init(&srv->reqcond, NULL);
	for(i = 0; i < nwthread; i++)
		wthreadcreate(srv);

	return srv;
}

void
srvstart(Vsrv *srv)
{
	if (srv->start)
		(*srv->start)(srv);
}

void
srvaddconn(Vsrv *srv, Vconn *conn)
{
	pthread_mutex_lock(&srv->lock);
	conn->next = srv->conns;
	srv->conns = conn;
	pthread_mutex_unlock(&srv->lock);
}

int
srvdelconn(Vsrv *srv, Vconn *conn)
{
	Vconn *c, **pc;
	pthread_mutex_lock(&srv->lock);
	pc = &srv->conns;
	c = *pc;
	while (!c) {
		if (c == conn) {
			*pc = c->next;
			c->next = NULL;
			break;
		}

		pc = &c->next;
		c = *pc;
	}
	pthread_mutex_unlock(&srv->lock);

	return c != NULL;
}

void
srvinreq(Vsrv *srv, Vreq *req)
{
	pthread_mutex_lock(&srv->lock);
	req->prev = srv->reqlast;
	if (srv->reqlast)
		srv->reqlast->next = req;
	srv->reqlast = req;
	if (!srv->reqfirst)
		srv->reqfirst = req;
	pthread_cond_signal(&srv->reqcond);
	pthread_mutex_unlock(&srv->lock);
}

static void
wthreadcreate(Vsrv *srv)
{
	Vwthread *wt;

	wt = calloc(1, sizeof(*wt));
	wt->srv = srv;
	pthread_create(&wt->thread, NULL, wthreadproc, wt);
	pthread_mutex_lock(&srv->lock);
	wt->next = srv->wthreads;
	srv->wthreads = wt;
	pthread_mutex_unlock(&srv->lock);
}

static void *
wthreadproc(void *a)
{
	Vreq *req;
	Vsrv *srv;
	Vwthread *wt;

	pthread_detach(pthread_self());
	wt = a;
	srv = wt->srv;
	pthread_mutex_lock(&srv->lock);
	while (!wt->shutdown) {
		req = srv->reqfirst;
		if (!req) {
			pthread_cond_wait(&srv->reqcond, &srv->lock);
			continue;
		}

		/* take out of the req list */
		srv->reqfirst = req->next;
		if (req->next)
			req->next->prev = NULL;

		if (srv->reqlast == req)
			srv->reqlast = NULL;

		/* put in the workreq list */
		req->next = srv->workreqs;
		if (srv->workreqs)
			srv->workreqs->prev = req;

		srv->workreqs = req;

		pthread_mutex_unlock(&srv->lock);
		processreq(req);
		pthread_mutex_lock(&srv->lock);
	}

	pthread_mutex_unlock(&srv->lock);
	return NULL;
}

static void
processreq(Vreq *req)
{
	Vcall *vc;
	Vsrv *srv;

	vc = req->tc;
	srv = req->conn->srv;
	switch (vc->id) {
	default:
unsupported:
		respondreqerr(req, "unsupported message");
		break;

	case Vtping:
		if (srv->ping)
			(*srv->ping)(req);
		else
			goto unsupported;
		break;

	case Vthello:
		if (srv->hello)
			(*srv->hello)(req);
		else
			goto unsupported;
		break;

	case Vtgoodbye:
		conndestroy(req->conn);
		break;

	case Vtread:
		if (srv->read)
			(*srv->read)(req);
		else
			goto unsupported;
		break;

	case Vtwrite:
		if (srv->write)
			(*srv->write)(req);
		else
			goto unsupported;
		break;

	case Vtsync:
		if (srv->sync)
			(*srv->sync)(req);
		else
			goto unsupported;
		break;
	}
}

void
respondreq(Vreq *req, Vcall *rc)
{
	Vsrv *srv;
	Vconn *conn;

	req->rc = rc;
	settag(req->rc, req->tc->tag);
	conn = req->conn;
	srv = conn->srv;

	pthread_mutex_lock(&srv->lock);
	if (req->prev)
		req->prev->next = req->next;
	else
		srv->workreqs = req->next;

	if (req->next)
		req->next->prev = req->prev;
	pthread_mutex_unlock(&srv->lock);

	connoutreq(conn, req);
}

void
respondreqerr(Vreq *req, char *ename)
{
	Vcall *rc;

	rc = packrerror(ename);
	respondreq(req, rc);
}

Vcall *
packrerror(char *ename)
{
	int size;
	Vcall *vc;
	struct cbuf buffer;
	struct cbuf *bufp;

	bufp = &buffer;
	size = 2 + (ename?strlen(ename):0);	/* ename[s] */
	vc = vcpack(bufp, size, Vrerror);
	if (!vc)
		return NULL;

	buf_put_str(bufp, ename, &vc->ename);
	return vc;
}

Vcall *
packrhello(char *sid, uchar rcrypto, uchar rcodec)
{
	int size;
	Vcall *vc;
	struct cbuf buffer;
	struct cbuf *bufp;

	bufp = &buffer;
	size = 4;	/* rcrypto[1] rcodec[1] */
	if (sid)
		size += strlen(sid);

	vc = vcpack(bufp, size, Vrhello);
	if (!vc)
		return NULL;

	buf_put_str(bufp, sid, &vc->sid);
	buf_put_int8(bufp, rcrypto, &vc->rcrypto);
	buf_put_int8(bufp, rcodec, &vc->rcodec);

	return vc;
}

Vcall *
packrread(u16 count, uchar *data)
{
	int size;
	Vcall *vc;
	struct cbuf buffer;
	struct cbuf *bufp;

	bufp = &buffer;
	size = count;	/* data[] */
	vc = vcpack(bufp, size, Vrread);
	if (!vc)
		return NULL;

	vc->data = buf_alloc(bufp, count);
	memmove(vc->data, data, count);

	return vc;
}

Vcall *
packrwrite(uchar *score)
{
	int size;
	Vcall *vc;
	struct cbuf buffer;
	struct cbuf *bufp;

	bufp = &buffer;
	size = Vscoresize;	/* score[20] */
	vc = vcpack(bufp, size, Vrwrite);
	if (!vc)
		return NULL;

	buf_put_score(bufp, score, &vc->score);
	return vc;
}

Vcall *
packrsync(void)
{
	return vcempty(Vrsync);
}

Vcall *
packrping(void)
{
	return vcempty(Vrping);
}
