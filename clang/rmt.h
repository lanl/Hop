#include <pthread.h>
#include "hop.h"

enum MsgType {
	Rerror	= 100,
	Tget	= 101,
	Rget,
	Tset	= 103,
	Rset,
	Tcreate	= 105,
	Rcreate,
	Tremove	= 107,
	Rremove,
	Ttestset= 109,
	Rtestset,
	Tatomic	= 111,
	Ratomic,
	Tlast,
};

enum {
	NOTAG	= 0xFFFF,
};

typedef struct HopMsg HopMsg;
typedef struct HopConn HopConn;
typedef struct HopReq HopReq;
typedef struct HopClnt HopClnt;
typedef struct HopWthread HopWthread;
typedef void (*hop_msghandler)(HopMsg *);

struct HopMsg {
	uint16_t	type;	// type of the message
	uint16_t	tag;	// message tag

	char*		key;	// key
	uint64_t	version;// entry's version
	HopValue	value;	// entry's value
	HopValue	oldval;
	uint16_t	valsnum;
	HopValue*	vals;
	uint16_t	atmop;	// atomic set operation
	char*		flags;	// create flags
	char*		edescr;	// error description
	uint32_t	ecode;	// error code

	uint32_t	size;	// size of the message
	uint8_t*	pkt;	// raw packet

	uint8_t*	buf;	// preallocated buffer
	uint32_t	buflen;	// size of the buffer
};

struct HopReq {
	HopConn*	conn;
	HopMsg*		tc;
	HopMsg*		rc;
	HopError*	err;

	HopReq*		prev;
	HopReq*		next;
};

struct HopConn {
	pthread_mutex_t	lock;
	pthread_cond_t	cond;
	Hop*		srv;
	int		shutdown;
	int		fd;
	pthread_t	rthread;
	pthread_t	wthread;
	HopReq*		outreqs;
	HopConn*	prev;
	HopConn*	next;
};

struct HopWthread {
	Hop*		srv;
	int		shutdown;
	pthread_t	thread;

	HopWthread*	next;
};

struct HopRmt {
	int		debuglevel;
	Hop*		hop;

	// implementation specific
	pthread_mutex_t	lock;
	pthread_cond_t	reqcond;
	HopConn*	conns;
	HopWthread*	wthreads;
	HopReq*		reqfirst;
	HopReq*		reqlast;
	HopReq*		workreqs;
};

/* clnt.c */
HopClnt *hclntcreate(char *addr, int port, int debuglevel);
void hclntdisconnect(HopClnt *clnt);
void hclntdestroy(HopClnt *clnt);
HopError *hrpc(HopClnt *clnt, HopMsg *tc, HopMsg **rc);

/* conv.c */
HopMsg *msg_alloc();
void msg_free(HopMsg *);
void msg_reset(HopMsg *);
HopError  *unpack(HopMsg *m, uint8_t *data, int datalen);
void set_tag(HopMsg *m, uint16_t tag);
void pack_tget(HopMsg *m, char *key, uint64_t version);
void pack_tset(HopMsg *m, char *key, HopValue *val);
void pack_tcreate(HopMsg *m, char *key, char *flags, HopValue *val);
void pack_tremove(HopMsg *m, char *key);
void pack_ttestset(HopMsg *m, char *key, uint64_t version, HopValue *oldval, HopValue *val);
void pack_tatomic(HopMsg *m, char *key, uint16_t op, uint16_t nvals, HopValue *vals);
void pack_rerror(HopMsg *m, char *edescr, uint32_t ecode);
void pack_rget(HopMsg *m, uint64_t version, HopValue *val);
void pack_rset(HopMsg *m, uint64_t version, HopValue *val);
void pack_rcreate(HopMsg *m, uint64_t version, HopValue *val);
void pack_rremove(HopMsg *m, uint64_t version, HopValue *val);
void pack_rtestset(HopMsg *m, uint64_t version, HopValue *val);
void pack_ratomic(HopMsg *m, uint64_t version, uint16_t nvals, HopValue *vals);

/* fmt.c */
int printhmsg(FILE *f, HopMsg *m);


static inline uint8_t *pint8(uint8_t *data, uint8_t val)
{
	data[0] = val;
	return data + 1;
}

static inline uint8_t *pint16(uint8_t *data, uint16_t val)
{
	data[0] = val;
	data[1] = val >> 8;
	return data + 2;
}

static inline uint8_t *pint32(uint8_t *data, uint32_t val)
{
	data[0] = val;
	data[1] = val >> 8;
	data[2] = val >> 16;
	data[3] = val >> 24;
	return data + 4;
}

static inline uint8_t *pint64(uint8_t *data, uint64_t val)
{
	data[0] = val;
	data[1] = val >> 8;
	data[2] = val >> 16;
	data[3] = val >> 24;
	data[4] = val >> 32;
	data[5] = val >> 40;
	data[6] = val >> 48;
	data[7] = val >> 56;
	return data + 8;
}

static inline uint8_t *pstr(uint8_t *data, char *val)
{
	int n;

	if (!val)
		data = pint16(data, 0);
	else {
		n = strlen(val);
		data = pint16(data, n);
		memmove(data, val, n);
		data += n;
	}

	return data;
}

static inline uint8_t *pvalue(uint8_t *data, uint8_t *val, uint32_t vlen)
{
	if (!val || !vlen)
		data = pint16(data, 0);
	else {
		data = pint16(data, vlen);
		memmove(data, val, vlen);
		data += vlen;
	}

	return data;
}

static inline uint8_t *gint8(uint8_t *data, uint8_t *val)
{
	*val = data[0];
	return data + 1;
}

static inline uint8_t *gint16(uint8_t *data, uint16_t *val)
{
	*val = data[0] | (data[1]<<8);
	return data + 2;
}

static inline uint8_t *gint32(uint8_t *data, uint32_t *val)
{
	*val = data[0] | (data[1]<<8) | (data[2]<<16) | (data[3]<<24);
	return data + 4;
}

static inline uint8_t *gint64(uint8_t *data, uint64_t *val)
{
	*val = (uint64_t)data[0] | ((uint64_t)data[1]<<8) | ((uint64_t)data[2]<<16) | 
		((uint64_t)data[3]<<24) | ((uint64_t)data[4]<<32) | ((uint64_t)data[5]<<40) | 
		((uint64_t)data[6]<<48) | ((uint64_t)data[7]<<56);
	return data + 8;
}

static inline uint8_t *gstr(uint8_t *data, uint32_t msgsize, char **s)
{
	uint16_t n;

	data = gint16(data, &n);
	if (n) {
		memmove(data - 2, data, n);
		data[n-2] = '\0';
		*s = (char *) (data - 2);
	} else
		*s = NULL;

	return data + n;
}

static inline uint8_t *gvalue(uint8_t *data, uint32_t len, HopValue *val)
{
	data = gint32(data, &val->len);
	if (val->len > len - 4)
		return NULL;

	val->data = data;
	return data + val->len;
}
