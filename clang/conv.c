#include <stdlib.h>
#include <string.h>
#include <stdio.h>
#include <errno.h>
#include "hop.h"
#include "rmt.h"

// minimum size of a Hop message for a type
// all of them start with size[4] type[2] tag[2]
int min_msg_size[] = {
        14, /* Rerror code[4] error[s] */
        18, /* Tget key[s] version[8] */
        20, /* Rget version[8] value[n] */
        14, /* Tset key[s] value[n] */
        16, /* Rset version[8] */
        16, /* Tcreate key[s] flags[s] value[n] */
        16, /* Rcreate version[8] */
        10, /* Tremove key[s] */
        8,  /* Rremove */
        26, /* Ttestset key[s] oldval[n] oldversion[8] value[n] */
        20, /* Rtestset version[8] value[n] */
        14, /* Tatomic op[2] key[s] valnum[2] value[n] value[n] ... */
        10, /* Ratomic version[8] valnum[2] value[n] value[n] ... */
        18, /* Tgetnewer key[s] version[8] */
        20, /* Rgetnewer version[8] value[n] */
};


HopMsg *msg_alloc()
{
	HopMsg *m;

	m = malloc(sizeof(HopMsg) + 8192);
	m->buf = (uint8_t *) &m[1];
	m->buflen = 8192;

	return m;
}

void msg_free(HopMsg *m)
{
	msg_reset(m);
	free(m);
}

void msg_reset(HopMsg *m)
{
	if (m->pkt != m->buf)
		free(m->pkt);

	m->pkt = NULL;
	if (m->vals != &m->value) {
		free(m->vals);
	}

	m->vals = NULL;
}

HopError *unpack(HopMsg *m, uint8_t *data, int datalen)
{
	int i;
	uint32_t sz;
	uint8_t *p, *endp;

	if (datalen < 8)
		return hop_error_new(EINVAL, "buffer too short: %d", datalen);

	endp = data + datalen;
	m->pkt = data;
	p = gint32(data, &m->size);
	p = gint16(p, &m->type);
	p = gint16(p, &m->tag);

	if (datalen < m->size)
		return hop_error_new(EINVAL, "buffer too short: %d expected %d", datalen, m->size);

	if (m->type < Rerror || m->type >= Tlast)
		return hop_error_new(EINVAL, "invalid message type: %d", m->type);

	sz = min_msg_size[m->type - Rerror];
	if (m->size < sz)
		goto szerror;

	endp = m->pkt + m->size;
	switch (m->type) {
	default:
		return hop_error_new(EINVAL, "invalid message type");

	case Rerror:
		p = gint32(p, &m->ecode);
		p = gstr(p, endp - p, &m->edescr);
		break;

	case Tget:
		p = gstr(p, endp - p, &m->key);
		if (p)
			p = gint64(p, &m->version);
		break;

	case Rget:
		p = gint64(p, &m->version);
		p = gvalue(p, endp - p, &m->value);
		break;

	case Tset:
		p = gstr(p, endp - p, &m->key);
		if (p)
			p = gvalue(p, endp - p, &m->value);
		break;

	case Rset:
		p = gint64(p, &m->version);
		break;

	case Tcreate:
		p = gstr(p, endp - p, &m->key);
		if (!p)
			goto szerror;

		p = gstr(p, endp - p, &m->flags);
		if (p)
			p = gvalue(p, endp - p, &m->value);
		break;

	case Rcreate:
		p = gint64(p, &m->version);
		break;

	case Tremove:
		p = gstr(p, endp - p, &m->key);
		break;

	case Rremove:
		/* nothing */
		break;

	case Ttestset:
		p = gstr(p, endp - p, &m->key);
		if (!p || p+8 >= endp)
			goto szerror;

		p = gint64(p, &m->version);
		p = gvalue(p, endp - p, &m->oldval);
		if (p)
			p = gvalue(p, endp - p, &m->value);
		break;

	case Rtestset:
		p = gint64(p, &m->version);
		p = gvalue(p, endp - p, &m->value);
		break;

	case Tatomic:
		p = gint16(p, &m->atmop);
		p = gstr(p, endp - p, &m->key);
		if (!p || endp-p < 2)
			goto szerror;

		p = gint16(p, &m->valsnum);
		if (m->valsnum) {
			if (m->valsnum == 1) {
				m->vals = &m->value;
			} else {
				m->vals = malloc(m->valsnum * sizeof(HopValue));
			}

			for(i = 0; i < m->valsnum; i++) {
				p = gvalue(p, endp - p, &m->vals[i]);
				if (!p)
					goto szerror;
			}
		}
		break;

	case Ratomic:
		p = gint64(p, &m->version);
		p = gint16(p, &m->valsnum);
		if (m->valsnum) {
			if (m->valsnum == 1) {
				m->vals = &m->value;
			} else {
				m->vals = malloc(m->valsnum * sizeof(HopValue));
			}

			for(i = 0; i < m->valsnum; i++) {
				p = gvalue(p, endp - p, &m->vals[i]);
				if (!p)
					goto szerror;
			}
		}
		break;
	}

	if (!p) {
szerror:
		return hop_error_new(EINVAL, "invalid size");
	}

	return NULL;
}

uint8_t *pack_common(HopMsg *m, uint32_t size, uint16_t type)
{
	uint8_t *p;

	size += 4 + 2 + 2; /* size[4] type[2] tag[2] */
	if (m->buflen < size)
		p = malloc(size);
	else
		p = m->buf;

	m->size = size;
	m->type = type;
	m->tag = NOTAG;
	m->pkt = p;
	p = pint32(p, size);
	p = pint16(p, type);
	p = pint16(p, NOTAG);

	return p;
}

void set_tag(HopMsg *m, uint16_t tag)
{
	m->tag = tag;
	pint16(m->pkt + 6, tag);
}

void pack_tget(HopMsg *m, char *key, uint64_t version)
{
	uint32_t size;
	uint8_t *p;

	size = 2 + strlen(key) + 8; /* key[s] version[8] */
	p = pack_common(m, size, Tget);
	m->key = key;
	p = pstr(p, key);
	m->version = version;
	pint64(p, version);
}

void pack_tset(HopMsg *m, char *key, HopValue *val)
{
	uint32_t size;
	uint8_t *p;

	size = 2 + strlen(key) + 4 + val->len; /* key[s] value[n] */
	p = pack_common(m, size, Tset);
	m->key = key;
	p = pstr(p, key);
	m->value.len = val->len;
	m->value.data = p + 4;
	p = pvalue(p, val->data, val->len);
}

void pack_tcreate(HopMsg *m, char *key, char *flags, HopValue *val)
{
	uint32_t size;
	uint8_t *p;

	size = 2 + strlen(key) + 2 + strlen(flags) + 4 + val->len; /* key[s] flags[s] value[n] */
	p = pack_common(m, size, Tcreate);
	m->key = key;
	p = pstr(p, key);
	m->flags = flags;
	p = pstr(p, flags);
	m->value.len = val->len;
	m->value.data = p;
	p = pvalue(p, val->data, val->len);
}

void pack_tremove(HopMsg *m, char *key)
{
	uint32_t size;
	uint8_t *p;

	size = 2 + strlen(key); /* key[s] */
	p = pack_common(m, size, Tremove);
	m->key = key;
	p = pstr(p, key);
}

void pack_ttestset(HopMsg *m, char *key, uint64_t version, HopValue *oldval, HopValue *val)
{
	uint32_t size;
	uint8_t *p;

	size = 2 + strlen(key) + 8 + 4 + oldval->len + 4 + val->len; /* key[s] version[8] oldvalue[n] value[n] */
	p = pack_common(m, size, Ttestset);
	m->key = key;
	p = pstr(p, key);
	m->version = version;
	p = pint64(p, version);
	m->oldval.len = oldval->len;
	m->oldval.data = p + 4;
	p = pvalue(p, oldval->data, oldval->len);
	m->value.len = val->len;
	m->value.data = p + 4;
	p = pvalue(p, val->data, val->len);
}

void pack_tatomic(HopMsg *m, char *key, uint16_t op, uint16_t nvals, HopValue *vals)
{
	int i;
	uint32_t size;
	uint8_t *p;

	size = 2 + 2 + strlen(key) + 2; /* op[2] key[s] valnum[2] */
	for(i = 0; i < nvals; i++) {
		size += 4 + vals[i].len;
	}

	p = pack_common(m, size, Tatomic);
	m->atmop = op;
	p = pint16(p, op);
	m->key = key;
	p = pstr(p, key);
	m->valsnum = nvals;
	p = pint16(p, nvals);
	if (nvals) {
		if (nvals == 1)
			m->vals = &m->value;
		else
			m->vals = malloc(nvals * sizeof(HopValue));

		for(i = 0; i < nvals; i++) {
			m->vals[i].len = vals[i].len;
			m->vals[i].data = p;
			p = pvalue(p, vals[i].data, vals[i].len);
		}
	}
}

void pack_rerror(HopMsg *m, char *edescr, uint32_t ecode)
{
	uint32_t size;
	uint8_t *p;

	size = 4 + 2 + strlen(edescr); /* ecode[4] edescr[s] */
	p = pack_common(m, size, Rerror);
	m->ecode = ecode;
	p = pint32(p, ecode);
	m->edescr = edescr;
	p = pstr(p, edescr);
}

void pack_rget(HopMsg *m, uint64_t version, HopValue *val)
{
	uint32_t size;
	uint8_t *p;

	size = 8 + 4 + val->len; /* version[8] value[n] */
	p = pack_common(m, size, Rget);
	m->version = version;
	p = pint64(p, version);
	m->value.len = val->len;
	m->value.data = p + 4;
	p = pvalue(p, val->data, val->len);
}

void pack_rset(HopMsg *m, uint64_t version, HopValue *val)
{
	uint32_t size;
	uint8_t *p;

	size = 8; /* version[8] */
	p = pack_common(m, size, Rset);
	m->version = version;
	p = pint64(p, version);
}

void pack_rcreate(HopMsg *m, uint64_t version, HopValue *val)
{
	uint32_t size;
	uint8_t *p;

	size = 8; /* version[8] */
	p = pack_common(m, size, Rcreate);
	m->version = version;
	p = pint64(p, version);
}

void pack_rremove(HopMsg *m, uint64_t version, HopValue *val)
{
	pack_common(m, 0, Rremove);
}

void pack_rtestset(HopMsg *m, uint64_t version, HopValue *val)
{
	uint32_t size;
	uint8_t *p;

	size = 8 + 4 + val->len; /* version[8] */
	p = pack_common(m, size, Rtestset);
	m->version = version;
	p = pint64(p, version);
	m->value.len = val->len;
	m->value.data = p + 4;
	p = pvalue(p, val->data, val->len);
}

void pack_ratomic(HopMsg *m, uint64_t version, uint16_t nvals, HopValue *vals)
{
	int i;
	uint32_t size;
	uint8_t *p;

	size = 8 + 2; /* version[8] valnum[2] */
	for(i = 0; i < nvals; i++) {
		size += 4 + vals[i].len;
	}

	p = pack_common(m, size, Ratomic);
	m->version = version;
	p = pint64(p, version);
	m->valsnum = nvals;
	p = pint16(p, nvals);
	if (nvals) {
		if (nvals == 1)
			m->vals = &m->value;
		else
			m->vals = malloc(nvals * sizeof(HopValue));

		for(i = 0; i < nvals; i++) {
			m->vals[i].len = vals[i].len;
			m->vals[i].data = p;
			p = pvalue(p, vals[i].data, vals[i].len);
		}
	}
}
