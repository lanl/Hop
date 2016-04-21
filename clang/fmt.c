#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <pthread.h>
#include <errno.h>
#include "hop.h"
#include "rmt.h"

static int
dumpdata(FILE *f, uint8_t *data, int datalen)
{
	int i, n;

	i = 0;
	n = 0;
	while (i < datalen) {
		n += fprintf(f, "%02x", data[i]);
		if (i%4 == 3)
			n += fprintf(f, " ");
		if (i%64 == 63)
			n += fprintf(f, "\n");

		i++;
	}
//	n += fprintf(f, "\n");

	return n;
}

static int
printstr(FILE *f, char *buf)
{
	uint16_t n;
	
	buf = (char *) gint16((uint8_t *) buf, &n);
	return fprintf(f, "'%.*s'", n, buf);
}

static int
printval(FILE *f, uint8_t *buf)
{
	uint32_t n;

	buf = gint32(buf, &n);
	return dumpdata(f, buf, n<32?n:32);
}

static int
printop(FILE *f, uint16_t op)
{
	switch (op) {
	default:
		return fprintf(f, "%u", op);
	case Add:
		return fprintf(f, "add");
	case Sub:
		return fprintf(f, "sub");
	case BitSet:
		return fprintf(f, "bitset");
	case BitClear:
		return fprintf(f, "bitclear");
	case Append:
		return fprintf(f, "append");
	case Remove:
		return fprintf(f, "remove");
	case Replace:
		return fprintf(f, "replace");
	}
}

int
printhmsg(FILE *f, HopMsg *m)
{
	int i, ret, type, tag;

	if (!m)
		return fprintf(f, "NULL");

	type = m->type;
	tag = m->tag;
	ret = 0;
	switch (type) {
	default:
		ret += fprintf(f, "invalid message: %d", m->type);
		break;

	case Rerror:
		ret += fprintf(f, "Rerror tag %u ecode %d ename ", tag, m->ecode);
		ret += printstr(f, m->edescr);
		break;

	case Tget:
		ret += fprintf(f, "Tget tag %u key ", tag);
		ret += printstr(f, m->key);
		ret += fprintf(f, " version %llu", m->version);
		break;

	case Rget:
		ret += fprintf(f, "Rget tag %u version %llu datalen %u", tag, m->version, m->value.len);
		break;

	case Tset:
		ret += fprintf(f, "Tset tag %u key ", tag);
		ret += printstr(f, m->key);
		ret += fprintf(f, " datalen %u", m->value.len);
		break;

	case Rset:
		ret += fprintf(f, "Rset tag %u version %llu", tag, m->version);
		break;

	case Tcreate:
		ret += fprintf(f, "Tcreate tag %u key ", tag);
		ret += printstr(f, m->key);
		ret += fprintf(f, " flags ");
		ret += printstr(f, m->flags);
		break;

	case Rcreate:
		ret += fprintf(f, "Rcreate tag %u version %llu ", tag, m->version);
		break;

	case Tremove:
		ret += fprintf(f, "Tremove tag %u key ", tag);
		ret += printstr(f, m->key);
		break;

	case Rremove:
		ret += fprintf(f, "Rremove tag %u", tag);
		break;

	case Ttestset:
		ret += fprintf(f, "Ttestset tag %u key ", tag);
		ret += printstr(f, m->key);
		ret += fprintf(f, " oldlen %u version %llu datalen %d", m->oldval.len, m->version, m->value.len);
		break;

	case Rtestset:
		ret += fprintf(f, "Rtestset tag %u version %llu datalen %u", tag, m->version, m->value.len);
		break;

	case Tatomic:
		ret += fprintf(f, "Tatomic tag %u op ", tag);
		ret += printop(f, m->atmop);
		ret += fprintf(f, " key ");
		ret += printstr(f, m->key);
		ret += fprintf(f, " valslen [");
		for(i = 0; i < m->valsnum; i++) {
			ret += fprintf(f, " %u", m->vals[i].len);
		}
		ret += fprintf(f, "]");
		break;

	case Ratomic:
		ret += fprintf(f, "Ratomic tag %u valslen [", tag);
		for(i = 0; i < m->valsnum; i++) {
			ret += fprintf(f, " %u", m->vals[i].len);
		}
		ret += fprintf(f, "]");
		break;
	}

	return ret;
}
