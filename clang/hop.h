#include <stdint.h>

#ifndef HOP_H
#define HOP_H

typedef struct Hop Hop;
typedef struct HopError HopError;
typedef struct HopOps HopOps;
typedef struct HopRet HopRet;
typedef struct HopValue HopValue;

enum HopVersion {
	Any		= 0,			// any version
	Lowest		= 1,			// lowest entry version
	Highest		= 0x7FFFFFFFFFFFFFFEL,	// highest entry version
	Newest		= 0x7FFFFFFFFFFFFFFFL,	// newest value (don't use cached values)
	Removed		= 0x8000000000000000L,	// internal use
	PastNewest	= 0xFFFFFFFFFFFFFFFFL,	// wait until the entry is updated
};

enum HopAtomicOps {
	// Atomically add the specified value to the current value.
	// The current value and the specified one need to be the same length.
	// Supports byte array lengths of 1, 2, 4, and 8, assumes little-endian
	// order, and converts them to the appropriate unsigned integer.
	Add = 0,

	// Atomically subtracts the specified value from the current value.
	// Same requirements as AtomicAdd.
	Sub,

	// If the specified value is nil, atomically set/clear one bit in the
	// current value that was zero before. Returns two byte arrays: the
	// new value of the entry, and the 'address' of the bit set/cleared as
	// a 32-bit integer, represented as 4-byte array.
	BitSet,
	BitClear,

	// Atomically append the specified value to the end of the current value
	Append,

	// Atomically remove all matches of the specified value from the current
	// value. If there are no matches, the entry's value and version are
	// not modified
	Remove,

	// Atomically replace all matches of the first specified value with the
	// second specified value. If there are no matches, the entry's value
	// and version are not modified
	Replace,
};

struct HopValue {
	uint32_t	len;
	uint8_t*	data;
};

struct HopError {
	uint32_t	errnum;
	char*		error;
};

struct HopRet {
	uint64_t	ver;
	HopValue	val;
	HopError	err;
	uint16_t	valsnum;
	HopValue*	vals;
};

struct Hop {
	int (*create)(char* key, char* flags, HopValue *val, HopRet *ret);
	int (*remove)(char *key, HopRet *ret);
	int (*get)(char *key, uint64_t ver, HopRet *ret);
	int (*set)(char *key, HopValue *val, HopRet *ret);
	int (*testset)(char *key, uint64_t oldver, HopValue *oldval, HopValue *val, HopRet *ret);
	int (*atomic)(char *key, uint16_t op, uint16_t valsnum, HopValue *vals, HopRet *ret);
};

void hop_value_free(HopValue *val);
HopError *hop_error_new(uint32_t ecode, char *edescr, ...);
HopError *hop_error_clone(HopError *err);
void hop_error_free(HopError *);

#endif
