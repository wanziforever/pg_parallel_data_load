/*-------------------------------------------------------------------------
 *
 * hashfunc.c
 *		Generic hashing functions, and hash functions for use in dynahash.c
 *		hashtables
 *
 *
 *-------------------------------------------------------------------------
 */

#include <stdint.h>
#include <stddef.h>
#include <stdlib.h>
#include <stdbool.h>
#include <stdio.h>
#include <string.h>
#include <limits.h>
#include <ctype.h>
#include <assert.h>
#include "hgint.h"
#include <inttypes.h>


#define Assert(p) assert(p)

typedef char *Pointer;

/* Get a bit mask of the bits set in non-uint32 aligned addresses */
#define UINT32_ALIGN_MASK (sizeof(uint32) - 1)

/* Rotate a uint32 value left by k bits - note multiple evaluation! */
#define rot(x,k) (((x)<<(k)) | ((x)>>(32-(k))))

#define UInt32GetDatum(X) ((Datum) (X))
#define PG_RETURN_DATUM(x)	 return (x)
#define PG_RETURN_INT32(x)	 return Int32GetDatum(x)
#define PG_RETURN_UINT32(x)  return UInt32GetDatum(x)
#define PG_RETURN_INT16(x)	 return Int16GetDatum(x)
#define PG_RETURN_UINT16(x)  return UInt16GetDatum(x)
#define PG_RETURN_CHAR(x)	 return CharGetDatum(x)
#define PG_RETURN_BOOL(x)	 return BoolGetDatum(x)
#define PG_RETURN_OID(x)	 return ObjectIdGetDatum(x)
#define PG_RETURN_POINTER(x) return PointerGetDatum(x)
#define PG_RETURN_CSTRING(x) return CStringGetDatum(x)
#define PG_RETURN_NAME(x)	 return NameGetDatum(x)
/* these macros hide the pass-by-reference-ness of the datatype: */
#define PG_RETURN_FLOAT4(x)  return Float4GetDatum(x)
#define PG_RETURN_FLOAT8(x)  return Float8GetDatum(x)
#define PG_RETURN_INT64(x)	 return Int64GetDatum(x)
#define PG_RETURN_UINT64(x)  return UInt64GetDatum(x)

#define DatumGetPointer(X) ((Pointer) (X))
  
#define UInt64GetDatum(X) ((Datum) (X))
#define PointerGetDatum(X) ((Datum) (X))
#define DatumGetUInt32(X) ((uint32) (X))
//#define DatumGetUInt64(X) (* ((uint64 *) DatumGetPointer(X)))
#define DatumGetUInt64(X) ((uint64) (X))

#define FLEXIBLE_ARRAY_MEMBER 0


/*----------
 * mix -- mix 3 32-bit values reversibly.
 *
 * This is reversible, so any information in (a,b,c) before mix() is
 * still in (a,b,c) after mix().
 *
 * If four pairs of (a,b,c) inputs are run through mix(), or through
 * mix() in reverse, there are at least 32 bits of the output that
 * are sometimes the same for one pair and different for another pair.
 * This was tested for:
 * * pairs that differed by one bit, by two bits, in any combination
 *	 of top bits of (a,b,c), or in any combination of bottom bits of
 *	 (a,b,c).
 * * "differ" is defined as +, -, ^, or ~^.  For + and -, I transformed
 *	 the output delta to a Gray code (a^(a>>1)) so a string of 1's (as
 *	 is commonly produced by subtraction) look like a single 1-bit
 *	 difference.
 * * the base values were pseudorandom, all zero but one bit set, or
 *	 all zero plus a counter that starts at zero.
 *
 * This does not achieve avalanche.  There are input bits of (a,b,c)
 * that fail to affect some output bits of (a,b,c), especially of a.  The
 * most thoroughly mixed value is c, but it doesn't really even achieve
 * avalanche in c.
 *
 * This allows some parallelism.  Read-after-writes are good at doubling
 * the number of bits affected, so the goal of mixing pulls in the opposite
 * direction from the goal of parallelism.  I did what I could.  Rotates
 * seem to cost as much as shifts on every machine I could lay my hands on,
 * and rotates are much kinder to the top and bottom bits, so I used rotates.
 *----------
 */
#define mix(a,b,c) \
{ \
  a -= c;  a ^= rot(c, 4);	c += b; \
  b -= a;  b ^= rot(a, 6);	a += c; \
  c -= b;  c ^= rot(b, 8);	b += a; \
  a -= c;  a ^= rot(c,16);	c += b; \
  b -= a;  b ^= rot(a,19);	a += c; \
  c -= b;  c ^= rot(b, 4);	b += a; \
}



#define Min(x, y)		((x) < (y) ? (x) : (y))

 /*----------
  * final -- final mixing of 3 32-bit values (a,b,c) into c
  *
  * Pairs of (a,b,c) values differing in only a few bits will usually
  * produce values of c that look totally different.  This was tested for
  * * pairs that differed by one bit, by two bits, in any combination
  *	 of top bits of (a,b,c), or in any combination of bottom bits of
  *	 (a,b,c).
  * * "differ" is defined as +, -, ^, or ~^.  For + and -, I transformed
  *	 the output delta to a Gray code (a^(a>>1)) so a string of 1's (as
  *	 is commonly produced by subtraction) look like a single 1-bit
  *	 difference.
  * * the base values were pseudorandom, all zero but one bit set, or
  *	 all zero plus a counter that starts at zero.
  *
  * The use of separate functions for mix() and final() allow for a
  * substantial performance increase since final() does not need to
  * do well in reverse, but is does need to affect all output bits.
  * mix(), on the other hand, does not need to affect all output
  * bits (affecting 32 bits is enough).  The original hash function had
  * a single mixing operation that had to satisfy both sets of requirements
  * and was slower as a result.
  *----------
  */
#define final(a,b,c) \
{ \
  c ^= b; c -= rot(b,14); \
  a ^= c; a -= rot(c,11); \
  b ^= a; b -= rot(a,25); \
  c ^= b; c -= rot(b,16); \
  a ^= c; a -= rot(c, 4); \
  b ^= a; b -= rot(a,14); \
  c ^= b; c -= rot(b,24); \
}


/*
 * These structs describe the header of a varlena object that may have been
 * TOASTed.  Generally, don't reference these structs directly, but use the
 * macros below.
 *
 * We use separate structs for the aligned and unaligned cases because the
 * compiler might otherwise think it could generate code that assumes
 * alignment while touching fields of a 1-byte-header varlena.
 */
typedef union
{
	struct						/* Normal varlena (4-byte length) */
	{
		uint32		va_header;
		char		va_data[FLEXIBLE_ARRAY_MEMBER];
	}			va_4byte;
	struct						/* Compressed-in-line format */
	{
		uint32		va_header;
		uint32		va_rawsize; /* Original data size (excludes header) */
		char		va_data[FLEXIBLE_ARRAY_MEMBER]; /* Compressed data */
	}			va_compressed;
} varattrib_4b;

typedef struct
{
	uint8		va_header;
	char		va_data[FLEXIBLE_ARRAY_MEMBER]; /* Data begins here */
} varattrib_1b;

/* TOAST pointers are a subset of varattrib_1b with an identifying tag byte */
typedef struct
{
	uint8		va_header;		/* Always 0x80 or 0x01 */
	uint8		va_tag;			/* Type of datum */
	char		va_data[FLEXIBLE_ARRAY_MEMBER]; /* Type-specific data */
} varattrib_1b_e;


#define VARATT_IS_4B(PTR) \
	((((varattrib_1b *) (PTR))->va_header & 0x01) == 0x00)
#define VARATT_IS_4B_U(PTR) \
	((((varattrib_1b *) (PTR))->va_header & 0x03) == 0x00)
#define VARATT_IS_4B_C(PTR) \
	((((varattrib_1b *) (PTR))->va_header & 0x03) == 0x02)
#define VARATT_IS_1B(PTR) \
	((((varattrib_1b *) (PTR))->va_header & 0x01) == 0x01)
#define VARATT_IS_1B_E(PTR) \
	((((varattrib_1b *) (PTR))->va_header) == 0x01)
#define VARATT_NOT_PAD_BYTE(PTR) \
	(*((uint8 *) (PTR)) != 0)
#define VARSIZE_4B(PTR) \
	((((varattrib_4b *) (PTR))->va_4byte.va_header >> 2) & 0x3FFFFFFF)
#define VARSIZE_1B(PTR) \
	((((varattrib_1b *) (PTR))->va_header >> 1) & 0x7F)
#define VARTAG_1B_E(PTR) \
	(((varattrib_1b_e *) (PTR))->va_tag)

#define SET_VARSIZE_4B(PTR,len) \
	(((varattrib_4b *) (PTR))->va_4byte.va_header = (((uint32) (len)) << 2))
#define SET_VARSIZE_4B_C(PTR,len) \
	(((varattrib_4b *) (PTR))->va_4byte.va_header = (((uint32) (len)) << 2) | 0x02)
#define SET_VARSIZE_1B(PTR,len) \
	(((varattrib_1b *) (PTR))->va_header = (((uint8) (len)) << 1) | 0x01)
#define SET_VARTAG_1B_E(PTR,tag) \
	(((varattrib_1b_e *) (PTR))->va_header = 0x01, \
	 ((varattrib_1b_e *) (PTR))->va_tag = (tag))

#define VARDATA_4B(PTR)		(((varattrib_4b *) (PTR))->va_4byte.va_data)
#define VARDATA_4B_C(PTR)	(((varattrib_4b *) (PTR))->va_compressed.va_data)
#define VARDATA_1B(PTR)		(((varattrib_1b *) (PTR))->va_data)
#define VARDATA_1B_E(PTR)	(((varattrib_1b_e *) (PTR))->va_data)

#define VARRAWSIZE_4B_C(PTR) \
	(((varattrib_4b *) (PTR))->va_compressed.va_rawsize)
#define VARDATA(PTR)						VARDATA_4B(PTR)
#define VARSIZE(PTR)						VARSIZE_4B(PTR)

#define VARSIZE_SHORT(PTR)					VARSIZE_1B(PTR)
#define VARDATA_SHORT(PTR)					VARDATA_1B(PTR)

  /*
   * hash_any() -- hash a variable-length key into a 32-bit value
   *		k		: the key (the unaligned variable-length array of bytes)
   *		len		: the length of the key, counting by bytes
   *
   * Returns a uint32 value.  Every bit of the key affects every bit of
   * the return value.  Every 1-bit and 2-bit delta achieves avalanche.
   * About 6*len+35 instructions. The best hash table sizes are powers
   * of 2.  There is no need to do mod a prime (mod is sooo slow!).
   * If you need less than 32 bits, use a bitmask.
   *
   * This procedure must never throw elog(ERROR); the ResourceOwner code
   * relies on this not to fail.
   *
   * Note: we could easily change this function to return a 64-bit hash value
   * by using the final values of both b and c.  b is perhaps a little less
   * well mixed than c, however.
   */
Datum
hash_any(register unsigned char* k, register int keylen)
{
	register uint32 a,
		b,
		c,
		len;

	/* Set up the internal state */
	len = keylen;
	a = b = c = 0x9e3779b9 + len + 3923095;

	/* If the source pointer is word-aligned, we use word-wide fetches */
	if (((uintptr_t)k & UINT32_ALIGN_MASK) == 0)
	{
		/* Code path for aligned source data */
		register uint32* ka = (uint32*)k;

		/* handle most of the key */
		while (len >= 12)
		{
			a += ka[0];
			b += ka[1];
			c += ka[2];
			mix(a, b, c);
			ka += 3;
			len -= 12;
		}

		/* handle the last 11 bytes */
		k = (unsigned char*)ka;
#ifdef WORDS_BIGENDIAN
		switch (len)
		{
		case 11:
			c += ((uint32)k[10] << 8);
			/* fall through */
		case 10:
			c += ((uint32)k[9] << 16);
			/* fall through */
		case 9:
			c += ((uint32)k[8] << 24);
			/* fall through */
		case 8:
			/* the lowest byte of c is reserved for the length */
			b += ka[1];
			a += ka[0];
			break;
		case 7:
			b += ((uint32)k[6] << 8);
			/* fall through */
		case 6:
			b += ((uint32)k[5] << 16);
			/* fall through */
		case 5:
			b += ((uint32)k[4] << 24);
			/* fall through */
		case 4:
			a += ka[0];
			break;
		case 3:
			a += ((uint32)k[2] << 8);
			/* fall through */
		case 2:
			a += ((uint32)k[1] << 16);
			/* fall through */
		case 1:
			a += ((uint32)k[0] << 24);
			/* case 0: nothing left to add */
		}
#else							/* !WORDS_BIGENDIAN */
		switch (len)
		{
		case 11:
			c += ((uint32)k[10] << 24);
			/* fall through */
		case 10:
			c += ((uint32)k[9] << 16);
			/* fall through */
		case 9:
			c += ((uint32)k[8] << 8);
			/* fall through */
		case 8:
			/* the lowest byte of c is reserved for the length */
			b += ka[1];
			a += ka[0];
			break;
		case 7:
			b += ((uint32)k[6] << 16);
			/* fall through */
		case 6:
			b += ((uint32)k[5] << 8);
			/* fall through */
		case 5:
			b += k[4];
			/* fall through */
		case 4:
			a += ka[0];
			break;
		case 3:
			a += ((uint32)k[2] << 16);
			/* fall through */
		case 2:
			a += ((uint32)k[1] << 8);
			/* fall through */
		case 1:
			a += k[0];
			/* case 0: nothing left to add */
		}
#endif							/* WORDS_BIGENDIAN */
	}
	else
	{
		/* Code path for non-aligned source data */

		/* handle most of the key */
		while (len >= 12)
		{
#ifdef WORDS_BIGENDIAN
			a += (k[3] + ((uint32)k[2] << 8) + ((uint32)k[1] << 16) + ((uint32)k[0] << 24));
			b += (k[7] + ((uint32)k[6] << 8) + ((uint32)k[5] << 16) + ((uint32)k[4] << 24));
			c += (k[11] + ((uint32)k[10] << 8) + ((uint32)k[9] << 16) + ((uint32)k[8] << 24));
#else							/* !WORDS_BIGENDIAN */
			a += (k[0] + ((uint32)k[1] << 8) + ((uint32)k[2] << 16) + ((uint32)k[3] << 24));
			b += (k[4] + ((uint32)k[5] << 8) + ((uint32)k[6] << 16) + ((uint32)k[7] << 24));
			c += (k[8] + ((uint32)k[9] << 8) + ((uint32)k[10] << 16) + ((uint32)k[11] << 24));
#endif							/* WORDS_BIGENDIAN */
			mix(a, b, c);
			k += 12;
			len -= 12;
		}

		/* handle the last 11 bytes */
#ifdef WORDS_BIGENDIAN
		switch (len)
		{
		case 11:
			c += ((uint32)k[10] << 8);
			/* fall through */
		case 10:
			c += ((uint32)k[9] << 16);
			/* fall through */
		case 9:
			c += ((uint32)k[8] << 24);
			/* fall through */
		case 8:
			/* the lowest byte of c is reserved for the length */
			b += k[7];
			/* fall through */
		case 7:
			b += ((uint32)k[6] << 8);
			/* fall through */
		case 6:
			b += ((uint32)k[5] << 16);
			/* fall through */
		case 5:
			b += ((uint32)k[4] << 24);
			/* fall through */
		case 4:
			a += k[3];
			/* fall through */
		case 3:
			a += ((uint32)k[2] << 8);
			/* fall through */
		case 2:
			a += ((uint32)k[1] << 16);
			/* fall through */
		case 1:
			a += ((uint32)k[0] << 24);
			/* case 0: nothing left to add */
		}
#else							/* !WORDS_BIGENDIAN */
		switch (len)
		{
		case 11:
			c += ((uint32)k[10] << 24);
			/* fall through */
		case 10:
			c += ((uint32)k[9] << 16);
			/* fall through */
		case 9:
			c += ((uint32)k[8] << 8);
			/* fall through */
		case 8:
			/* the lowest byte of c is reserved for the length */
			b += ((uint32)k[7] << 24);
			/* fall through */
		case 7:
			b += ((uint32)k[6] << 16);
			/* fall through */
		case 6:
			b += ((uint32)k[5] << 8);
			/* fall through */
		case 5:
			b += k[4];
			/* fall through */
		case 4:
			a += ((uint32)k[3] << 24);
			/* fall through */
		case 3:
			a += ((uint32)k[2] << 16);
			/* fall through */
		case 2:
			a += ((uint32)k[1] << 8);
			/* fall through */
		case 1:
			a += k[0];
			/* case 0: nothing left to add */
		}
#endif							/* WORDS_BIGENDIAN */
	}

	final(a, b, c);

	/* report the result */
	return UInt32GetDatum(c);
}

/*
 * hash_any_extended() -- hash into a 64-bit value, using an optional seed
 *		k		: the key (the unaligned variable-length array of bytes)
 *		len		: the length of the key, counting by bytes
 *		seed	: a 64-bit seed (0 means no seed)
 *
 * Returns a uint64 value.  Otherwise similar to hash_any.
 */
Datum
hash_any_extended(register unsigned char* k, register int keylen,
	uint64 seed)
{
	register uint32 a,
		b,
		c,
		len;

	/* Set up the internal state */
	len = keylen;
	a = b = c = 0x9e3779b9 + len + 3923095;

	/* If the seed is non-zero, use it to perturb the internal state. */
	if (seed != 0)
	{
		/*
		 * In essence, the seed is treated as part of the data being hashed,
		 * but for simplicity, we pretend that it's padded with four bytes of
		 * zeroes so that the seed constitutes a 12-byte chunk.
		 */
		a += (uint32)(seed >> 32);
		b += (uint32)seed;
		mix(a, b, c);
	}

	/* If the source pointer is word-aligned, we use word-wide fetches */
	if (((uintptr_t)k & UINT32_ALIGN_MASK) == 0)
	{
		/* Code path for aligned source data */
		register uint32* ka = (uint32*)k;

		/* handle most of the key */
		while (len >= 12)
		{
			a += ka[0];
			b += ka[1];
			c += ka[2];
			mix(a, b, c);
			ka += 3;
			len -= 12;
		}

		/* handle the last 11 bytes */
		k = (unsigned char*)ka;
#ifdef WORDS_BIGENDIAN
		switch (len)
		{
		case 11:
			c += ((uint32)k[10] << 8);
			/* fall through */
		case 10:
			c += ((uint32)k[9] << 16);
			/* fall through */
		case 9:
			c += ((uint32)k[8] << 24);
			/* fall through */
		case 8:
			/* the lowest byte of c is reserved for the length */
			b += ka[1];
			a += ka[0];
			break;
		case 7:
			b += ((uint32)k[6] << 8);
			/* fall through */
		case 6:
			b += ((uint32)k[5] << 16);
			/* fall through */
		case 5:
			b += ((uint32)k[4] << 24);
			/* fall through */
		case 4:
			a += ka[0];
			break;
		case 3:
			a += ((uint32)k[2] << 8);
			/* fall through */
		case 2:
			a += ((uint32)k[1] << 16);
			/* fall through */
		case 1:
			a += ((uint32)k[0] << 24);
			/* case 0: nothing left to add */
		}
#else							/* !WORDS_BIGENDIAN */
		switch (len)
		{
		case 11:
			c += ((uint32)k[10] << 24);
			/* fall through */
		case 10:
			c += ((uint32)k[9] << 16);
			/* fall through */
		case 9:
			c += ((uint32)k[8] << 8);
			/* fall through */
		case 8:
			/* the lowest byte of c is reserved for the length */
			b += ka[1];
			a += ka[0];
			break;
		case 7:
			b += ((uint32)k[6] << 16);
			/* fall through */
		case 6:
			b += ((uint32)k[5] << 8);
			/* fall through */
		case 5:
			b += k[4];
			/* fall through */
		case 4:
			a += ka[0];
			break;
		case 3:
			a += ((uint32)k[2] << 16);
			/* fall through */
		case 2:
			a += ((uint32)k[1] << 8);
			/* fall through */
		case 1:
			a += k[0];
			/* case 0: nothing left to add */
		}
#endif							/* WORDS_BIGENDIAN */
	}
	else
	{
		/* Code path for non-aligned source data */

		/* handle most of the key */
		while (len >= 12)
		{
#ifdef WORDS_BIGENDIAN
			a += (k[3] + ((uint32)k[2] << 8) + ((uint32)k[1] << 16) + ((uint32)k[0] << 24));
			b += (k[7] + ((uint32)k[6] << 8) + ((uint32)k[5] << 16) + ((uint32)k[4] << 24));
			c += (k[11] + ((uint32)k[10] << 8) + ((uint32)k[9] << 16) + ((uint32)k[8] << 24));
#else							/* !WORDS_BIGENDIAN */
			a += (k[0] + ((uint32)k[1] << 8) + ((uint32)k[2] << 16) + ((uint32)k[3] << 24));
			b += (k[4] + ((uint32)k[5] << 8) + ((uint32)k[6] << 16) + ((uint32)k[7] << 24));
			c += (k[8] + ((uint32)k[9] << 8) + ((uint32)k[10] << 16) + ((uint32)k[11] << 24));
#endif							/* WORDS_BIGENDIAN */
			mix(a, b, c);
			k += 12;
			len -= 12;
		}

		/* handle the last 11 bytes */
#ifdef WORDS_BIGENDIAN
		switch (len)
		{
		case 11:
			c += ((uint32)k[10] << 8);
			/* fall through */
		case 10:
			c += ((uint32)k[9] << 16);
			/* fall through */
		case 9:
			c += ((uint32)k[8] << 24);
			/* fall through */
		case 8:
			/* the lowest byte of c is reserved for the length */
			b += k[7];
			/* fall through */
		case 7:
			b += ((uint32)k[6] << 8);
			/* fall through */
		case 6:
			b += ((uint32)k[5] << 16);
			/* fall through */
		case 5:
			b += ((uint32)k[4] << 24);
			/* fall through */
		case 4:
			a += k[3];
			/* fall through */
		case 3:
			a += ((uint32)k[2] << 8);
			/* fall through */
		case 2:
			a += ((uint32)k[1] << 16);
			/* fall through */
		case 1:
			a += ((uint32)k[0] << 24);
			/* case 0: nothing left to add */
		}
#else							/* !WORDS_BIGENDIAN */
		switch (len)
		{
		case 11:
			c += ((uint32)k[10] << 24);
			/* fall through */
		case 10:
			c += ((uint32)k[9] << 16);
			/* fall through */
		case 9:
			c += ((uint32)k[8] << 8);
			/* fall through */
		case 8:
			/* the lowest byte of c is reserved for the length */
			b += ((uint32)k[7] << 24);
			/* fall through */
		case 7:
			b += ((uint32)k[6] << 16);
			/* fall through */
		case 6:
			b += ((uint32)k[5] << 8);
			/* fall through */
		case 5:
			b += k[4];
			/* fall through */
		case 4:
			a += ((uint32)k[3] << 24);
			/* fall through */
		case 3:
			a += ((uint32)k[2] << 16);
			/* fall through */
		case 2:
			a += ((uint32)k[1] << 8);
			/* fall through */
		case 1:
			a += k[0];
			/* case 0: nothing left to add */
		}
#endif							/* WORDS_BIGENDIAN */
	}

	final(a, b, c);

	/* report the result */
	PG_RETURN_UINT64(((uint64)b << 32) | c);
}

/*
 * hash_uint32() -- hash a 32-bit value to a 32-bit value
 *
 * This has the same result as
 *		hash_any(&k, sizeof(uint32))
 * but is faster and doesn't force the caller to store k into memory.
 */
Datum
hash_uint32(uint32 k)
{
	register uint32 a,
		b,
		c;

	a = b = c = 0x9e3779b9 + (uint32) sizeof(uint32) + 3923095;
	a += k;

	final(a, b, c);

	/* report the result */
	return UInt32GetDatum(c);
}

/*
 * hash_uint32_extended() -- hash a 32-bit value to a 64-bit value, with a seed
 *
 * Like hash_uint32, this is a convenience function.
 */
Datum
hash_uint32_extended(uint32 k, uint64 seed)
{
	register uint32 a,
		b,
		c;

	a = b = c = 0x9e3779b9 + (uint32) sizeof(uint32) + 3923095;

	if (seed != 0)
	{
		a += (uint32)(seed >> 32);
		b += (uint32)seed;
		mix(a, b, c);
	}

	a += k;

	final(a, b, c);

	/* report the result */
	PG_RETURN_UINT64(((uint64)b << 32) | c);
}

/*
 * string_hash: hash function for keys that are NUL-terminated strings.
 *
 * NOTE: this is the default hash function if none is specified.
 */
uint32
string_hash(void* key, Size keysize)
{
	/*
	 * If the string exceeds keysize-1 bytes, we want to hash only that many,
	 * because when it is copied into the hash table it will be truncated at
	 * that length.
	 */
	Size		s_len = strlen((char*)key);

	s_len = Min(s_len, keysize - 1);
	return DatumGetUInt32(hash_any((unsigned char*)key,
		(int)s_len));
}

/*
 * tag_hash: hash function for fixed-size tag values
 */
uint32
tag_hash(void* key, Size keysize)
{
	return DatumGetUInt32(hash_any((unsigned char*)key,
		(int)keysize));
}

/*
 * uint32_hash: hash function for keys that are uint32 or int32
 *
 * (tag_hash works for this case too, but is slower)
 */
uint32
uint32_hash(void* key, Size keysize)
{
	Assert(keysize == sizeof(uint32));
	return DatumGetUInt32(hash_uint32(*((uint32*)key)));
}


#define UINT64CONST(x) (x##UL)
uint64_t
hash_combine64(uint64_t a, uint64_t b)
{
	a ^= b + UINT64CONST(0x49a0f4dd15e5a8e3) + (a << 54) + (a >> 7);
	return a;
}

bool
compute_partition_hash_value(uint32_t k, uint64_t* hashcode)
{
	//uint64_t		seed = HASH_PARTITION_SEED;
	uint64_t seed = 0x7A5B22367996DCFDUL;

	hash_uint32_extended(k, seed);
	return true;
}

uint32_t strto32uint(char* st)
{
	char* x;
	for (x = st; *x; x++) {
		if (!isdigit(*x))
			return 0L;
	}
	return (strtoul(st, 0L, 10));
}

bool generate_hash_key(char* key, uint64_t* hashcode)
{
	uint32_t intkey = 0;
	uint64_t hash = 0;

	intkey = strto32uint(key);

	compute_partition_hash_value(intkey, &hash);
	*hashcode = hash_combine64(0, hash);
	return true;
}


int
get_matching_hash_bounds_int(int var, int greatest_modulus)
{
	uint64_t seed = 0x7A5B22367996DCFDUL;
	Datum hash;
	hash = hash_uint32_extended(var, seed);
	/* currently only support on hash column */
	uint64 rowHash = 0;
	rowHash = hash_combine64(0, hash);
	if (greatest_modulus == 0)
		greatest_modulus = 1;

	return rowHash % greatest_modulus;
}


int
get_matching_hash_bounds_string(char* var, int greatest_modulus)
{
	uint64_t seed = 0x7A5B22367996DCFDUL;
	Datum hash = 0;
	hash = hash_any_extended((unsigned char*)var, strlen(var), seed);
	uint64 rowHash = 0;
	rowHash = hash_combine64(0, hash);
	if (greatest_modulus == 0)
		greatest_modulus = 1;

	return rowHash % greatest_modulus;
}

int
get_matching_hash_bounds_bigint(int64 var, int greatest_modulus)
{
	uint64_t seed = 0x7A5B22367996DCFDUL;
	int64		val = var;
	uint32		lohalf = (uint32)val;
	uint32		hihalf = (uint32)(val >> 32);

	lohalf ^= (val >= 0) ? hihalf : ~hihalf;
	Datum hash = 0;
	hash = hash_uint32_extended(lohalf, seed);
	uint64 rowHash = 0;
	rowHash = hash_combine64(0, hash);
	if (greatest_modulus == 0)
		greatest_modulus = 1;

	return rowHash % greatest_modulus;

}

//int main(int argc, char *argv[])
//{
//  int testint = 4321;
//  int64 testbigint = 222222222222;
//  char testchar[128] = "dennytest";
//
//  int greatest_modulus = 3;
//
//  int result = 0;
//
//  result = get_matching_hash_bounds_int(testint, greatest_modulus);
//  printf("int hash test result is %d\n", result);
//
//  result = get_matching_hash_bounds_bigint(testbigint, greatest_modulus);
//  printf("bigint hash test result is %d\n", result);
//
//  result = get_matching_hash_bounds_string(testchar, greatest_modulus);
//  printf("string hash test result is %d\n", result);
//     
//  return 0;
//}

typedef int16 NumericDigit;
struct NumericShort
{
	uint16		n_header;		/* Sign + display scale + weight */
	NumericDigit n_data[FLEXIBLE_ARRAY_MEMBER]; /* Digits */
};

struct NumericLong
{
	uint16		n_sign_dscale;	/* Sign + display scale */
	int16		n_weight;		/* Weight of 1st digit	*/
	NumericDigit n_data[FLEXIBLE_ARRAY_MEMBER]; /* Digits */
};

union NumericChoice
{
	uint16		n_header;		/* Header word */
	struct NumericLong n_long;	/* Long form (4-byte header) */
	struct NumericShort n_short;	/* Short form (2-byte header) */
};

struct NumericData
{
	int32		vl_len_;		/* varlena header (do not touch directly!) */
	union NumericChoice choice; /* choice of format */
};

typedef struct NumericData *Numeric;

#define VARHDRSZ		((int32) sizeof(int32))

/*
 * Interpretation of high bits.
 */

#define NUMERIC_SIGN_MASK	0xC000
#define NUMERIC_POS			0x0000
#define NUMERIC_NEG			0x4000
#define NUMERIC_SHORT		0x8000
#define NUMERIC_NAN			0xC000

#define NUMERIC_FLAGBITS(n) ((n)->choice.n_header & NUMERIC_SIGN_MASK)
#define NUMERIC_IS_NAN(n)		(NUMERIC_FLAGBITS(n) == NUMERIC_NAN)
#define NUMERIC_IS_SHORT(n)		(NUMERIC_FLAGBITS(n) == NUMERIC_SHORT)

#define NUMERIC_HDRSZ	(VARHDRSZ + sizeof(uint16) + sizeof(int16))
#define NUMERIC_HDRSZ_SHORT (VARHDRSZ + sizeof(uint16))

/*
 * If the flag bits are NUMERIC_SHORT or NUMERIC_NAN, we want the short header;
 * otherwise, we want the long one.  Instead of testing against each value, we
 * can just look at the high bit, for a slight efficiency gain.
 */
#define NUMERIC_HEADER_IS_SHORT(n)	(((n)->choice.n_header & 0x8000) != 0)
#define NUMERIC_HEADER_SIZE(n) \
	(VARHDRSZ + sizeof(uint16) + \
	 (NUMERIC_HEADER_IS_SHORT(n) ? 0 : sizeof(int16)))

static const int round_powers[4] = {0, 1000, 100, 10};

/*
 * Short format definitions.
 */

#define NUMERIC_SHORT_SIGN_MASK			0x2000
#define NUMERIC_SHORT_DSCALE_MASK		0x1F80
#define NUMERIC_SHORT_DSCALE_SHIFT		7
#define NUMERIC_SHORT_DSCALE_MAX		\
	(NUMERIC_SHORT_DSCALE_MASK >> NUMERIC_SHORT_DSCALE_SHIFT)
#define NUMERIC_SHORT_WEIGHT_SIGN_MASK	0x0040
#define NUMERIC_SHORT_WEIGHT_MASK		0x003F
#define NUMERIC_SHORT_WEIGHT_MAX		NUMERIC_SHORT_WEIGHT_MASK
#define NUMERIC_SHORT_WEIGHT_MIN		(-(NUMERIC_SHORT_WEIGHT_MASK+1))

#define NBASE		10000
#define HALF_NBASE	5000
#define DEC_DIGITS	4			/* decimal digits per NBASE digit */
#define MUL_GUARD_DIGITS	2	/* these are measured in NBASE digits */
#define DIV_GUARD_DIGITS	4

typedef int16 NumericDigit;

#define DatumGetNumeric(X)		  ((Numeric) PG_DETOAST_DATUM(X))
#define DatumGetNumericCopy(X)	  ((Numeric) PG_DETOAST_DATUM_COPY(X))
#define NumericGetDatum(X)		  PointerGetDatum(X)
#define PG_GETARG_NUMERIC(n)	  DatumGetNumeric(PG_GETARG_DATUM(n))
#define PG_GETARG_NUMERIC_COPY(n) DatumGetNumericCopy(PG_GETARG_DATUM(n))
#define PG_RETURN_NUMERIC(x)	  return NumericGetDatum(x)

/*
 * Extract sign, display scale, weight.
 */

#define NUMERIC_DSCALE_MASK			0x3FFF

#define NUMERIC_SIGN(n) \
	(NUMERIC_IS_SHORT(n) ? \
		(((n)->choice.n_short.n_header & NUMERIC_SHORT_SIGN_MASK) ? \
		NUMERIC_NEG : NUMERIC_POS) : NUMERIC_FLAGBITS(n))
#define NUMERIC_DSCALE(n)	(NUMERIC_HEADER_IS_SHORT((n)) ? \
	((n)->choice.n_short.n_header & NUMERIC_SHORT_DSCALE_MASK) \
		>> NUMERIC_SHORT_DSCALE_SHIFT \
	: ((n)->choice.n_long.n_sign_dscale & NUMERIC_DSCALE_MASK))
#define NUMERIC_WEIGHT(n)	(NUMERIC_HEADER_IS_SHORT((n)) ? \
	(((n)->choice.n_short.n_header & NUMERIC_SHORT_WEIGHT_SIGN_MASK ? \
		~NUMERIC_SHORT_WEIGHT_MASK : 0) \
	 | ((n)->choice.n_short.n_header & NUMERIC_SHORT_WEIGHT_MASK)) \
	: ((n)->choice.n_long.n_weight))


typedef struct NumericVar
{
	int			ndigits;		/* # of digits in digits[] - can be 0! */
	int			weight;			/* weight of first digit */
	int			sign;			/* NUMERIC_POS, NUMERIC_NEG, or NUMERIC_NAN */
	int			dscale;			/* display scale */
	NumericDigit *buf;			/* start of palloc'd space for digits[] */
	NumericDigit *digits;		/* base-NBASE digits */
} NumericVar;

#define MEMSET_LOOP_LIMIT 1024
#define LONG_ALIGN_MASK (sizeof(long) - 1)

#define MemSetAligned(start, val, len) \
	do \
	{ \
		long   *_start = (long *) (start); \
		int		_val = (val); \
		Size	_len = (len); \
\
		if ((_len & LONG_ALIGN_MASK) == 0 && \
			_val == 0 && \
			_len <= MEMSET_LOOP_LIMIT && \
			MEMSET_LOOP_LIMIT != 0) \
		{ \
			long *_stop = (long *) ((char *) _start + _len); \
			while (_start < _stop) \
				*_start++ = 0; \
		} \
		else \
			memset(_start, _val, _len); \
	} while (0)


#define init_var(v)		MemSetAligned(v, 0, sizeof(NumericVar))

#define NUMERIC_DIGITS(num) (NUMERIC_HEADER_IS_SHORT(num) ? \
	(num)->choice.n_short.n_data : (num)->choice.n_long.n_data)
#define NUMERIC_NDIGITS(num) \
	((VARSIZE(num) - NUMERIC_HEADER_SIZE(num)) / sizeof(NumericDigit))
#define NUMERIC_CAN_BE_SHORT(scale,weight) \
	((scale) <= NUMERIC_SHORT_DSCALE_MAX && \
	(weight) <= NUMERIC_SHORT_WEIGHT_MAX && \
   (weight) >= NUMERIC_SHORT_WEIGHT_MIN)

#define SET_VARSIZE(PTR, len)				SET_VARSIZE_4B(PTR, len)
#define SET_VARSIZE_SHORT(PTR, len)			SET_VARSIZE_1B(PTR, len)
#define SET_VARSIZE_COMPRESSED(PTR, len)	SET_VARSIZE_4B_C(PTR, len)

/*
 * dump_numeric() - Dump a value in the db storage format for debugging
 */
static void
dump_numeric(const char *str, Numeric num)
{
	NumericDigit *digits = NUMERIC_DIGITS(num);
	int			ndigits;
	int			i;

	ndigits = NUMERIC_NDIGITS(num);

	printf("%s: NUMERIC w=%d d=%d ", str,
		   NUMERIC_WEIGHT(num), NUMERIC_DSCALE(num));
	switch (NUMERIC_SIGN(num))
	{
		case NUMERIC_POS:
			printf("POS");
			break;
		case NUMERIC_NEG:
			printf("NEG");
			break;
		case NUMERIC_NAN:
			printf("NaN");
			break;
		default:
			printf("SIGN=0x%x", NUMERIC_SIGN(num));
			break;
	}

	for (i = 0; i < ndigits; i++)
		printf(" %0*d", DEC_DIGITS, digits[i]);
	printf("\n");
}

/*
 * make_result_opt_error() -
 *
 *	Create the packed db numeric format in palloc()'d memory from
 *	a variable.  If "*have_error" flag is provided, on error it's set to
 *	true, NULL returned.  This is helpful when caller need to handle errors
 *	by itself.
 */
Numeric
make_result_opt_error(const NumericVar *var, bool *have_error)
{
	Numeric		result;
	NumericDigit *digits = var->digits;
	int			weight = var->weight;
	int			sign = var->sign;
	int			n;
	Size		len;

	if (sign == NUMERIC_NAN)
	{
		result = (Numeric) malloc(NUMERIC_HDRSZ_SHORT);

		SET_VARSIZE(result, NUMERIC_HDRSZ_SHORT);
		result->choice.n_header = NUMERIC_NAN;
		/* the header word is all we need */

		dump_numeric("make_result()", result);
		return result;
	}

	n = var->ndigits;

	/* truncate leading zeroes */
	while (n > 0 && *digits == 0)
	{
		digits++;
		weight--;
		n--;
	}
	/* truncate trailing zeroes */
	while (n > 0 && digits[n - 1] == 0)
		n--;

	/* If zero result, force to weight=0 and positive sign */
	if (n == 0)
	{
		weight = 0;
		sign = NUMERIC_POS;
	}

	/* Build the result */
	if (NUMERIC_CAN_BE_SHORT(var->dscale, weight))
	{
		len = NUMERIC_HDRSZ_SHORT + n * sizeof(NumericDigit);
		result = (Numeric) malloc(len);
		SET_VARSIZE(result, len);
		result->choice.n_short.n_header =
			(sign == NUMERIC_NEG ? (NUMERIC_SHORT | NUMERIC_SHORT_SIGN_MASK)
			 : NUMERIC_SHORT)
			| (var->dscale << NUMERIC_SHORT_DSCALE_SHIFT)
			| (weight < 0 ? NUMERIC_SHORT_WEIGHT_SIGN_MASK : 0)
			| (weight & NUMERIC_SHORT_WEIGHT_MASK);
	}
	else
	{
		len = NUMERIC_HDRSZ + n * sizeof(NumericDigit);
		result = (Numeric) malloc(len);
		SET_VARSIZE(result, len);
		result->choice.n_long.n_sign_dscale =
			sign | (var->dscale & NUMERIC_DSCALE_MASK);
		result->choice.n_long.n_weight = weight;
	}

	assert(NUMERIC_NDIGITS(result) == n);
	if (n > 0)
		memcpy(NUMERIC_DIGITS(result), digits, n * sizeof(NumericDigit));

	/* Check for overflow of int16 fields */
	if (NUMERIC_WEIGHT(result) != weight ||
		NUMERIC_DSCALE(result) != var->dscale)
	{
		if (have_error)
		{
			*have_error = true;
			return NULL;
		}
		else
		{
      fprintf(stderr, "value overflows numeric format\n");
		}
	}

	dump_numeric("make_result()", result);
	return result;
}


/*
 * make_result() -
 *
 *	An interface to make_result_opt_error() without "have_error" argument.
 */
static Numeric
make_result(const NumericVar *var)
{
	return make_result_opt_error(var, NULL);
}

#define digitbuf_alloc(ndigits)  \
	((NumericDigit *) malloc((ndigits) * sizeof(NumericDigit)))
#define digitbuf_free(buf)	\
	do { \
		 if ((buf) != NULL) \
			 free(buf); \
	} while (0)

/*
 * alloc_var() -
 *
 *	Allocate a digit buffer of ndigits digits (plus a spare digit for rounding)
 */
static void
alloc_var(NumericVar *var, int ndigits)
{
	digitbuf_free(var->buf);
	var->buf = digitbuf_alloc(ndigits + 1);
	var->buf[0] = 0;			/* spare digit for rounding */
	var->digits = var->buf + 1;
	var->ndigits = ndigits;
}


/*
 * free_var() -
 *
 *	Return the digit buffer of a variable to the free pool
 */
static void
free_var(NumericVar *var)
{
	digitbuf_free(var->buf);
	var->buf = NULL;
	var->digits = NULL;
	var->sign = NUMERIC_NAN;
}



static void
strip_var(NumericVar *var)
{
	NumericDigit *digits = var->digits;
	int			ndigits = var->ndigits;

	/* Strip leading zeroes */
	while (ndigits > 0 && *digits == 0)
	{
		digits++;
		var->weight--;
		ndigits--;
	}

	/* Strip trailing zeroes */
	while (ndigits > 0 && digits[ndigits - 1] == 0)
		ndigits--;

	/* If it's zero, normalize the sign and weight */
	if (ndigits == 0)
	{
		var->sign = NUMERIC_POS;
		var->weight = 0;
	}

	var->digits = digits;
	var->ndigits = ndigits;
}

/*
 * round_var
 *
 * Round the value of a variable to no more than rscale decimal digits
 * after the decimal point.  NOTE: we allow rscale < 0 here, implying
 * rounding before the decimal point.
 */
static void
round_var(NumericVar *var, int rscale)
{
	NumericDigit *digits = var->digits;
	int			di;
	int			ndigits;
	int			carry;

	var->dscale = rscale;

	/* decimal digits wanted */
	di = (var->weight + 1) * DEC_DIGITS + rscale;

	/*
	 * If di = 0, the value loses all digits, but could round up to 1 if its
	 * first extra digit is >= 5.  If di < 0 the result must be 0.
	 */
	if (di < 0)
	{
		var->ndigits = 0;
		var->weight = 0;
		var->sign = NUMERIC_POS;
	}
	else
	{
		/* NBASE digits wanted */
		ndigits = (di + DEC_DIGITS - 1) / DEC_DIGITS;

		/* 0, or number of decimal digits to keep in last NBASE digit */
		di %= DEC_DIGITS;

		if (ndigits < var->ndigits ||
			(ndigits == var->ndigits && di > 0))
		{
			var->ndigits = ndigits;

#if DEC_DIGITS == 1
			/* di must be zero */
			carry = (digits[ndigits] >= HALF_NBASE) ? 1 : 0;
#else
			if (di == 0)
				carry = (digits[ndigits] >= HALF_NBASE) ? 1 : 0;
			else
			{
				/* Must round within last NBASE digit */
				int			extra,
							pow10;

#if DEC_DIGITS == 4
				pow10 = round_powers[di];
#elif DEC_DIGITS == 2
				pow10 = 10;
#else
#error unsupported NBASE
#endif
				extra = digits[--ndigits] % pow10;
				digits[ndigits] -= extra;
				carry = 0;
				if (extra >= pow10 / 2)
				{
					pow10 += digits[ndigits];
					if (pow10 >= NBASE)
					{
						pow10 -= NBASE;
						carry = 1;
					}
					digits[ndigits] = pow10;
				}
			}
#endif

			/* Propagate carry if needed */
			while (carry)
			{
				carry += digits[--ndigits];
				if (carry >= NBASE)
				{
					digits[ndigits] = carry - NBASE;
					carry = 1;
				}
				else
				{
					digits[ndigits] = carry;
					carry = 0;
				}
			}

			if (ndigits < 0)
			{
				Assert(ndigits == -1);	/* better not have added > 1 digit */
				Assert(var->digits > var->buf);
				var->digits--;
				var->ndigits++;
				var->weight++;
			}
		}
	}
}

/*
 * set_var_from_str()
 *
 *	Parse a string and put the number into a variable
 *
 * This function does not handle leading or trailing spaces, and it doesn't
 * accept "NaN" either.  It returns the end+1 position so that caller can
 * check for trailing spaces/garbage if deemed necessary.
 *
 * cp is the place to actually start parsing; str is what to use in error
 * reports.  (Typically cp would be the same except advanced over spaces.)
 */
static const char *
set_var_from_str(const char *str, const char *cp, NumericVar *dest)
{
	bool		have_dp = false;
	int			i;
	unsigned char *decdigits;
	int			sign = NUMERIC_POS;
	int			dweight = -1;
	int			ddigits;
	int			dscale = 0;
	int			weight;
	int			ndigits;
	int			offset;
	NumericDigit *digits;

	/*
	 * We first parse the string to extract decimal digits and determine the
	 * correct decimal weight.  Then convert to NBASE representation.
	 */
	switch (*cp)
	{
		case '+':
			sign = NUMERIC_POS;
			cp++;
			break;

		case '-':
			sign = NUMERIC_NEG;
			cp++;
			break;
	}

	if (*cp == '.')
	{
		have_dp = true;
		cp++;
	}

	if (!isdigit((unsigned char) *cp))
     fprintf(stderr, "invalid input syntax for type %s: %s\n", "numeric", str);

	decdigits = (unsigned char *) malloc(strlen(cp) + DEC_DIGITS * 2);

	/* leading padding for digit alignment later */
	memset(decdigits, 0, DEC_DIGITS);
	i = DEC_DIGITS;

	while (*cp)
	{
		if (isdigit((unsigned char) *cp))
		{
			decdigits[i++] = *cp++ - '0';
			if (!have_dp)
				dweight++;
			else
				dscale++;
		}
		else if (*cp == '.')
		{
			if (have_dp)
         fprintf(stderr, "invalid input syntax for type %s: \"%s\"\n", "numeric", str);
			have_dp = true;
			cp++;
		}
		else
			break;
	}

	ddigits = i - DEC_DIGITS;
	/* trailing padding for digit alignment later */
	memset(decdigits + i, 0, DEC_DIGITS - 1);

	/* Handle exponent, if any */
	if (*cp == 'e' || *cp == 'E')
	{
		long		exponent;
		char	   *endptr;

		cp++;
		exponent = strtol(cp, &endptr, 10);
		if (endptr == cp)
       fprintf(stderr, "invalid input syntax for type %s: \"%s\"\n", "numeric", str);
		cp = endptr;

		/*
		 * At this point, dweight and dscale can't be more than about
		 * INT_MAX/2 due to the MaxAllocSize limit on string length, so
		 * constraining the exponent similarly should be enough to prevent
		 * integer overflow in this function.  If the value is too large to
		 * fit in storage format, make_result() will complain about it later;
		 * for consistency use the same ereport errcode/text as make_result().
		 */
		if (exponent >= INT_MAX / 2 || exponent <= -(INT_MAX / 2))
       fprintf(stderr, "value overflow numeric format\n");
		dweight += (int) exponent;
		dscale -= (int) exponent;
		if (dscale < 0)
			dscale = 0;
	}

	/*
	 * Okay, convert pure-decimal representation to base NBASE.  First we need
	 * to determine the converted weight and ndigits.  offset is the number of
	 * decimal zeroes to insert before the first given digit to have a
	 * correctly aligned first NBASE digit.
	 */
	if (dweight >= 0)
		weight = (dweight + 1 + DEC_DIGITS - 1) / DEC_DIGITS - 1;
	else
		weight = -((-dweight - 1) / DEC_DIGITS + 1);
	offset = (weight + 1) * DEC_DIGITS - (dweight + 1);
	ndigits = (ddigits + offset + DEC_DIGITS - 1) / DEC_DIGITS;

	alloc_var(dest, ndigits);
	dest->sign = sign;
	dest->weight = weight;
	dest->dscale = dscale;

	i = DEC_DIGITS - offset;
	digits = dest->digits;

	while (ndigits-- > 0)
	{
#if DEC_DIGITS == 4
		*digits++ = ((decdigits[i] * 10 + decdigits[i + 1]) * 10 +
					 decdigits[i + 2]) * 10 + decdigits[i + 3];
#elif DEC_DIGITS == 2
		*digits++ = decdigits[i] * 10 + decdigits[i + 1];
#elif DEC_DIGITS == 1
		*digits++ = decdigits[i];
#else
#error unsupported NBASE
#endif
		i += DEC_DIGITS;
	}

	free(decdigits);

	/* Strip any leading/trailing zeroes, and normalize weight if zero */
	strip_var(dest);

	/* Return end+1 position for caller */
	return cp;
}

/*
 * apply_typmod() -
 *
 *	Do bounds checking and rounding according to the attributes
 *	typmod field.
 */
static void
apply_typmod(NumericVar *var, int32 typmod)
{
	int			precision;
	int			scale;
	int			maxdigits;
	int			ddigits;
	int			i;

	/* Do nothing if we have a default typmod (-1) */
	if (typmod < (int32) (VARHDRSZ))
		return;

	typmod -= VARHDRSZ;
	precision = (typmod >> 16) & 0xffff;
	scale = typmod & 0xffff;
	maxdigits = precision - scale;

	/* Round to target scale (and set var->dscale) */
	round_var(var, scale);

	/*
	 * Check for overflow - note we can't do this before rounding, because
	 * rounding could raise the weight.  Also note that the var's weight could
	 * be inflated by leading zeroes, which will be stripped before storage
	 * but perhaps might not have been yet. In any case, we must recognize a
	 * true zero, whose weight doesn't mean anything.
	 */
	ddigits = (var->weight + 1) * DEC_DIGITS;
	if (ddigits > maxdigits)
	{
		/* Determine true weight; and check for all-zero result */
		for (i = 0; i < var->ndigits; i++)
		{
			NumericDigit dig = var->digits[i];

			if (dig)
			{
				/* Adjust for any high-order decimal zero digits */
#if DEC_DIGITS == 4
				if (dig < 10)
					ddigits -= 3;
				else if (dig < 100)
					ddigits -= 2;
				else if (dig < 1000)
					ddigits -= 1;
#elif DEC_DIGITS == 2
				if (dig < 10)
					ddigits -= 1;
#elif DEC_DIGITS == 1
				/* no adjustment */
#else
#error unsupported NBASE
#endif
				if (ddigits > maxdigits)
           fprintf(stderr, "numeric field overflow, a field with precision %d, scale %d must round to an absolute value less than %s%d\n", precision, scale, maxdigits ? "10^":"", maxdigits ? maxdigits : 1);
				break;
			}
			ddigits -= DEC_DIGITS;
		}
	}
}

Datum
numeric_in(char *str)
{
	int32		typmod = -1;
	Numeric		res;
	const char *cp;

	/* Skip leading spaces */
	cp = str;
	while (*cp)
	{
		if (!isspace((unsigned char) *cp))
			break;
		cp++;
	}

  // no check for NaN
	
	{
		/*
		 * Use set_var_from_str() to parse a normal numeric value
		 */
		NumericVar	value;

		init_var(&value);

		cp = set_var_from_str(str, cp, &value);

		apply_typmod(&value, typmod);

		res = make_result(&value);
		free_var(&value);
	}

	PG_RETURN_NUMERIC(res);
}

Datum
hash_numeric_extended(Numeric key, uint64 seed)
{
	Datum		digit_hash;
	Datum		result;
	int			weight;
	int			start_offset;
	int			end_offset;
	int			i;
	int			hash_len;
	NumericDigit *digits;

	if (NUMERIC_IS_NAN(key))
		PG_RETURN_UINT64(seed);

	weight = NUMERIC_WEIGHT(key);
	start_offset = 0;
	end_offset = 0;

	digits = NUMERIC_DIGITS(key);
	for (i = 0; i < NUMERIC_NDIGITS(key); i++)
	{
		if (digits[i] != (NumericDigit) 0)
			break;

		start_offset++;

		weight--;
	}

	if (NUMERIC_NDIGITS(key) == start_offset)
		PG_RETURN_UINT64(seed - 1);

	for (i = NUMERIC_NDIGITS(key) - 1; i >= 0; i--)
	{
		if (digits[i] != (NumericDigit) 0)
			break;

		end_offset++;
	}

	Assert(start_offset + end_offset < NUMERIC_NDIGITS(key));

	hash_len = NUMERIC_NDIGITS(key) - start_offset - end_offset;
	digit_hash = hash_any_extended((unsigned char *) (NUMERIC_DIGITS(key)
                                                    + start_offset),
                                 hash_len * sizeof(NumericDigit),
                                 seed);
  printf("%016" PRIxPTR "---weight----%d\n", digit_hash, weight);
  fflush(stdout);
	result = UInt64GetDatum(DatumGetUInt64(digit_hash) ^ weight);
  
	PG_RETURN_DATUM(result);
}


int get_matching_hash_bounds_numeric(char *var, int greatest_modulus) {
  uint64_t seed = 0x7A5B22367996DCFDUL;
	Datum hash;
  Numeric n = (Numeric)numeric_in(var);
  printf("----%p\n", n);
	hash = hash_numeric_extended(n, seed);
	/* currently only support on hash column */
	uint64 rowHash = 0;
	rowHash = hash_combine64(0, hash);
	if (greatest_modulus == 0)
		greatest_modulus = 1;

	return rowHash % greatest_modulus;
}
