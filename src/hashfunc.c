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
#include <ctype.h>
#include <assert.h>
#include "hgint.h"


#define Assert(p) assert(p)


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

#define UInt64GetDatum(X) ((Datum) (X))
#define DatumGetUInt32(X) ((uint32) (X))


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
	return 0;
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
