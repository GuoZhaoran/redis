/* Hash Tables Implementation.
 *
 * This file implements in-memory hash tables with insert/del/replace/find/
 * get-random-element operations. Hash tables will auto-resize if needed
 * tables of power of two in size are used or a combination of multiple
 * defined max tables, collisions are handled by chaining. See the source
 * code for more information... :)
 *
 * Copyright (c) 2021, Guozhaoran <guozhaoran.prince@qq.com>
 * Copyright (c) 2006-2012, Salvatore Sanfilippo <antirez at gmail dot com>
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 *
 *   * Redistributions of source code must retain the above copyright notice,
 *     this list of conditions and the following disclaimer.
 *   * Redistributions in binary form must reproduce the above copyright
 *     notice, this list of conditions and the following disclaimer in the
 *     documentation and/or other materials provided with the distribution.
 *   * Neither the name of Redis nor the names of its contributors may be used
 *     to endorse or promote products derived from this software without
 *     specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
 * AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE
 * LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
 * CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
 * SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
 * INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
 * CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 * ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
 * POSSIBILITY OF SUCH DAMAGE.
 */

#ifndef __DICTREMAKE_REMAKE_H
#define __DICTREMAKE_REMAKE_H

#include "mt19937-64.h"
#include <limits.h>
#include <stdint.h>
#include <stdlib.h>

#define DICTREMAKE_OK 0
#define DICTREMAKE_ERR 1

typedef struct dictRemakeEntry {
    void *key;
    union {
        void *val;
        uint64_t u64;
        int64_t s64;
        double d;
    } v;
    struct dictRemakeEntry *next;     /* Next entry in the same hash bucket. */
    void *metadata[];           /* An arbitrary number of bytes (starting at a
                                 * pointer-aligned address) of size as returned
                                 * by dictRemakeType's dictRemakeEntryMetadataBytes(). */
} dictRemakeEntry;

typedef struct dictRemake dictRemake;

typedef struct dictRemakeType {
    uint64_t (*hashFunction)(const void *key);
    void *(*keyDup)(dictRemake *d, const void *key);
    void *(*valDup)(dictRemake *d, const void *obj);
    int (*keyCompare)(dictRemake *d, const void *key1, const void *key2);
    void (*keyDestructor)(dictRemake *d, void *key);
    void (*valDestructor)(dictRemake *d, void *obj);
    int (*expandAllowed)(size_t moreMem, double usedRatio);
    /* Allow a dictRemakeEntry to carry extra caller-defined metadata.  The
     * extra memory is initialized to 0 when a dictRemakeEntry is allocated. */
    size_t (*dictRemakeEntryMetadataBytes)(dictRemake *d);
} dictRemakeType;

#define DICTREMAKE_HT_SIZE(exp) ((exp) == -1 ? 0 : (unsigned long)1<<(exp))
#define DICTREMAKE_HT_SIZE_MASK(exp) ((exp) == -1 ? 0 : (DICTREMAKE_HT_SIZE(exp))-1)

struct dictRemake {
    dictRemakeType *type;

    dictRemakeEntry **ht_table[2];
    unsigned long ht_used[2];

    long rehashidx; /* rehashing not in progress if rehashidx == -1 */

    /* Keep small vars at end for optimal (minimal) struct padding */
    int16_t pauserehash; /* If >0 rehashing is paused (<0 indicates coding error) */
    signed char ht_size_exp[2]; /* exponent of size. (size = 1<<exp) */
};

/* If safe is set to 1 this is a safe iterator, that means, you can call
 * dictRemakeAdd, dictRemakeFind, and other functions against the dictRemakeionary even while
 * iterating. Otherwise it is a non safe iterator, and only dictRemakeNext()
 * should be called while iterating. */
typedef struct dictRemakeIterator {
    dictRemake *d;
    long index;
    int table, safe;
    dictRemakeEntry *entry, *nextEntry;
    /* unsafe iterator fingerprint for misuse detection. */
    long long fingerprint;
} dictRemakeIterator;

typedef void (dictRemakeScanFunction)(void *privdata, const dictRemakeEntry *de);
typedef void (dictRemakeScanBucketFunction)(dictRemake *d, dictRemakeEntry **bucketref);

/* This is the initial size of every hash table */
#define DICTREMAKE_HT_INITIAL_EXP      2
#define DICTREMAKE_HT_INITIAL_SIZE     (1<<(DICTREMAKE_HT_INITIAL_EXP))

/* ------------------------------- Macros ------------------------------------*/
#define dictRemakeFreeVal(d, entry) \
    if ((d)->type->valDestructor) \
        (d)->type->valDestructor((d), (entry)->v.val)

#define dictRemakeSetVal(d, entry, _val_) do { \
    if ((d)->type->valDup) \
        (entry)->v.val = (d)->type->valDup((d), _val_); \
    else \
        (entry)->v.val = (_val_); \
} while(0)

#define dictRemakeSetSignedIntegerVal(entry, _val_) \
    do { (entry)->v.s64 = _val_; } while(0)

#define dictRemakeSetUnsignedIntegerVal(entry, _val_) \
    do { (entry)->v.u64 = _val_; } while(0)

#define dictRemakeSetDoubleVal(entry, _val_) \
    do { (entry)->v.d = _val_; } while(0)

#define dictRemakeFreeKey(d, entry) \
    if ((d)->type->keyDestructor) \
        (d)->type->keyDestructor((d), (entry)->key)

#define dictRemakeSetKey(d, entry, _key_) do { \
    if ((d)->type->keyDup) \
        (entry)->key = (d)->type->keyDup((d), _key_); \
    else \
        (entry)->key = (_key_); \
} while(0)

#define dictRemakeCompareKeys(d, key1, key2) \
    (((d)->type->keyCompare) ? \
        (d)->type->keyCompare((d), key1, key2) : \
        (key1) == (key2))

#define dictRemakeMetadata(entry) (&(entry)->metadata)
#define dictRemakeMetadataSize(d) ((d)->type->dictRemakeEntryMetadataBytes \
                             ? (d)->type->dictRemakeEntryMetadataBytes(d) : 0)

#define dictRemakeHashKey(d, key) (d)->type->hashFunction(key)
#define dictRemakeGetKey(he) ((he)->key)
#define dictRemakeGetVal(he) ((he)->v.val)
#define dictRemakeGetSignedIntegerVal(he) ((he)->v.s64)
#define dictRemakeGetUnsignedIntegerVal(he) ((he)->v.u64)
#define dictRemakeGetDoubleVal(he) ((he)->v.d)
#define dictRemakeSlots(d) (DICTREMAKE_HT_SIZE((d)->ht_size_exp[0])+DICTREMAKE_HT_SIZE((d)->ht_size_exp[1]))
#define dictRemakeSize(d) ((d)->ht_used[0]+(d)->ht_used[1])
#define dictRemakeIsRehashing(d) ((d)->rehashidx != -1)
#define dictRemakePauseRehashing(d) (d)->pauserehash++
#define dictRemakeResumeRehashing(d) (d)->pauserehash--

/* If our unsigned long type can store a 64 bit number, use a 64 bit PRNG. */
#if ULONG_MAX >= 0xffffffffffffffff
#define randomULong() ((unsigned long) genrand64_int64())
#else
#define randomULong() random()
#endif

/* API */
dictRemake *dictRemakeCreate(dictRemakeType *type);
int dictRemakeExpand(dictRemake *d, unsigned long size);
int dictRemakeTryExpand(dictRemake *d, unsigned long size);
int dictRemakeAdd(dictRemake *d, void *key, void *val);
dictRemakeEntry *dictRemakeAddRaw(dictRemake *d, void *key, dictRemakeEntry **existing);
dictRemakeEntry *dictRemakeAddOrFind(dictRemake *d, void *key);
int dictRemakeReplace(dictRemake *d, void *key, void *val);
int dictRemakeDelete(dictRemake *d, const void *key);
dictRemakeEntry *dictRemakeUnlink(dictRemake *d, const void *key);
void dictRemakeFreeUnlinkedEntry(dictRemake *d, dictRemakeEntry *he);
void dictRemakeRelease(dictRemake *d);
dictRemakeEntry * dictRemakeFind(dictRemake *d, const void *key);
void *dictRemakeFetchValue(dictRemake *d, const void *key);
int dictRemakeResize(dictRemake *d);
dictRemakeIterator *dictRemakeGetIterator(dictRemake *d);
dictRemakeIterator *dictRemakeGetSafeIterator(dictRemake *d);
dictRemakeEntry *dictRemakeNext(dictRemakeIterator *iter);
void dictRemakeReleaseIterator(dictRemakeIterator *iter);
dictRemakeEntry *dictRemakeGetRandomKey(dictRemake *d);
dictRemakeEntry *dictRemakeGetFairRandomKey(dictRemake *d);
unsigned int dictRemakeGetSomeKeys(dictRemake *d, dictRemakeEntry **des, unsigned int count);
void dictRemakeGetStats(char *buf, size_t bufsize, dictRemake *d);
uint64_t dictRemakeGenHashFunction(const void *key, int len);
uint64_t dictRemakeGenCaseHashFunction(const unsigned char *buf, int len);
void dictRemakeEmpty(dictRemake *d, void(callback)(dictRemake*));
void dictRemakeEnableResize(void);
void dictRemakeDisableResize(void);
int dictRemakeRehash(dictRemake *d, int n);
int dictRemakeRehashMilliseconds(dictRemake *d, int ms);
void dictRemakeSetHashFunctionSeed(uint8_t *seed);
uint8_t *dictRemakeGetHashFunctionSeed(void);
unsigned long dictRemakeScan(dictRemake *d, unsigned long v, dictRemakeScanFunction *fn, dictRemakeScanBucketFunction *bucketfn, void *privdata);
uint64_t dictRemakeGetHash(dictRemake *d, const void *key);
dictRemakeEntry **dictRemakeFindEntryRefByPtrAndHash(dictRemake *d, const void *oldptr, uint64_t hash);

#ifdef REDIS_TEST
int dictRemakeTest(int argc, char *argv[], int accurate);
#endif

#endif //__DICTREMAKE_REMAKE_H
