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

#include "fmacros.h"

#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include <string.h>
#include <stdarg.h>
#include <limits.h>
#include <sys/time.h>

#include "dict_remake.h"
#include "zmalloc.h"
#include "redisassert.h"

/* Using dictRemakeEnableResize() / dictRemakeDisableResize() we make possible to
 * enable/disable resizing of the hash table as needed. This is very important
 * for Redis, as we use copy-on-write and don't want to move too much memory
 * around when there is a child performing saving operations.
 *
 * Note that even when dict_remake_can_resize is set to 0, not all resizes are
 * prevented: a hash table is still allowed to grow if the ratio between
 * the number of elements and the buckets > dict_remake_force_resize_ratio. */
static int dict_remake_can_resize = 1;
static unsigned int dict_remake_force_resize_ratio = 5;

/* -------------------------- private prototypes ---------------------------- */

static int _dictRemakeExpandIfNeeded(dictRemake *d);
static signed char _dictRemakeNextExp(unsigned long size);
static long _dictRemakeKeyIndex(dictRemake *d, const void *key, uint64_t hash, dictRemakeEntry **existing);
static int _dictRemakeInit(dictRemake *d, dictRemakeType *type);

/* -------------------------- hash functions -------------------------------- */

static uint8_t dict_remake_hash_function_seed[16];

void dictRemakeSetHashFunctionSeed(uint8_t *seed) {
    memcpy(dict_remake_hash_function_seed,seed,sizeof(dict_remake_hash_function_seed));
}

uint8_t *dictRemakeGetHashFunctionSeed(void) {
    return dict_remake_hash_function_seed;
}

/* The default hashing function uses SipHash implementation
 * in siphash.c. */

uint64_t siphash(const uint8_t *in, const size_t inlen, const uint8_t *k);
uint64_t siphash_nocase(const uint8_t *in, const size_t inlen, const uint8_t *k);

uint64_t dictRemakeGenHashFunction(const void *key, int len) {
    return siphash(key,len,dict_remake_hash_function_seed);
}

uint64_t dictRemakeGenCaseHashFunction(const unsigned char *buf, int len) {
    return siphash_nocase(buf,len,dict_remake_hash_function_seed);
}

/* ----------------------------- API implementation ------------------------- */

/* Reset a hash table already initialized with ht_init().
 * NOTE: This function should only be called by ht_destroy(). */
static void _dictRemakeReset(dictRemake *d, int htidx)
{
    d->ht_table[htidx] = NULL;
    d->ht_size_exp[htidx] = -1;
    d->ht_used[htidx] = 0;
}

/* Create a new hash table */
dictRemake *dictRemakeCreate(dictRemakeType *type)
{
    dictRemake *d = zmalloc(sizeof(*d));

    _dictRemakeInit(d,type);
    return d;
}

/* Initialize the hash table */
int _dictRemakeInit(dictRemake *d, dictRemakeType *type)
{
    _dictRemakeReset(d, 0);
    _dictRemakeReset(d, 1);
    d->type = type;
    d->rehashidx = -1;
    d->pauserehash = 0;
    return DICTREMAKE_OK;
}

/* Resize the table to the minimal size that contains all the elements,
 * but with the invariant of a USED/BUCKETS ratio near to <= 1 */
int dictRemakeResize(dictRemake *d)
{
    unsigned long minimal;

    if (!dict_remake_can_resize || dictRemakeIsRehashing(d)) return DICTREMAKE_ERR;
    minimal = d->ht_used[0];
    if (minimal < DICTREMAKE_HT_INITIAL_SIZE)
        minimal = DICTREMAKE_HT_INITIAL_SIZE;
    return dictRemakeExpand(d, minimal);
}

/* Expand or create the hash table,
 * when malloc_failed is non-NULL, it'll avoid panic if malloc fails (in which case it'll be set to 1).
 * Returns DICTREMAKE_OK if expand was performed, and DICTREMAKE_ERR if skipped. */
int _dictRemakeExpand(dictRemake *d, unsigned long size, int* malloc_failed)
{
    if (malloc_failed) *malloc_failed = 0;

    /* the size is invalid if it is smaller than the number of
     * elements already inside the hash table */
    if (dictRemakeIsRehashing(d) || d->ht_used[0] > size)
        return DICTREMAKE_ERR;

    /* the new hash table */
    dictRemakeEntry **new_ht_table;
    unsigned long new_ht_used;
    signed char new_ht_size_exp = _dictRemakeNextExp(size);

    /* Detect overflows */
    size_t newsize = 1ul<<new_ht_size_exp;
    if (newsize < size || newsize * sizeof(dictRemakeEntry*) < newsize)
        return DICTREMAKE_ERR;

    /* Rehashing to the same table size is not useful. */
    if (new_ht_size_exp == d->ht_size_exp[0]) return DICTREMAKE_ERR;

    /* Allocate the new hash table and initialize all pointers to NULL */
    if (malloc_failed) {
        new_ht_table = ztrycalloc(newsize*sizeof(dictRemakeEntry*));
        *malloc_failed = new_ht_table == NULL;
        if (*malloc_failed)
            return DICTREMAKE_ERR;
    } else
        new_ht_table = zcalloc(newsize*sizeof(dictRemakeEntry*));

    new_ht_used = 0;

    /* Is this the first initialization? If so it's not really a rehashing
     * we just set the first hash table so that it can accept keys. */
    if (d->ht_table[0] == NULL) {
        d->ht_size_exp[0] = new_ht_size_exp;
        d->ht_used[0] = new_ht_used;
        d->ht_table[0] = new_ht_table;
        return DICTREMAKE_OK;
    }

    /* Prepare a second hash table for incremental rehashing */
    d->ht_size_exp[1] = new_ht_size_exp;
    d->ht_used[1] = new_ht_used;
    d->ht_table[1] = new_ht_table;
    d->rehashidx = 0;
    return DICTREMAKE_OK;
}

/* return DICTREMAKE_ERR if expand was not performed */
int dictRemakeExpand(dictRemake *d, unsigned long size) {
    return _dictRemakeExpand(d, size, NULL);
}

/* return DICTREMAKE_ERR if expand failed due to memory allocation failure */
int dictRemakeTryExpand(dictRemake *d, unsigned long size) {
    int malloc_failed;
    _dictRemakeExpand(d, size, &malloc_failed);
    return malloc_failed? DICTREMAKE_ERR : DICTREMAKE_OK;
}

/* Performs N steps of incremental rehashing. Returns 1 if there are still
 * keys to move from the old to the new hash table, otherwise 0 is returned.
 *
 * Note that a rehashing step consists in moving a bucket (that may have more
 * than one key as we use chaining) from the old to the new hash table, however
 * since part of the hash table may be composed of empty spaces, it is not
 * guaranteed that this function will rehash even a single bucket, since it
 * will visit at max N*10 empty buckets in total, otherwise the amount of
 * work it does would be unbound and the function may block for a long time. */
int dictRemakeRehash(dictRemake *d, int n) {
    int empty_visits = n*10; /* Max number of empty buckets to visit. */
    if (!dictRemakeIsRehashing(d)) return 0;

    while(n-- && d->ht_used[0] != 0) {
        dictRemakeEntry *de, *nextde;

        /* Note that rehashidx can't overflow as we are sure there are more
         * elements because ht[0].used != 0 */
        assert(DICTREMAKE_HT_SIZE(d->ht_size_exp[0]) > (unsigned long)d->rehashidx);
        while(d->ht_table[0][d->rehashidx] == NULL) {
            d->rehashidx++;
            if (--empty_visits == 0) return 1;
        }
        de = d->ht_table[0][d->rehashidx];
        /* Move all the keys in this bucket from the old to the new hash HT */
        while(de) {
            uint64_t h;

            nextde = de->next;
            /* Get the index in the new hash table */
            h = dictRemakeHashKey(d, de->key) & DICTREMAKE_HT_SIZE_MASK(d->ht_size_exp[1]);
            de->next = d->ht_table[1][h];
            d->ht_table[1][h] = de;
            d->ht_used[0]--;
            d->ht_used[1]++;
            de = nextde;
        }
        d->ht_table[0][d->rehashidx] = NULL;
        d->rehashidx++;
    }

    /* Check if we already rehashed the whole table... */
    if (d->ht_used[0] == 0) {
        zfree(d->ht_table[0]);
        /* Copy the new ht onto the old one */
        d->ht_table[0] = d->ht_table[1];
        d->ht_used[0] = d->ht_used[1];
        d->ht_size_exp[0] = d->ht_size_exp[1];
        _dictRemakeReset(d, 1);
        d->rehashidx = -1;
        return 0;
    }

    /* More to rehash... */
    return 1;
}

long long timeInMillisecondsRemake(void) {
    struct timeval tv;

    gettimeofday(&tv,NULL);
    return (((long long)tv.tv_sec)*1000)+(tv.tv_usec/1000);
}

/* Rehash in ms+"delta" milliseconds. The value of "delta" is larger
 * than 0, and is smaller than 1 in most cases. The exact upper bound
 * depends on the running time of dictRemakeRehash(d,100).*/
int dictRemakeRehashMilliseconds(dictRemake *d, int ms) {
    if (d->pauserehash > 0) return 0;

    long long start = timeInMillisecondsRemake();
    int rehashes = 0;

    while(dictRemakeRehash(d,100)) {
        rehashes += 100;
        if (timeInMillisecondsRemake()-start > ms) break;
    }
    return rehashes;
}

/* This function performs just a step of rehashing, and only if hashing has
 * not been paused for our hash table. When we have iterators in the
 * middle of a rehashing we can't mess with the two hash tables otherwise
 * some element can be missed or duplicated.
 *
 * This function is called by common lookup or update operations in the
 * dictRemakeionary so that the hash table automatically migrates from H1 to H2
 * while it is actively used. */
static void _dictRemakeRehashStep(dictRemake *d) {
    if (d->pauserehash == 0) dictRemakeRehash(d,1);
}

/* Add an element to the target hash table */
int dictRemakeAdd(dictRemake *d, void *key, void *val)
{
    dictRemakeEntry *entry = dictRemakeAddRaw(d,key,NULL);

    if (!entry) return DICTREMAKE_ERR;
    dictRemakeSetVal(d, entry, val);
    return DICTREMAKE_OK;
}

/* Low level add or find:
 * This function adds the entry but instead of setting a value returns the
 * dictRemakeEntry structure to the user, that will make sure to fill the value
 * field as they wish.
 *
 * This function is also directly exposed to the user API to be called
 * mainly in order to store non-pointers inside the hash value, example:
 *
 * entry = dictRemakeAddRaw(dictRemake,mykey,NULL);
 * if (entry != NULL) dictRemakeSetSignedIntegerVal(entry,1000);
 *
 * Return values:
 *
 * If key already exists NULL is returned, and "*existing" is populated
 * with the existing entry if existing is not NULL.
 *
 * If key was added, the hash entry is returned to be manipulated by the caller.
 */
dictRemakeEntry *dictRemakeAddRaw(dictRemake *d, void *key, dictRemakeEntry **existing)
{
    long index;
    dictRemakeEntry *entry;
    int htidx;

    if (dictRemakeIsRehashing(d)) _dictRemakeRehashStep(d);

    /* Get the index of the new element, or -1 if
     * the element already exists. */
    if ((index = _dictRemakeKeyIndex(d, key, dictRemakeHashKey(d,key), existing)) == -1)
        return NULL;

    /* Allocate the memory and store the new entry.
     * Insert the element in top, with the assumption that in a database
     * system it is more likely that recently added entries are accessed
     * more frequently. */
    htidx = dictRemakeIsRehashing(d) ? 1 : 0;
    size_t metasize = dictRemakeMetadataSize(d);
    entry = zmalloc(sizeof(*entry) + metasize);
    if (metasize > 0) {
        memset(dictRemakeMetadata(entry), 0, metasize);
    }
    entry->next = d->ht_table[htidx][index];
    d->ht_table[htidx][index] = entry;
    d->ht_used[htidx]++;

    /* Set the hash entry fields. */
    dictRemakeSetKey(d, entry, key);
    return entry;
}

/* Search and remove an element. This is a helper function for
 * dictRemakeDelete() and dictRemakeUnlink(), please check the top comment
 * of those functions. */
static dictRemakeEntry *dictRemakeGenericDelete(dictRemake *d, const void *key, int nofree) {
    uint64_t h, idx;
    dictRemakeEntry *he, *prevHe;
    int table;

    /* dictRemake is empty */
    if (dictRemakeSize(d) == 0) return NULL;

    if (dictRemakeIsRehashing(d)) _dictRemakeRehashStep(d);
    h = dictRemakeHashKey(d, key);

    for (table = 0; table <= 1; table++) {
        idx = h & DICTREMAKE_HT_SIZE_MASK(d->ht_size_exp[table]);
        he = d->ht_table[table][idx];
        prevHe = NULL;
        while(he) {
            if (key==he->key || dictRemakeCompareKeys(d, key, he->key)) {
                /* Unlink the element from the list */
                if (prevHe)
                    prevHe->next = he->next;
                else
                    d->ht_table[table][idx] = he->next;
                if (!nofree) {
                    dictRemakeFreeUnlinkedEntry(d, he);
                }
                d->ht_used[table]--;
                return he;
            }
            prevHe = he;
            he = he->next;
        }
        if (!dictRemakeIsRehashing(d)) break;
    }
    return NULL; /* not found */
}

/* Remove an element, returning DICTREMAKE_OK on success or DICTREMAKE_ERR if the
 * element was not found. */
int dictRemakeDelete(dictRemake *ht, const void *key) {
    return dictRemakeGenericDelete(ht,key,0) ? DICTREMAKE_OK : DICTREMAKE_ERR;
}

/* Remove an element from the table, but without actually releasing
 * the key, value and dictRemakeionary entry. The dictRemakeionary entry is returned
 * if the element was found (and unlinked from the table), and the user
 * should later call `dictRemakeFreeUnlinkedEntry()` with it in order to release it.
 * Otherwise if the key is not found, NULL is returned.
 *
 * This function is useful when we want to remove something from the hash
 * table but want to use its value before actually deleting the entry.
 * Without this function the pattern would require two lookups:
 *
 *  entry = dictRemakeFind(...);
 *  // Do something with entry
 *  dictRemakeDelete(dictRemakeionary,entry);
 *
 * Thanks to this function it is possible to avoid this, and use
 * instead:
 *
 * entry = dictRemakeUnlink(dictRemakeionary,entry);
 * // Do something with entry
 * dictRemakeFreeUnlinkedEntry(entry); // <- This does not need to lookup again.
 */
dictRemakeEntry *dictRemakeUnlink(dictRemake *d, const void *key) {
    return dictRemakeGenericDelete(d,key,1);
}

/* You need to call this function to really free the entry after a call
 * to dictRemakeUnlink(). It's safe to call this function with 'he' = NULL. */
void dictRemakeFreeUnlinkedEntry(dictRemake *d, dictRemakeEntry *he) {
    if (he == NULL) return;
    dictRemakeFreeKey(d, he);
    dictRemakeFreeVal(d, he);
    zfree(he);
}

/* Destroy an entire dictRemakeionary */
int _dictRemakeClear(dictRemake *d, int htidx, void(callback)(dictRemake*)) {
    unsigned long i;

    /* Free all the elements */
    for (i = 0; i < DICTREMAKE_HT_SIZE(d->ht_size_exp[htidx]) && d->ht_used[htidx] > 0; i++) {
        dictRemakeEntry *he, *nextHe;

        if (callback && (i & 65535) == 0) callback(d);

        if ((he = d->ht_table[htidx][i]) == NULL) continue;
        while(he) {
            nextHe = he->next;
            dictRemakeFreeKey(d, he);
            dictRemakeFreeVal(d, he);
            zfree(he);
            d->ht_used[htidx]--;
            he = nextHe;
        }
    }
    /* Free the table and the allocated cache structure */
    zfree(d->ht_table[htidx]);
    /* Re-initialize the table */
    _dictRemakeReset(d, htidx);
    return DICTREMAKE_OK; /* never fails */
}

/* Clear & Release the hash table */
void dictRemakeRelease(dictRemake *d)
{
    _dictRemakeClear(d,0,NULL);
    _dictRemakeClear(d,1,NULL);
    zfree(d);
}

dictRemakeEntry *dictRemakeFind(dictRemake *d, const void *key)
{
    dictRemakeEntry *he;
    uint64_t h, idx, table;

    if (dictRemakeSize(d) == 0) return NULL; /* dictRemake is empty */
    if (dictRemakeIsRehashing(d)) _dictRemakeRehashStep(d);
    h = dictRemakeHashKey(d, key);
    for (table = 0; table <= 1; table++) {
        idx = h & DICTREMAKE_HT_SIZE_MASK(d->ht_size_exp[table]);
        he = d->ht_table[table][idx];
        while(he) {
            if (key==he->key || dictRemakeCompareKeys(d, key, he->key))
                return he;
            he = he->next;
        }
        if (!dictRemakeIsRehashing(d)) return NULL;
    }
    return NULL;
}

/* A fingerprint is a 64 bit number that represents the state of the dictRemakeionary
 * at a given time, it's just a few dictRemake properties xored together.
 * When an unsafe iterator is initialized, we get the dictRemake fingerprint, and check
 * the fingerprint again when the iterator is released.
 * If the two fingerprints are different it means that the user of the iterator
 * performed forbidden operations against the dictRemakeionary while iterating. */
long long dictRemakeFingerprint(dictRemake *d) {
    long long integers[6], hash = 0;
    int j;

    integers[0] = (long) d->ht_table[0];
    integers[1] = d->ht_size_exp[0];
    integers[2] = d->ht_used[0];
    integers[3] = (long) d->ht_table[1];
    integers[4] = d->ht_size_exp[1];
    integers[5] = d->ht_used[1];

    /* We hash N integers by summing every successive integer with the integer
     * hashing of the previous sum. Basically:
     *
     * Result = hash(hash(hash(int1)+int2)+int3) ...
     *
     * This way the same set of integers in a different order will (likely) hash
     * to a different number. */
    for (j = 0; j < 6; j++) {
        hash += integers[j];
        /* For the hashing step we use Tomas Wang's 64 bit integer hash. */
        hash = (~hash) + (hash << 21); // hash = (hash << 21) - hash - 1;
        hash = hash ^ (hash >> 24);
        hash = (hash + (hash << 3)) + (hash << 8); // hash * 265
        hash = hash ^ (hash >> 14);
        hash = (hash + (hash << 2)) + (hash << 4); // hash * 21
        hash = hash ^ (hash >> 28);
        hash = hash + (hash << 31);
    }
    return hash;
}

/* Return a random entry from the hash table. Useful to
 * implement randomized algorithms */
dictRemakeEntry *dictRemakeGetRandomKey(dictRemake *d)
{
    dictRemakeEntry *he, *orighe;
    unsigned long h;
    int listlen, listele;

    if (dictRemakeSize(d) == 0) return NULL;
    if (dictRemakeIsRehashing(d)) _dictRemakeRehashStep(d);
    if (dictRemakeIsRehashing(d)) {
        unsigned long s0 = DICTREMAKE_HT_SIZE(d->ht_size_exp[0]);
        do {
            /* We are sure there are no elements in indexes from 0
             * to rehashidx-1 */
            h = d->rehashidx + (randomULong() % (dictRemakeSlots(d) - d->rehashidx));
            he = (h >= s0) ? d->ht_table[1][h - s0] : d->ht_table[0][h];
        } while(he == NULL);
    } else {
        unsigned long m = DICTREMAKE_HT_SIZE_MASK(d->ht_size_exp[0]);
        do {
            h = randomULong() & m;
            he = d->ht_table[0][h];
        } while(he == NULL);
    }

    /* Now we found a non empty bucket, but it is a linked
     * list and we need to get a random element from the list.
     * The only sane way to do so is counting the elements and
     * select a random index. */
    listlen = 0;
    orighe = he;
    while(he) {
        he = he->next;
        listlen++;
    }
    listele = random() % listlen;
    he = orighe;
    while(listele--) he = he->next;
    return he;
}

/* ------------------------- private functions ------------------------------ */

/* Because we may need to allocate huge memory chunk at once when dictRemake
 * expands, we will check this allocation is allowed or not if the dictRemake
 * type has expandAllowed member function. */
static int dictRemakeTypeExpandAllowed(dictRemake *d) {
    if (d->type->expandAllowed == NULL) return 1;
    return d->type->expandAllowed(
            DICTREMAKE_HT_SIZE(_dictRemakeNextExp(d->ht_used[0] + 1)) * sizeof(dictRemakeEntry*),
            (double)d->ht_used[0] / DICTREMAKE_HT_SIZE(d->ht_size_exp[0]));
}

/* Expand the hash table if needed */
static int _dictRemakeExpandIfNeeded(dictRemake *d)
{
    /* Incremental rehashing already in progress. Return. */
    if (dictRemakeIsRehashing(d)) return DICTREMAKE_OK;

    /* If the hash table is empty expand it to the initial size. */
    if (DICTREMAKE_HT_SIZE(d->ht_size_exp[0]) == 0) return dictRemakeExpand(d, DICTREMAKE_HT_INITIAL_SIZE);

    /* If we reached the 1:1 ratio, and we are allowed to resize the hash
     * table (global setting) or we should avoid it but the ratio between
     * elements/buckets is over the "safe" threshold, we resize doubling
     * the number of buckets. */
    if (d->ht_used[0] >= DICTREMAKE_HT_SIZE(d->ht_size_exp[0]) &&
        (dict_remake_can_resize ||
         d->ht_used[0]/ DICTREMAKE_HT_SIZE(d->ht_size_exp[0]) > dict_remake_force_resize_ratio) &&
        dictRemakeTypeExpandAllowed(d))
    {
        return dictRemakeExpand(d, d->ht_used[0] + 1);
    }
    return DICTREMAKE_OK;
}

/* TODO: clz optimization */
/* Our hash table capability is a power of two */
static signed char _dictRemakeNextExp(unsigned long size)
{
    unsigned char e = DICTREMAKE_HT_INITIAL_EXP;

    if (size >= LONG_MAX) return (8*sizeof(long)-1);
    while(1) {
        if (((unsigned long)1<<e) >= size)
            return e;
        e++;
    }
}

/* Returns the index of a free slot that can be populated with
 * a hash entry for the given 'key'.
 * If the key already exists, -1 is returned
 * and the optional output parameter may be filled.
 *
 * Note that if we are in the process of rehashing the hash table, the
 * index is always returned in the context of the second (new) hash table. */
static long _dictRemakeKeyIndex(dictRemake *d, const void *key, uint64_t hash, dictRemakeEntry **existing)
{
    unsigned long idx, table;
    dictRemakeEntry *he;
    if (existing) *existing = NULL;

    /* Expand the hash table if needed */
    if (_dictRemakeExpandIfNeeded(d) == DICTREMAKE_ERR)
        return -1;
    for (table = 0; table <= 1; table++) {
        idx = hash & DICTREMAKE_HT_SIZE_MASK(d->ht_size_exp[table]);
        /* Search if this slot does not already contain the given key */
        he = d->ht_table[table][idx];
        while(he) {
            if (key==he->key || dictRemakeCompareKeys(d, key, he->key)) {
                if (existing) *existing = he;
                return -1;
            }
            he = he->next;
        }
        if (!dictRemakeIsRehashing(d)) break;
    }
    return idx;
}

void dictRemakeEmpty(dictRemake *d, void(callback)(dictRemake*)) {
    _dictRemakeClear(d,0,callback);
    _dictRemakeClear(d,1,callback);
    d->rehashidx = -1;
    d->pauserehash = 0;
}

void dictRemakeEnableResize(void) {
    dict_remake_can_resize = 1;
}

void dictRemakeDisableResize(void) {
    dict_remake_can_resize = 0;
}

uint64_t dictRemakeGetHash(dictRemake *d, const void *key) {
    return dictRemakeHashKey(d, key);
}

/* ------------------------------- Debugging ---------------------------------*/

#define DICTREMAKE_STATS_VECTLEN 50
size_t _dictRemakeGetStatsHt(char *buf, size_t bufsize, dictRemake *d, int htidx) {
    unsigned long i, slots = 0, chainlen, maxchainlen = 0;
    unsigned long totchainlen = 0;
    unsigned long clvector[DICTREMAKE_STATS_VECTLEN];
    size_t l = 0;

    if (d->ht_used[htidx] == 0) {
        return snprintf(buf,bufsize,
                        "No stats available for empty dictRemakeionaries\n");
    }

    /* Compute stats. */
    for (i = 0; i < DICTREMAKE_STATS_VECTLEN; i++) clvector[i] = 0;
    for (i = 0; i < DICTREMAKE_HT_SIZE(d->ht_size_exp[htidx]); i++) {
        dictRemakeEntry *he;

        if (d->ht_table[htidx][i] == NULL) {
            clvector[0]++;
            continue;
        }
        slots++;
        /* For each hash entry on this slot... */
        chainlen = 0;
        he = d->ht_table[htidx][i];
        while(he) {
            chainlen++;
            he = he->next;
        }
        clvector[(chainlen < DICTREMAKE_STATS_VECTLEN) ? chainlen : (DICTREMAKE_STATS_VECTLEN-1)]++;
        if (chainlen > maxchainlen) maxchainlen = chainlen;
        totchainlen += chainlen;
    }

    /* Generate human readable stats. */
    l += snprintf(buf+l,bufsize-l,
                  "Hash table %d stats (%s):\n"
                  " table size: %lu\n"
                  " number of elements: %lu\n"
                  " different slots: %lu\n"
                  " max chain length: %lu\n"
                  " avg chain length (counted): %.02f\n"
                  " avg chain length (computed): %.02f\n"
                  " Chain length distribution:\n",
                  htidx, (htidx == 0) ? "main hash table" : "rehashing target",
                  DICTREMAKE_HT_SIZE(d->ht_size_exp[htidx]), d->ht_used[htidx], slots, maxchainlen,
                  (float)totchainlen/slots, (float)d->ht_used[htidx]/slots);

    for (i = 0; i < DICTREMAKE_STATS_VECTLEN-1; i++) {
        if (clvector[i] == 0) continue;
        if (l >= bufsize) break;
        l += snprintf(buf+l,bufsize-l,
                      "   %s%ld: %ld (%.02f%%)\n",
                      (i == DICTREMAKE_STATS_VECTLEN-1)?">= ":"",
                      i, clvector[i], ((float)clvector[i]/DICTREMAKE_HT_SIZE(d->ht_size_exp[htidx]))*100);
    }

    /* Unlike snprintf(), return the number of characters actually written. */
    if (bufsize) buf[bufsize-1] = '\0';
    return strlen(buf);
}

void dictRemakeGetStats(char *buf, size_t bufsize, dictRemake *d) {
    size_t l;
    char *orig_buf = buf;
    size_t orig_bufsize = bufsize;

    l = _dictRemakeGetStatsHt(buf,bufsize,d,0);
    buf += l;
    bufsize -= l;
    if (dictRemakeIsRehashing(d) && bufsize > 0) {
        _dictRemakeGetStatsHt(buf,bufsize,d,1);
    }
    /* Make sure there is a NULL term at the end. */
    if (orig_bufsize) orig_buf[orig_bufsize-1] = '\0';
}

/* ------------------------------- Benchmark ---------------------------------*/

#ifdef REDIS_TEST

#define UNUSED(V) ((void) V)

uint64_t hashCallbackRemake(const void *key) {
    return dictRemakeGenHashFunction((unsigned char*)key, strlen((char*)key));
}

int compareCallbackRemake(dictRemake *d, const void *key1, const void *key2) {
    int l1,l2;
    UNUSED(d);

    l1 = strlen((char*)key1);
    l2 = strlen((char*)key2);
    if (l1 != l2) return 0;
    return memcmp(key1, key2, l1) == 0;
}

void freeCallbackRemake(dictRemake *d, void *val) {
    UNUSED(d);

    zfree(val);
}

char *stringFromLongLongRemake(long long value) {
    char buf[32];
    int len;
    char *s;

    len = sprintf(buf,"%lld",value);
    s = zmalloc(len+1);
    memcpy(s, buf, len);
    s[len] = '\0';
    return s;
}

dictRemakeType BenchmarkDictTypeRemake = {
    hashCallbackRemake,
    NULL,
    NULL,
    compareCallbackRemake,
    freeCallbackRemake,
    NULL,
    NULL
};

#define start_benchmark() start = timeInMillisecondsRemake()
#define end_benchmark(msg) do { \
    elapsed = timeInMillisecondsRemake()-start; \
    printf(msg ": %ld items in %lld ms\n", count, elapsed); \
} while(0)

/* ./redis-server test dict_remake [<count> | --accurate] */
int dictRemakeTest(int argc, char **argv, int accurate) {
    long j;
    long long start, elapsed;
    dictRemake *dictRemake = dictRemakeCreate(&BenchmarkDictTypeRemake);
    long count = 0;

    if (argc == 4) {
        if (accurate) {
            count = 5000000;
        } else {
            count = strtol(argv[3],NULL,10);
        }
    } else {
        count = 5000;
    }

    start_benchmark();
    for (j = 0; j < count; j++) {
        int retval = dictRemakeAdd(dictRemake,stringFromLongLongRemake(j),(void*)j);
        assert(retval == DICTREMAKE_OK);
    }
    end_benchmark("Inserting");
    assert((long)dictRemakeSize(dictRemake) == count);

    /* Wait for rehashing. */
    while (dictRemakeIsRehashing(dictRemake)) {
        dictRemakeRehashMilliseconds(dictRemake,100);
    }

    start_benchmark();
    for (j = 0; j < count; j++) {
        char *key = stringFromLongLongRemake(j);
        dictRemakeEntry *de = dictRemakeFind(dictRemake,key);
        assert(de != NULL);
        zfree(key);
    }
    end_benchmark("Linear access of existing elements");

    start_benchmark();
    for (j = 0; j < count; j++) {
        char *key = stringFromLongLongRemake(j);
        dictRemakeEntry *de = dictRemakeFind(dictRemake,key);
        assert(de != NULL);
        zfree(key);
    }
    end_benchmark("Linear access of existing elements (2nd round)");

    start_benchmark();
    for (j = 0; j < count; j++) {
        char *key = stringFromLongLongRemake(rand() % count);
        dictRemakeEntry *de = dictRemakeFind(dictRemake,key);
        assert(de != NULL);
        zfree(key);
    }
    end_benchmark("Random access of existing elements");

    start_benchmark();
    for (j = 0; j < count; j++) {
        dictRemakeEntry *de = dictRemakeGetRandomKey(dictRemake);
        assert(de != NULL);
    }
    end_benchmark("Accessing random keys");

    start_benchmark();
    for (j = 0; j < count; j++) {
        char *key = stringFromLongLongRemake(rand() % count);
        key[0] = 'X';
        dictRemakeEntry *de = dictRemakeFind(dictRemake,key);
        assert(de == NULL);
        zfree(key);
    }
    end_benchmark("Accessing missing");

    start_benchmark();
    for (j = 0; j < count; j++) {
        char *key = stringFromLongLongRemake(j);
        int retval = dictRemakeDelete(dictRemake,key);
        assert(retval == DICTREMAKE_OK);
        key[0] += 17; /* Change first number to letter. */
        retval = dictRemakeAdd(dictRemake,key,(void*)j);
        assert(retval == DICTREMAKE_OK);
    }
    end_benchmark("Removing and adding");
    dictRemakeRelease(dictRemake);
    return 0;
}
#endif