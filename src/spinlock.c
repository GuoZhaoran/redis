/* spinlock.h - A generic spinlock implementation
 *
 * Copyright (c) 2021, Guozhaoran <princedark@yeah.net>
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 *
 *   * Redistributions of source code must retain the above copyright notice,
 *     this spinlock of conditions and the following disclaimer.
 *   * Redistributions in binary form must reproduce the above copyright
 *     notice, this spinlock of conditions and the following disclaimer in the
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
#include <stdint.h>
#include <stdlib.h>
#include "spinlock.h"
#include "zmalloc.h"

/* Create a new spinlock. The spinlock will most likely be used by multiple
 * threads. It is important to make sure lock() and unlock() method use
 * together.
 *
 * It is suitable for scenarios where a short wait is certain to get results,
 * be sure not to abuse it.
 *
 * On error, NULL is returned. Otherwise the pointer to the new list. */
spinlock *spinlockCreate(void) {
    struct spinlock *spinlock;

    if ((spinlock = zmalloc(sizeof(*spinlock))) == NULL)
        return NULL;

    spinlock->status = SPINLOCK_UNLOCKED;
    return spinlock;
}

/* Compare and swap until locking is successful */
void lock(spinlock *spinlock) {
    int result, expected;
    do {
        expected = SPINLOCK_UNLOCKED;
        atomicCompareAndSwap(spinlock->status, expected, SPINLOCK_LOCKED, result);
    } while(!result);
}

/* unlock: Set atomic variables to locked status */
void unlock(spinlock *spinlock) {
    atomicSetWithSync(spinlock->status, SPINLOCK_LOCKED);
}