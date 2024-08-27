/*
 * Copyright (c) 2024-2024, yanruibinghxu@gmail.com All rights reserved.
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
#include "cls_fmacros.h"
#include "cls_config.h"
#include "cls_solarisfixes.h"

#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include <unistd.h>

#ifdef __linux__
#include <sys/mman.h>
#endif

/* This function provide us access to the original libc free(). This is useful
 * for instance to free results obtained by backtrace_symbols(). We need
 * to define this function before including zmalloc.h that may shadow the
 * free implementation if we use jemalloc or another non standard allocator. */
void zlibc_free(void *ptr) {
    free(ptr);
}

#include <string.h>
#include "cls_malloc.h"
#include "cls_atomicvar.h"

#define UNUSED(x) ((void)(x))

#ifdef HAVE_MALLOC_SIZE
#define PREFIX_SIZE (0)
#else
/* Use at least 8 bytes alignment on all systems. */
#if SIZE_MAX < 0xffffffffffffffffull
#define PREFIX_SIZE 8
#else
#define PREFIX_SIZE (sizeof(size_t))
#endif
#endif

/* When using the libc allocator, use a minimum allocation size to match the
 * jemalloc behavior that doesn't return NULL in this case.
 */
#define MALLOC_MIN_SIZE(x) ((x) > 0 ? (x) : sizeof(long))
#define update_zmalloc_stat_alloc(__n) atomicIncr(used_memory,(__n))
#define update_zmalloc_stat_free(__n) atomicDecr(used_memory,(__n))

static clsAtomic size_t used_memory = 0;

static void zmalloc_default_oom(size_t size) {
    fprintf(stderr, "zmalloc: Out of memory trying to allocate %zu bytes\n",
        size);
    fflush(stderr);
    abort();
}

static void (*zmalloc_oom_handler)(size_t) = zmalloc_default_oom;

#ifdef HAVE_MALLOC_SIZE
void *extend_to_usable(void *ptr, size_t size) {
    UNUSED(size);
    return ptr;
}
#endif

/* Try allocating memory, and return NULL if failed.
 * '*usable' is set to the usable size if non NULL. */
static inline void *ztrymalloc_usable_internal(size_t size, size_t *usable) {
    /* Possible overflow, return NULL, so that the caller can panic or handle a failed allocation. */
    if (size >= SIZE_MAX/2) return NULL;
    void *ptr = malloc(MALLOC_MIN_SIZE(size)+PREFIX_SIZE);
    if (!ptr) return NULL;

#ifdef HAVE_MALLOC_SIZE
    size = zmalloc_size(ptr);
    update_zmalloc_stat_alloc(size);
    if (usable) *usable = size;
    return ptr;
#else
    size = MALLOC_MIN_SIZE(size);
    *((size_t*)ptr) = size;
    update_zmalloc_stat_alloc(size+PREFIX_SIZE);
    if (usable) *usable = size;
    return (char*)ptr+PREFIX_SIZE;
#endif
}

void *ztrymalloc_usable(size_t size, size_t *usable) {
    size_t usable_size = 0;
    void *ptr = ztrymalloc_usable_internal(size, &usable_size);
#ifdef HAVE_MALLOC_SIZE
    ptr = extend_to_usable(ptr, usable_size);
#endif
    if (usable) *usable = usable_size;
    return ptr;
}

/* Allocate memory or panic */
void *zmalloc(size_t size) {
    void *ptr = ztrymalloc_usable_internal(size, NULL);
    if (!ptr) zmalloc_oom_handler(size);
    return ptr;
}

/* Try allocating memory, and return NULL if failed. */
void *ztrymalloc(size_t size) {
    void *ptr = ztrymalloc_usable_internal(size, NULL);
    return ptr;
}

/* Allocate memory or panic.
 * '*usable' is set to the usable size if non NULL. */
void *zmalloc_usable(size_t size, size_t *usable) {
    size_t usable_size = 0;
    void *ptr = ztrymalloc_usable_internal(size, &usable_size);
    if (!ptr) zmalloc_oom_handler(size);
#ifdef HAVE_MALLOC_SIZE
    ptr = extend_to_usable(ptr, usable_size);
#endif
    if (usable) *usable = usable_size;
    return ptr;
}

/* Try allocating memory and zero it, and return NULL if failed.
 * '*usable' is set to the usable size if non NULL. */
static inline void *ztrycalloc_usable_internal(size_t size, size_t *usable) {
    /* Possible overflow, return NULL, so that the caller can panic or handle a failed allocation. */
    if (size >= SIZE_MAX/2) return NULL;
    void *ptr = calloc(1, MALLOC_MIN_SIZE(size)+PREFIX_SIZE);
    if (ptr == NULL) return NULL;

#ifdef HAVE_MALLOC_SIZE
    size = zmalloc_size(ptr);
    update_zmalloc_stat_alloc(size);
    if (usable) *usable = size;
    return ptr;
#else
    size = MALLOC_MIN_SIZE(size);
    *((size_t*)ptr) = size;
    update_zmalloc_stat_alloc(size+PREFIX_SIZE);
    if (usable) *usable = size;
    return (char*)ptr+PREFIX_SIZE;
#endif
}

void *ztrycalloc_usable(size_t size, size_t *usable) {
    size_t usable_size = 0;
    void *ptr = ztrycalloc_usable_internal(size, &usable_size);
#ifdef HAVE_MALLOC_SIZE
    ptr = extend_to_usable(ptr, usable_size);
#endif
    if (usable) *usable = usable_size;
    return ptr;
}

/* Allocate memory and zero it or panic.
 * We need this wrapper to have a calloc compatible signature */
void *zcalloc_num(size_t num, size_t size) {
    /* Ensure that the arguments to calloc(), when multiplied, do not wrap.
     * Division operations are susceptible to divide-by-zero errors so we also check it. */
    if ((size == 0) || (num > SIZE_MAX/size)) {
        zmalloc_oom_handler(SIZE_MAX);
        return NULL;
    }
    void *ptr = ztrycalloc_usable_internal(num*size, NULL);
    if (!ptr) zmalloc_oom_handler(num*size);
    return ptr;
}

/* Allocate memory and zero it or panic */
void *zcalloc(size_t size) {
    void *ptr = ztrycalloc_usable_internal(size, NULL);
    if (!ptr) zmalloc_oom_handler(size);
    return ptr;
}

/* Try allocating memory, and return NULL if failed. */
void *ztrycalloc(size_t size) {
    void *ptr = ztrycalloc_usable_internal(size, NULL);
    return ptr;
}

/* Allocate memory or panic.
 * '*usable' is set to the usable size if non NULL. */
void *zcalloc_usable(size_t size, size_t *usable) {
    size_t usable_size = 0;
    void *ptr = ztrycalloc_usable_internal(size, &usable_size);
    if (!ptr) zmalloc_oom_handler(size);
#ifdef HAVE_MALLOC_SIZE
    ptr = extend_to_usable(ptr, usable_size);
#endif
    if (usable) *usable = usable_size;
    return ptr;
}

/* Try reallocating memory, and return NULL if failed.
 * '*usable' is set to the usable size if non NULL. */
static inline void *ztryrealloc_usable_internal(void *ptr, size_t size, size_t *usable) {
#ifndef HAVE_MALLOC_SIZE
    void *realptr;
#endif
    size_t oldsize;
    void *newptr;

    /* not allocating anything, just redirect to free. */
    if (size == 0 && ptr != NULL) {
        zfree(ptr);
        if (usable) *usable = 0;
        return NULL;
    }
    /* Not freeing anything, just redirect to malloc. */
    if (ptr == NULL)
        return ztrymalloc_usable(size, usable);

    /* Possible overflow, return NULL, so that the caller can panic or handle a failed allocation. */
    if (size >= SIZE_MAX/2) {
        zfree(ptr);
        if (usable) *usable = 0;
        return NULL;
    }

#ifdef HAVE_MALLOC_SIZE
    oldsize = zmalloc_size(ptr);
    newptr = realloc(ptr,size);
    if (newptr == NULL) {
        if (usable) *usable = 0;
        return NULL;
    }

    update_zmalloc_stat_free(oldsize);
    size = zmalloc_size(newptr);
    update_zmalloc_stat_alloc(size);
    if (usable) *usable = size;
    return newptr;
#else
    realptr = (char*)ptr-PREFIX_SIZE;
    oldsize = *((size_t*)realptr);
    newptr = realloc(realptr,size+PREFIX_SIZE);
    if (newptr == NULL) {
        if (usable) *usable = 0;
        return NULL;
    }

    *((size_t*)newptr) = size;
    update_zmalloc_stat_free(oldsize);
    update_zmalloc_stat_alloc(size);
    if (usable) *usable = size;
    return (char*)newptr+PREFIX_SIZE;
#endif
}

void *ztryrealloc_usable(void *ptr, size_t size, size_t *usable) {
    size_t usable_size = 0;
    ptr = ztryrealloc_usable_internal(ptr, size, &usable_size);
#ifdef HAVE_MALLOC_SIZE
    ptr = extend_to_usable(ptr, usable_size);
#endif
    if (usable) *usable = usable_size;
    return ptr;
}

/* Reallocate memory and zero it or panic */
void *zrealloc(void *ptr, size_t size) {
    ptr = ztryrealloc_usable_internal(ptr, size, NULL);
    if (!ptr && size != 0) zmalloc_oom_handler(size);
    return ptr;
}

/* Try Reallocating memory, and return NULL if failed. */
void *ztryrealloc(void *ptr, size_t size) {
    ptr = ztryrealloc_usable_internal(ptr, size, NULL);
    return ptr;
}

/* Reallocate memory or panic.
 * '*usable' is set to the usable size if non NULL. */
void *zrealloc_usable(void *ptr, size_t size, size_t *usable) {
    size_t usable_size = 0;
    ptr = ztryrealloc_usable(ptr, size, &usable_size);
    if (!ptr && size != 0) zmalloc_oom_handler(size);
#ifdef HAVE_MALLOC_SIZE
    ptr = extend_to_usable(ptr, usable_size);
#endif
    if (usable) *usable = usable_size;
    return ptr;
}

/* Provide zmalloc_size() for systems where this function is not provided by
 * malloc itself, given that in that case we store a header with this
 * information as the first bytes of every allocation. */
#ifndef HAVE_MALLOC_SIZE
size_t zmalloc_size(void *ptr) {
    void *realptr = (char*)ptr-PREFIX_SIZE;
    size_t size = *((size_t*)realptr);
    return size+PREFIX_SIZE;
}
size_t zmalloc_usable_size(void *ptr) {
    return zmalloc_size(ptr)-PREFIX_SIZE;
}
#endif

void zfree(void *ptr) {
#ifndef HAVE_MALLOC_SIZE
    void *realptr;
    size_t oldsize
#endif

    if (ptr == NULL) return;
#ifdef HAVE_MALLOC_SIZE
    update_zmalloc_stat_free(zmalloc_size(ptr));
    free(ptr);
#else
    realptr = (char*)ptr-PREFIX_SIZE;
    oldsize = *((size_t*)realptr);
    update_zmalloc_stat_free(oldsize+PREFIX_SIZE);
    free(realptr);
#endif
}

/* Similar to zfree, '*usable' is set to the usable size being freed. */
void zfree_usable(void *ptr, size_t *usable) {
#ifndef HAVE_MALLOC_SIZE
    void *realptr;
    size_t oldsize;
#endif

    if (ptr == NULL) return;
#ifdef HAVE_MALLOC_SIZE
    update_zmalloc_stat_free(*usable = zmalloc_size(ptr));
    free(ptr);
#else
    realptr = (char*)ptr-PREFIX_SIZE;
    *usable = oldsize = *((size_t*)realptr);
    update_zmalloc_stat_free(oldsize+PREFIX_SIZE);
    free(realptr);
#endif
}

char *zstrdup(const char *s) {
    size_t l = strlen(s)+1;
    char *p = zmalloc(l);

    memcpy(p, s, l);
    return p;
}

size_t zmalloc_used_memory(void) {
    size_t um;
    atomicGet(used_memory,um);
    return um;
}

void zmalloc_set_oom_handler(void (*oom_handler)(size_t)) {
    zmalloc_oom_handler = oom_handler;
}

/* Returns the size of physical memory (RAM) in bytes.
 * It looks ugly, but this is the cleanest way to achieve cross platform results.
 * Cleaned up from:
 *
 * http://nadeausoftware.com/articles/2012/09/c_c_tip_how_get_physical_memory_size_system
 *
 * Note that this function:
 * 1) Was released under the following CC attribution license:
 *    http://creativecommons.org/licenses/by/3.0/deed.en_US.
 * 2) Was originally implemented by David Robert Nadeau.
 * 3) Was modified for Redis by Matt Stancliff.
 * 4) This note exists in order to comply with the original license.
 */
size_t zmalloc_get_memory_size(void) {
#if defined(__unix__) || defined(__unix) || defined(unix) || \
    (defined(__APPLE__) && defined(__MACH__))
#if defined(CTL_HW) && (defined(HW_MEMSIZE) || defined(HW_PHYSMEM64))
    int mib[2];
    mib[0] = CTL_HW;
#if defined(HW_MEMSIZE)
    mib[1] = HW_MEMSIZE;            /* OSX. --------------------- */
#elif defined(HW_PHYSMEM64)
    mib[1] = HW_PHYSMEM64;          /* NetBSD, OpenBSD. --------- */
#endif
    int64_t size = 0;               /* 64-bit */
    size_t len = sizeof(size);
    if (sysctl( mib, 2, &size, &len, NULL, 0) == 0)
        return (size_t)size;
    return 0L;          /* Failed? */

#elif defined(_SC_PHYS_PAGES) && defined(_SC_PAGESIZE)
    /* FreeBSD, Linux, OpenBSD, and Solaris. -------------------- */
    return (size_t)sysconf(_SC_PHYS_PAGES) * (size_t)sysconf(_SC_PAGESIZE);

#elif defined(CTL_HW) && (defined(HW_PHYSMEM) || defined(HW_REALMEM))
    /* DragonFly BSD, FreeBSD, NetBSD, OpenBSD, and OSX. -------- */
    int mib[2];
    mib[0] = CTL_HW;
#if defined(HW_REALMEM)
    mib[1] = HW_REALMEM;        /* FreeBSD. ----------------- */
#elif defined(HW_PHYSMEM)
    mib[1] = HW_PHYSMEM;        /* Others. ------------------ */
#endif
    unsigned int size = 0;      /* 32-bit */
    size_t len = sizeof(size);
    if (sysctl(mib, 2, &size, &len, NULL, 0) == 0)
        return (size_t)size;
    return 0L;          /* Failed? */
#else
    return 0L;          /* Unknown method to get the data. */
#endif
#else
    return 0L;          /* Unknown OS. */
#endif
}


