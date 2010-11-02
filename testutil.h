#include <stdlib.h>
#include <stdarg.h>
#include <err.h>
#include <pthread.h>
#include <sys/time.h>

static void die(const char *err, ...)
{
	va_list params;

	va_start(params, err);
	verrx(EXIT_FAILURE, err, params);
	va_end(params);
}

static void *xmalloc(size_t size)
{
	void *ptr;

	ptr = malloc(size);
	if (!ptr)
		die("malloc: out of memory");

	return ptr;
}

static void xpthread_create(pthread_t *thread, void *(*routine)(void *),
			void *arg)
{
	int ret;

	ret = pthread_create(thread, NULL, routine, arg);
	if (ret)
		die("pthread_create failed");
}

static void xpthread_join(pthread_t th)
{
	int ret;

	ret = pthread_join(th, NULL);
	if (ret)
		die("pthread_join failed");
}

#define KEYGEN_KEY_SIZE sizeof("0x0000000000000000-0x0000000000000000")
#define KEYGEN_PREFIX_SIZE (sizeof("0x0000000000000000-") - 1)

struct keygen {
	unsigned int prefix;
	char key[KEYGEN_KEY_SIZE];
	unsigned int (*next)(unsigned int *state);
	unsigned int seed;
};

static unsigned int keygen_sequence_next(unsigned int *seed)
{
	return (*seed)++;
}

static void keygen_sequence_init(struct keygen *keygen, unsigned int seed)
{
	keygen->next = keygen_sequence_next;
	keygen->seed = 0;
}

static unsigned int keygen_random_next(unsigned int *seed)
{
	return (unsigned int) rand_r(seed);
}

static void keygen_random_init(struct keygen *keygen, unsigned int seed)
{
	keygen->next = keygen_random_next;
	keygen->seed = seed;
}

static char *keygen_next_key(struct keygen *keygen)
{
	unsigned int next = keygen->next(&keygen->seed);

	sprintf(keygen->key, "0x%016llx-0x%016llx",
			(unsigned long long)keygen->prefix,
			(unsigned long long)next);

	return keygen->key;
}

static inline char *keygen_prefix(struct keygen *keygen, char *buf)
{
	sprintf(buf, "0x%016llx-", (unsigned long long)keygen->prefix);

	return buf;
}

static const char *key_generator = "sequence";

static void keygen_set_generator(const char *generator)
{
	key_generator = generator;
}

static void keygen_init(struct keygen *keygen, unsigned int seed)
{
	keygen->prefix = seed;

	if (!strcmp(key_generator, "random"))
		keygen_random_init(keygen, seed);
	else
		keygen_sequence_init(keygen, seed);
}

static unsigned long long tv_to_us(const struct timeval *tv)
{
	unsigned long long us = tv->tv_usec;

	return us + tv->tv_sec * 1000000UL;
}

static unsigned long long stopwatch_start()
{
	struct timeval tv;

	gettimeofday(&tv, NULL);

	return tv_to_us(&tv);
}

static unsigned long long stopwatch_stop(unsigned long long start)
{
	struct timeval tv;

	gettimeofday(&tv, NULL);

	return tv_to_us(&tv) - start;
}

#define _MIN(x, y) ({				\
	typeof(x) _min1 = (x);			\
	typeof(y) _min2 = (y);			\
	(void) (&_min1 == &_min2);		\
	_min1 < _min2 ? _min1 : _min2; })

#define _MAX(x, y) ({				\
	typeof(x) _max1 = (x);			\
	typeof(y) _max2 = (y);			\
	(void) (&_max1 == &_max2);		\
	_max1 > _max2 ? _max1 : _max2; })
