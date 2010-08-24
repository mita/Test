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

struct keygen {
	long seed;
	char *(*next_key)(struct keygen *keygen);
	void *data;
};

struct keygen_sequence {
	char key[sizeof("0x0000000000000000-0x0000000000000000")];
	unsigned long long counter;
};

static char *keygen_sequence_next_key(struct keygen *keygen)
{
	struct keygen_sequence *keygen_data = keygen->data;

	sprintf(keygen_data->key, "0x%016llx-0x%016llx",
			(unsigned long long) keygen->seed,
			keygen_data->counter++);

	return keygen_data->key;
}

static void keygen_sequence_init(struct keygen *keygen, long seed)
{
	struct keygen_sequence *keygen_data = xmalloc(sizeof(*keygen_data));

	keygen->seed = seed;
	keygen->next_key = keygen_sequence_next_key;
	keygen->data = keygen_data;

	keygen_data->counter = 0;
}

struct keygen_random {
	char key[sizeof("0x0000000000000000-0x0000000000000000")];
	unsigned int seed;
};

static char *keygen_random_next_key(struct keygen *keygen)
{
	struct keygen_random *keygen_data = keygen->data;
	unsigned long long random;

	random = rand_r(&keygen_data->seed);
	random *= RAND_MAX;
	random += rand_r(&keygen_data->seed);

	sprintf(keygen_data->key, "0x%016llx-0x%016llx",
			(unsigned long long)keygen->seed, random);

	return keygen_data->key;
}

static void keygen_random_init(struct keygen *keygen, long seed)
{
	struct keygen_random *keygen_data = xmalloc(sizeof(*keygen_data));

	keygen->seed = seed;
	keygen->next_key = keygen_random_next_key;
	keygen->data = keygen_data;

	keygen_data->seed = seed;
}

static char *keygen_next_key(struct keygen *keygen)
{
	return keygen->next_key(keygen);
}

static const char *key_generator = "sequence";

static void keygen_set_generator(const char *generator)
{
	key_generator = generator;
}

static void *keygen_alloc(long seed)
{
	struct keygen *keygen = xmalloc(sizeof(*keygen));

	if (!strcmp(key_generator, "random"))
		keygen_random_init(keygen, seed);
	else
		keygen_sequence_init(keygen, seed);

	return keygen;
}

static void keygen_free(struct keygen *keygen)
{
	free(keygen->data);
	free(keygen);
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
