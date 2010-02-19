#define _GNU_SOURCE

#include <stdarg.h>
#include <err.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <getopt.h>
#include <limits.h>
#include <sys/time.h>
#include <libmemcached/memcached.h>

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


static void do_memcached_set(memcached_st *memc, const char *key,
		size_t key_length, const char *value, size_t value_length)
{
	memcached_return ret;

	ret = memcached_set(memc, key, key_length, value, value_length, 0, 0);
	if (ret != MEMCACHED_SUCCESS)
		warnx("memcached_set: %s", memcached_strerror(memc, ret));
}

/*
 * delete, set, mget
 */
static void run(const char *server, unsigned long requests,
		unsigned long value_length)
{
	struct memcached_st memc;
	struct memcached_server_st *servers;
	const char *key = "key";
	const size_t key_len = strlen(key);
	char *value = xmalloc(value_length);
	memcached_return ret;
	int i;

	memcached_create(&memc);
	servers = memcached_servers_parse(server);
	ret = memcached_server_push(&memc, servers);

	if (ret != MEMCACHED_SUCCESS)
		die("memcached_server_push: %s", memcached_strerror(&memc, ret));


	ret = memcached_delete(&memc, key, key_len, 0);
	if (ret != MEMCACHED_SUCCESS)
		warnx("memcached_delete: %s", memcached_strerror(&memc, ret));

	for (i = 0; i < requests; i++)
		do_memcached_set(&memc, key, key_len, value, value_length);

	const char *keys[] = { key, };
	const size_t key_lens[] = { key_len, };

	ret = memcached_mget(&memc, keys, key_lens, 1);
	if (ret != MEMCACHED_SUCCESS)
		warnx("memcached_mget: %s", memcached_strerror(&memc, ret));

	char *v;
	size_t length;
	uint32_t flags;
	for (i = 0; i < requests; i++) {
		v = memcached_fetch(&memc, NULL, NULL, &length, &flags, &ret);
		if (ret != MEMCACHED_SUCCESS)
			break;

		if (value_length != length)
			warnx("unexpected value length: %zd\n", length);

		free(v);
	}

	if (i != requests)
		warnx("%zd records lost", requests - i);

	v = memcached_fetch(&memc, NULL, NULL, &length, &flags, &ret);
	if (ret == MEMCACHED_SUCCESS)
		warnx("unexpected record");
	else if (ret != MEMCACHED_END)
		warnx("unexpected error: %s", memcached_strerror(&memc, ret));

	memcached_server_list_free(servers);
	memcached_free(&memc);
}

static unsigned long value_length = 100UL;
static unsigned long requests = 10000UL;
static char *server = "127.0.0.1:21201";

static void parse_options(int argc, char **argv)
{
	int c;

	while ((c = getopt(argc, argv, "n:l:s:")) != -1) {
		switch(c) {
		case 'n':
			requests = atol(optarg);
			break;
		case 'l':
			value_length = atol(optarg);
			break;
		case 's':
			server = optarg;
			break;
		default:
			die("invalid option: %c", c);
		}
	}
}

int main(int argc, char **argv)
{
	parse_options(argc, argv);
	run(server, requests, value_length);

	return 0;
}
