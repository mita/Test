#define _GNU_SOURCE

#include <stdarg.h>
#include <err.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <getopt.h>
#include <limits.h>
#include <sys/time.h>

static void die(const char *err, ...)
{
	va_list params;

	va_start(params, err);
	verrx(EXIT_FAILURE, err, params);
	va_end(params);
}

#include <libmemcached/memcached.h>

static void do_memcached_set(memcached_st *memc, const char *key,
		size_t key_length, const char *value, size_t value_length)
{
	memcached_return ret;

	ret = memcached_set(memc, key, key_length, value, value_length, 0, 0);
	if (ret != MEMCACHED_SUCCESS)
		warnx("memcached_set: %s", memcached_strerror(memc, ret));
}

static void run(const char *server)
{
	struct memcached_st memc;
	struct memcached_server_st *servers;
	memcached_return ret;

	memcached_create(&memc);
	servers = memcached_servers_parse(server);
	ret = memcached_server_push(&memc, servers);

	if (ret != MEMCACHED_SUCCESS)
		die("memcached_server_push: %s", memcached_strerror(&memc, ret));

	do_memcached_set(&memc, "key", 3, "1", 2);
	do_memcached_set(&memc, "key", 3, "2", 2);
	do_memcached_set(&memc, "key", 3, "3", 2);

	const char *keys[] = { "key", };
	const size_t key_length[] = { 3, };

	ret = memcached_mget(&memc, keys, key_length, 1);
	if (ret != MEMCACHED_SUCCESS) {
		warnx("memcached_mget: %s", memcached_strerror(&memc, ret));
	}

	do {
		char *value;
		size_t value_length;
		uint32_t flags;

		value = memcached_fetch(&memc, NULL, NULL, &value_length, &flags, &ret);
		if (ret != MEMCACHED_SUCCESS)
			break;

		printf("value len: %zd\n", value_length);
		free(value);
	} while (1);

	memcached_server_list_free(servers);
	memcached_free(&memc);
}

static char *server = "127.0.0.1:21201";

static void parse_options(int argc, char **argv)
{
	int c;

	while ((c = getopt(argc, argv, "s:")) != -1) {
		switch(c) {
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
	run(server);

	return 0;
}
