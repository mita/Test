#define _GNU_SOURCE

#include <stdarg.h>
#include <err.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <getopt.h>
#include <pthread.h>
#include <limits.h>
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

static void xpthread_create(pthread_t *thread, const pthread_attr_t *attr,
				void *(start_routine)(void*), void *arg)
{
	int ret;

	ret = pthread_create(thread, attr, start_routine, arg);
	if (ret)
		die("pthread_create: %d", ret);
}

static int tcp_nodelay;

#ifdef CHUNKD_BENCHMARK

#include <chunkc.h>

static void do_chunkd_set(struct st_client *stc, const char *key,
		size_t key_length, char *value, size_t value_length)
{
	bool ret;

	ret = stc_put_inline(stc, key, key_length, value, value_length, 0);
	if (!ret)
		warnx("stc_put failed");
}

static void do_chunkd_get(struct st_client *stc, const char *key,
				size_t key_length)
{
	char *value;
	size_t value_length;

	value = stc_get_inline(stc, key, key_length, &value_length);
	if (!value) {
		warnx("stc_get failed");
		return;
	}

	free(value);
}

static struct st_client *stc_new2(const char *server)
{
	char *host;
	char *port;

	host = strdupa(server);

	host = strtok(host, ":");
	if (!host)
		return NULL;

	port = strtok(NULL, ":");
	if (!port)
		return NULL;

	return stc_new(host, atoi(port), "testuser", "testuser", false);
}

static void run(const int id, const char *server, const int value_length,
			const int requests, const int command)
{
	char key[20];
	char *value;
	int i;
	struct st_client *stc;
	bool ret;

	value = xmalloc(value_length);
	memset(value, '*', value_length);

	stc = stc_new2(server);
	if (!stc)
		die("stc_new failed");

	ret = stc_table_openz(stc, "testtable",
			command == 'w' ? CHF_TBL_CREAT : 0);
	if (!ret)
		die("stc_table_openz failed");

	for (i =  0; i < requests; i++) {
		sprintf(key, "%04d-%011d", id, i);

		if (command == 'w')
			do_chunkd_set(stc, key, 16, value, value_length);
		else
			do_chunkd_get(stc, key, 16);
	}
	stc_free(stc);
	free(value);
}

#else /* CHUNKD_BENCHMARK */

#include <libmemcached/memcached.h>

static void do_memcached_set(memcached_st *memc, const char *key,
		size_t key_length, const char *value, size_t value_length)
{
	memcached_return ret;

	ret = memcached_set(memc, key, key_length, value, value_length, 0, 0);
	if (ret != MEMCACHED_SUCCESS)
		warnx("memcached_set: %s", memcached_strerror(memc, ret));
}

static void do_memcached_get(memcached_st *memc, const char *key,
				size_t key_length)
{
	memcached_return ret;
	char *value;
	size_t value_length;
	uint32_t flags;

	value = memcached_get(memc, key, key_length, &value_length,
				&flags, &ret);
	if (ret != MEMCACHED_SUCCESS) {
		warnx("memcached_get: %s", memcached_strerror(memc, ret));
		return;
	}

	free(value);
}

static void run(const int id, const char *server, const int value_length,
			const int requests, const int command)
{
	char key[20];
	char *value;
	struct memcached_st memc;
	struct memcached_server_st *servers;
	memcached_return ret;
	int i;

	value = xmalloc(value_length);
	memset(value, '*', value_length);

	memcached_create(&memc);
	servers = memcached_servers_parse(server);
	ret = memcached_server_push(&memc, servers);

	if (ret != MEMCACHED_SUCCESS)
		die("memcached_server_push: %d", ret);

	if (tcp_nodelay) {
		ret = memcached_behavior_set(&memc,
					MEMCACHED_BEHAVIOR_TCP_NODELAY, 1);
		if (ret != MEMCACHED_SUCCESS)
			die("memcached_behavior_set: %d", ret);
	}

	for (i =  0; i < requests; i++) {
		sprintf(key, "%04d-%011d", id, i);

		if (command == 'w')
			do_memcached_set(&memc, key, 16, value, value_length);
		else
			do_memcached_get(&memc, key, 16);
	}

	memcached_server_list_free(servers);
	memcached_free(&memc);

	free(value);
}

#endif /* CHUNKD_BENCHMARK */

static unsigned long value_length = 100UL;
static unsigned long requests = 100000UL;
static char *server = "127.0.0.1:21201";
/* 'r' for Read, 'w' for Write request */
static int command = 'w';
static unsigned int threads = 8;
static int verbose;

static void parse_options(int argc, char **argv)
{
	int c;

	while ((c = getopt(argc, argv, "n:l:s:t:rwvd")) != -1) {
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
		case 't':
			threads = atoi(optarg);
			break;
		case 'r':
		case 'w':
			command = c;
			break;
		case 'v':
			verbose = 1;
			break;
		case 'd':
			tcp_nodelay = 1;
			break;
		default:
			die("invalid option: %c", c);
		}
	}
}

struct benchmark_thread_data {
	int id;
	long time_ms;
};

static long time_diff(struct timeval a, struct timeval b)
{
	int us, s;

	us = (int)(a.tv_usec - b.tv_usec);
	s = (int)(a.tv_sec - b.tv_sec);

	return s * 1000 + us / 1000;
}

static void *benchmark_thread(void *arg)
{
	struct benchmark_thread_data *data = arg;
	struct timeval start, end;

	gettimeofday(&start, NULL);
	run(data->id, server, value_length, requests, command);
	gettimeofday(&end, NULL);
	data->time_ms = time_diff(end, start);

	return NULL;
}

static void wait_threads(pthread_t *threads, int n)
{
	int i;

	for (i = 0; i < n; i++) {
		void *value;
		int ret;

		ret = pthread_join(threads[i], &value);
		if (ret) {
			warnx("pthread_join: %d", ret);
			continue;
		}
		if (value != NULL)
			warnx("thread returns non NULL value %p", value);
	}
}

static void benchmark(void)
{
	int i;
	long sum = 0;
	long min_ms = LONG_MAX, max_ms = 0, avg_ms;
	pthread_t *tid;
	struct benchmark_thread_data *data;

#ifdef CHUNKD_BENCHMARK
	stc_init();
#endif

	tid = xmalloc(sizeof(tid[0]) * threads);
	data = xmalloc(sizeof(data[0]) * threads);

	for (i = 0; i < threads; i++) {
		data[i].id = i;
		xpthread_create(&tid[i], NULL, benchmark_thread, &data[i]);
	}
	wait_threads(tid, threads);

#define _MIN(a, b) ((a) < (b) ? (a) : (b))
#define _MAX(a, b) ((a) < (b) ? (b) : (a))

	for (i = 0; i < threads; i++) {
		long ms = data[i].time_ms;

		sum += ms;
		min_ms = _MIN(min_ms, ms);
		max_ms = _MAX(max_ms, ms);
	}

	avg_ms = sum / threads;

	printf("%d %ld.%03ld %ld.%03ld %ld.%03ld\n", threads,
			avg_ms / 1000, avg_ms % 1000,
			min_ms / 1000, min_ms % 1000,
			max_ms / 1000, max_ms % 1000);

	if (verbose) {
		unsigned long long total_bytes;
		unsigned long long bytes_per_msec;

		total_bytes = value_length;
		total_bytes *= threads;
		total_bytes *= requests;

		bytes_per_msec = total_bytes / avg_ms;

		printf("Throughput: %llu KB/sec\n",
				bytes_per_msec * 1000UL / 1024UL);
	}

	free(data);
	free(tid);
}

int main(int argc, char **argv)
{
	parse_options(argc, argv);
	benchmark();

	return 0;
}
