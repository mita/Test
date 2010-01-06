#include <err.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <getopt.h>
#include <pthread.h>
#include <sys/time.h>
#include <libmemcached/memcached.h>

static void *xmalloc(size_t size)
{
	void *ptr;

	ptr = malloc(size);
	if (!ptr)
		err(EXIT_FAILURE, "out of memory");

	return ptr;
}

static void xpthread_create(pthread_t *thread, const pthread_attr_t *attr,
				void *(start_routine)(void*), void *arg)
{
	int ret;

	ret = pthread_create(thread, attr, start_routine, arg);
	if (ret)
		err(EXIT_FAILURE, "pthread_create %d", ret);
}

static void do_memcached_set(memcached_st *memc, const char *key,
		size_t key_length, const char *value, size_t value_length)
{
	memcached_return ret;

	ret = memcached_set(memc, key, key_length, value, value_length, 0, 0);
	if (ret != MEMCACHED_SUCCESS) {
		warn("memcached_set: %s", memcached_strerror(memc, ret));
	}
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
	if (ret != MEMCACHED_SUCCESS)
		warn("memcached_get: %s", memcached_strerror(memc, ret));

	free(value);
}

static int tcp_nodelay;

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
		err(EXIT_FAILURE, "memcached_server_push failed");

	if (tcp_nodelay) {
		ret = memcached_behavior_set(&memc, MEMCACHED_BEHAVIOR_TCP_NODELAY, 1);
		if (ret != MEMCACHED_SUCCESS)
			err(EXIT_FAILURE, "memcached_behavior_set failed");
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
			command = 'r';
			break;
		case 'w':
			command = 'w';
			break;
		case 'v':
			verbose = 1;
			break;
		case 'd':
			tcp_nodelay = 1;
			break;
		default:
			err(EXIT_FAILURE, "invalid option: %c", c);
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
			warn("pthread_join");
			continue;
		}
		if (value != NULL)
			warn("thread returns non NULL value %p", value);
	}
}

static void benchmark(void)
{
	int i;
	long sum = 0;
	long avg;
	pthread_t *tid;
	struct benchmark_thread_data *data;

	tid = xmalloc(sizeof(tid[0]) * threads);
	data = xmalloc(sizeof(data[0]) * threads);

	for (i = 0; i < threads; i++) {
		data[i].id = i;
		xpthread_create(&tid[i], NULL, benchmark_thread, &data[i]);
	}
	wait_threads(tid, threads);

	for (i = 0; i < threads; i++)
		sum += data[i].time_ms;

	avg = sum / threads;
	printf("%d %ld.%03ld\n", threads, avg / 1000, avg % 1000);

	if (verbose) {
		unsigned long long total_bytes;
		unsigned long long bytes_per_msec;

		total_bytes = value_length;
		total_bytes *= threads;
		total_bytes *= requests;

		bytes_per_msec = total_bytes / avg;

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
