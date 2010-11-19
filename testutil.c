#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <limits.h>
#include <stdarg.h>
#include <err.h>
#include <sys/time.h>
#include "testutil.h"

void die(const char *err, ...)
{
	va_list params;

	va_start(params, err);
	verrx(EXIT_FAILURE, err, params);
	va_end(params);
}

void *xmalloc(size_t size)
{
	void *ptr;

	ptr = malloc(size);
	if (!ptr)
		die("malloc: out of memory");

	return ptr;
}

void xpthread_create(pthread_t *thread, void *(*routine)(void *), void *arg)
{
	int ret;

	ret = pthread_create(thread, NULL, routine, arg);
	if (ret)
		die("pthread_create failed");
}

void xpthread_join(pthread_t th)
{
	int ret;

	ret = pthread_join(th, NULL);
	if (ret)
		die("pthread_join failed");
}

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

char *keygen_next_key(struct keygen *keygen)
{
	unsigned int next = keygen->next(&keygen->seed);

	sprintf(keygen->key, "0x%016llx-0x%016llx",
			(unsigned long long)keygen->prefix,
			(unsigned long long)next);

	return keygen->key;
}

char *keygen_prefix(struct keygen *keygen, char *buf)
{
	sprintf(buf, "0x%016llx-", (unsigned long long)keygen->prefix);

	return buf;
}

static const char *key_generator = "sequence";

void keygen_set_generator(const char *generator)
{
	key_generator = generator;
}

void keygen_init(struct keygen *keygen, unsigned int seed)
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

static void fixup_config(struct benchmark_config *config)
{
	if (config->thnum < 1)
		config->thnum = 1;
	if (config->share < 1)
		config->share = config->thnum;
	if (config->num_works < 1)
		config->num_works = config->thnum;
}

void parse_options(struct benchmark_config *config, int argc, char **argv)
{
	int i;

	for (i = 1; i < argc; i++) {
		if (!strcmp(argv[i], "-command")) {
			config->producer = argv[++i];
		} else if (!strcmp(argv[i], "-producer")) {
			config->producer = argv[++i];
		} else if (!strcmp(argv[i], "-consumer")) {
			config->consumer = argv[++i];
		} else if (!strcmp(argv[i], "-host")) {
			config->host = argv[++i];
		} else if (!strcmp(argv[i], "-port")) {
			config->port = atoi(argv[++i]);
		} else if (!strcmp(argv[i], "-path")) {
			config->path= argv[++i];
		} else if (!strcmp(argv[i], "-num")) {
			config->num = atoi(argv[++i]);
		} else if (!strcmp(argv[i], "-vsiz")) {
			config->vsiz = atoi(argv[++i]);
		} else if (!strcmp(argv[i], "-seed")) {
			config->seed_offset = atoi(argv[++i]);
		} else if (!strcmp(argv[i], "-batch")) {
			config->batch = atoi(argv[++i]);
		} else if (!strcmp(argv[i], "-thnum")) {
			config->thnum = atoi(argv[++i]);
		} else if (!strcmp(argv[i], "-work")) {
			config->num_works = atoi(argv[++i]);
		} else if (!strcmp(argv[i], "-key")) {
			keygen_set_generator(argv[++i]);
		} else if (!strcmp(argv[i], "-debug")) {
			config->debug = true;
		} else if (!strcmp(argv[i], "-share")) {
			config->share = atoi(argv[++i]);
		} else {
			die("Invalid command option: %s", argv[i]);
		}
	}

	fixup_config(config);
}

struct work {
	struct work *next;
	unsigned int seed;
	unsigned long long elapsed;
};

struct work_queue {
	struct work *head;
	pthread_mutex_t mutex;
	pthread_cond_t cond;
	bool open;
};

static void work_queue_open(struct work_queue *queue)
{
	pthread_mutex_lock(&queue->mutex);
	queue->open = true;
	pthread_cond_broadcast(&queue->cond);
	pthread_mutex_unlock(&queue->mutex);
}

static void work_queue_close(struct work_queue *queue)
{
	pthread_mutex_lock(&queue->mutex);
	queue->open = false;
	pthread_cond_broadcast(&queue->cond);
	pthread_mutex_unlock(&queue->mutex);
}

static void work_queue_init(struct work_queue *queue)
{
	queue->head = NULL;
	pthread_mutex_init(&queue->mutex, NULL);
	pthread_cond_init(&queue->cond, NULL);
	work_queue_open(queue);
}

static void work_queue_push(struct work_queue *queue, struct work *work)
{
	pthread_mutex_lock(&queue->mutex);
	if (!queue->open)
		die("work queue is closed");
	work->next = queue->head;
	queue->head = work;
	pthread_cond_signal(&queue->cond);
	pthread_mutex_unlock(&queue->mutex);
}

static struct work *work_queue_pop(struct work_queue *queue)
{
	struct work *work;

	pthread_mutex_lock(&queue->mutex);
	while (1) {
		work = queue->head;
		if (work) {
			queue->head = work->next;
			break;
		} else if (!queue->open) {
			break;
		}
		pthread_cond_wait(&queue->cond, &queue->mutex);
	}
	pthread_mutex_unlock(&queue->mutex);

	return work;
}

struct worker_info {
	pthread_t tid;
	void *db;
	const char *command;
	struct work_queue *in_queue;
	struct work_queue *out_queue;
	struct benchmark_config *config;
};

static void handle_work(struct worker_info *data, struct work *work)
{
	const char *command = data->command;
	struct benchmark_config *config = data->config;
	struct benchmark_operations *bops = &config->ops;
	unsigned long long start;

	start = stopwatch_start();

	if (!strcmp(command, "putlist") || !strcmp(command, "putlist2")) {
		bops->putlist_test(data->db, command, config->num,
				config->vsiz, config->batch, work->seed);
	} else if (!strcmp(command, "fwmkeys")) {
		bops->fwmkeys_test(data->db, config->num, work->seed);
	} else if (!strcmp(command, "range") || !strcmp(command, "range2")) {
		bops->range_test(data->db, command, config->num,
				config->vsiz, config->batch, work->seed);
	} else if (!strcmp(command, "getlist") || !strcmp(command, "getlist2")) {
		bops->getlist_test(data->db, command, config->num,
				config->vsiz, config->batch, work->seed);
	} else if (!strcmp(command, "fwmkeys-getlist")) {
		bops->fwmkeys_test(data->db, config->num, work->seed);
		bops->getlist_test(data->db, "getlist", config->num,
				config->vsiz, config->batch, work->seed);
	} else if (!strcmp(command, "fwmkeys-getlist2")) {
		bops->fwmkeys_test(data->db, config->num, work->seed);
		bops->getlist_test(data->db, "getlist2", config->num,
				config->vsiz, config->batch, work->seed);
	} else if (!strcmp(command, "outlist") || !strcmp(command, "outlist2")) {
		bops->outlist_test(data->db, command, config->num,
					config->batch, work->seed);
	} else if (!strcmp(command, "fwmkeys-outlist")) {
		bops->fwmkeys_test(data->db, config->num, work->seed);
		bops->outlist_test(data->db, "outlist", config->num,
					config->batch, work->seed);
	} else if (!strcmp(command, "fwmkeys-outlist2")) {
		bops->fwmkeys_test(data->db, config->num, work->seed);
		bops->outlist_test(data->db, "outlist2", config->num,
					config->batch, work->seed);
	} else if (!strcmp(command, "put")) {
		bops->put_test(data->db, config->num, config->vsiz, work->seed);
	} else if (!strcmp(command, "get")) {
		bops->get_test(data->db, config->num, config->vsiz, work->seed);
	} else if (!strcmp(command, "nop")) {
		/* nop */
	} else {
		die("Invalid command %s", command);
	}
	work->elapsed += stopwatch_stop(start);
}

static void *benchmark_thread(void *arg)
{
	struct worker_info *data = arg;
	struct work *work;

	while ((work = work_queue_pop(data->in_queue)) != NULL) {
		handle_work(data, work);
		work_queue_push(data->out_queue, work);
	}

	return NULL;
}

static struct worker_info *create_workers(struct benchmark_config *config,
		const char *command, struct work_queue *in_queue,
		struct work_queue *out_queue)
{
	int thnum = config->thnum;
	struct worker_info *data = xmalloc(sizeof(*data) * thnum);
	int i;

	for (i = 0; i < thnum; i++) {
		if ((i % config->share) == i)
			data[i].db = config->ops.open_db(config);
		else
			data[i].db = data[i % config->share].db;

		data[i].config = config;
		data[i].command = command;
		data[i].in_queue = in_queue;
		data[i].out_queue = out_queue;
	}
	for (i = 0; i < thnum; i++)
		xpthread_create(&data[i].tid, benchmark_thread, &data[i]);

	return data;
}

static void join_workers(struct benchmark_config *config,
			struct worker_info *data)
{
	int i;

	for (i = 0; i < config->thnum; i++)
		xpthread_join(data[i].tid);
}

static void destroy_workers(struct worker_info *data)
{
	struct benchmark_config *config = data[0].config;
	int i;

	for (i = 0; i < config->thnum; i++) {
		if ((i % config->share) == i)
			config->ops.close_db(data[i].db);
	}
	free(data);
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

void benchmark(struct benchmark_config *config)
{
	int i;
	unsigned long long sum = 0, min = ULONG_MAX, max = 0, avg;
	struct worker_info *producers;
	struct worker_info *consumers;
	struct work_queue queue_to_producer;
	struct work_queue queue_to_consumer;
	struct work_queue trash_queue;

	work_queue_init(&queue_to_producer);
	work_queue_init(&queue_to_consumer);
	work_queue_init(&trash_queue);

	producers = create_workers(config, config->producer, &queue_to_producer,
				&queue_to_consumer);
	consumers = create_workers(config, config->consumer, &queue_to_consumer,
				&trash_queue);

	for (i = 0; i < config->num_works; i++) {
		struct work *work = xmalloc(sizeof(*work));

		work->seed = config->seed_offset + i;
		work->elapsed = 0;
		work_queue_push(&queue_to_producer, work);
	}
	work_queue_close(&queue_to_producer);

	join_workers(config, producers);
	work_queue_close(&queue_to_consumer);

	join_workers(config, consumers);
	work_queue_close(&trash_queue);

	for (i = 0; i < config->num_works; i++) {
		struct work *work = work_queue_pop(&trash_queue);
		unsigned long long elapsed = work->elapsed;

		sum += elapsed;
		min = _MIN(min, elapsed);
		max = _MAX(max, elapsed);
		free(work);
	}
	avg = sum / config->num_works;

	printf("# %lld.%03lld %lld.%03lld %lld.%03lld\n",
			avg / 1000000, avg / 1000 % 1000,
			min / 1000000, min / 1000 % 1000,
			max / 1000000, max / 1000 % 1000);
	fflush(stdout);

	destroy_workers(consumers);
	destroy_workers(producers);
}
