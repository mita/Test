#include <tcutil.h>
#include <tcrdb.h>
#include <string.h>
#include <stdio.h>
#include <limits.h>
#include "testutil.h"

static TCRDB *open_db(char *host, int port)
{
	TCRDB *rdb;

	rdb = tcrdbnew();

	if (!tcrdbopen(rdb, host, port)){
		int ecode = tcrdbecode(rdb);
		fprintf(stderr, "open error: %s\n", tcrdberrmsg(ecode));
	}
	return rdb;
}

static void close_db(TCRDB *rdb)
{
	if (!tcrdbclose(rdb)){
		int ecode = tcrdbecode(rdb);
		fprintf(stderr, "close error: %s\n", tcrdberrmsg(ecode));
	}
	tcrdbdel(rdb);
}

static char *command = "";
static char *host = "localhost";
static int port = 1978;
static int num = 5000000;
static int vsiz = 100;
static long seed;
static int batch = 1000;
static int thnum = 1;

static void parse_options(int argc, char **argv)
{
	int i;

	for (i = 1; i < argc; i++) {
		if (!strcmp(argv[i], "-command")) {
			command = argv[++i];
		} else if (!strcmp(argv[i], "-host")) {
			host = argv[++i];
		} else if (!strcmp(argv[i], "-port")) {
			port = atoi(argv[++i]);
		} else if (!strcmp(argv[i], "-num")) {
			num = atoi(argv[++i]);
		} else if (!strcmp(argv[i], "-vsiz")) {
			vsiz = atoi(argv[++i]);
		} else if (!strcmp(argv[i], "-seed")) {
			seed = atol(argv[++i]);
		} else if (!strcmp(argv[i], "-batch")) {
			batch = atoi(argv[++i]);
		} else if (!strcmp(argv[i], "-thnum")) {
			thnum = atoi(argv[++i]);
			if (thnum < 1)
				die("Invalid -thnum option");
		} else if (!strcmp(argv[i], "-key")) {
			keygen_set_generator(argv[++i]);
		} else {
			die("Invalid command option");
		}
	}
}

static void put_test(TCRDB *rdb, const char *command, int num, int vsiz,
			long seed)
{
	struct keygen *keygen = keygen_alloc(seed);
	char *value = xmalloc(vsiz);
	int i;

	for (i = 0; i < num; i++) {
		const char *key = keygen_next_key(keygen);

		tcrdbput(rdb, key, strlen(key) + 1, value, vsiz);
	}

	free(value);
	keygen_free(keygen);
}

static void putlist_test(TCRDB *rdb, const char *command, int num, int vsiz,
			long seed)
{
	struct keygen *keygen = keygen_alloc(seed);
	char *value = xmalloc(vsiz);
	TCLIST *list = tclistnew();
	int i;

	for (i = 0; i < num; i++) {
		tclistpush2(list, keygen_next_key(keygen));
		tclistpush(list, value, vsiz);

		if (tclistnum(list) / 2 >= batch) {
			tcrdbmisc(rdb, command, 0, list);
			tclistclear(list);
		}
	}
	tcrdbmisc(rdb, command, 0, list);

	tclistdel(list);
	free(value);
	keygen_free(keygen);
}

static void readlist(TCLIST *recs, int vsiz, int batch)
{
#define PARANOID
#ifdef PARANOID
	int i;
	int recnum = tclistnum(recs);

	if (recnum != batch * 2)
		die("Unexpected list size %d", recnum);

	for (i = 0; i < recnum; i += 2) {
		const void *key;
		const void *val;
		int keysiz;
		int valsiz;

		key = tclistval(recs, i, &keysiz);
		val = tclistval(recs, i + 1, &valsiz);

		if (valsiz != vsiz)
			die("Unexpected value size %d", valsiz);
	}
#endif

	tclistdel(recs);
}

static void getlist_test(TCRDB *rdb, const char *command, int num, int vsiz,
			long seed)
{
	struct keygen *keygen = keygen_alloc(seed);
	TCLIST *list = tclistnew();
	TCLIST *recs;
	int i;

	for (i = 0; i < num; i++) {
		tclistpush2(list, keygen_next_key(keygen));

		if (tclistnum(list) >= batch) {
			recs = tcrdbmisc(rdb, command, 0, list);
			readlist(recs, vsiz, tclistnum(list));
			tclistclear(list);
		}
	}
	recs = tcrdbmisc(rdb, command, 0, list);
	readlist(recs, vsiz, tclistnum(list));

	tclistdel(list);
	keygen_free(keygen);
}

static void outlist_test(TCRDB *rdb, const char *command, int num, long seed)
{
	struct keygen *keygen = keygen_alloc(seed);
	TCLIST *list = tclistnew();
	int i;

	for (i = 0; i < num; i++) {
		tclistpush2(list, keygen_next_key(keygen));

		if (tclistnum(list) >= batch) {
			tcrdbmisc(rdb, command, 0, list);
			tclistclear(list);
		}
	}
	tcrdbmisc(rdb, command, 0, list);

	tclistdel(list);
	keygen_free(keygen);
}

struct thread_data {
	pthread_t tid;
	pthread_barrier_t *barrier;
	TCRDB *rdb;
	long seed;
	unsigned long long elapsed;
};

static void *benchmark_thread(void *arg)
{
	struct thread_data *data = arg;
	unsigned long long start;
	TCRDB *rdb = data->rdb;
	long seed = data->seed;

	pthread_barrier_wait(data->barrier);

	start = stopwatch_start();

	if (!strcmp(command, "putlist") || !strcmp(command, "putlist2")) {
		putlist_test(rdb, command, num, vsiz, seed);
	} else if (!strcmp(command, "getlist") || !strcmp(command, "getlist2")) {
		getlist_test(rdb, command, num, vsiz, seed);
	} else if (!strcmp(command, "outlist") || !strcmp(command, "outlist2")) {
		outlist_test(rdb, command, num, seed);
	} else if (!strcmp(command, "put")) {
		put_test(rdb, command, num, vsiz, seed);
	} else {
		die("Invalid command %s", command);
	}

	data->elapsed = stopwatch_stop(start);

	return NULL;
}

static void benchmark(void)
{
	int i;
	unsigned long long sum = 0, min = ULONG_MAX, max = 0, avg;
	pthread_barrier_t barrier;
	struct thread_data *data;

	pthread_barrier_init(&barrier, NULL, thnum);
	data = xmalloc(sizeof(*data) * thnum);

	for (i = 0; i < thnum; i++) {
		data[i].rdb = open_db(host, port);
		data[i].seed = seed + i;
		data[i].barrier = &barrier;
	}

	for (i = 0; i < thnum; i++)
		xpthread_create(&data[i].tid, benchmark_thread, &data[i]);

	for (i = 0; i < thnum; i++)
		xpthread_join(data[i].tid);

	for (i = 0; i < thnum; i++) {
		unsigned long long elapsed = data[i].elapsed;

		sum += elapsed;
		min = _MIN(min, elapsed);
		max = _MAX(max, elapsed);
	}
	avg = sum / thnum;

	printf("# %lld.%03lld %lld.%03lld %lld.%03lld\n",
			avg / 1000000, avg / 1000 % 1000,
			min / 1000000, min / 1000 % 1000,
			max / 1000000, max / 1000 % 1000);
	fflush(stdout);

	for (i = 0; i < thnum; i++)
		close_db(data[i].rdb);

	free(data);
}

int main(int argc, char **argv)
{
	parse_options(argc, argv);
	benchmark();

	return 0;
}
