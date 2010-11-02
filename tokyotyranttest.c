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
		die("open error: %s", tcrdberrmsg(ecode));
	}
	return rdb;
}

static void close_db(TCRDB *rdb)
{
	if (!tcrdbclose(rdb)){
		int ecode = tcrdbecode(rdb);
		die("close error: %s", tcrdberrmsg(ecode));
	}
	tcrdbdel(rdb);
}

static char *command = "";
static char *host = "localhost";
static int port = 1978;
static int num = 5000000;
static int vsiz = 100;
static unsigned int seed;
static int batch = 1000;
static int thnum = 1;
static bool debug = false;
static int share;

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
			seed = atoi(argv[++i]);
		} else if (!strcmp(argv[i], "-batch")) {
			batch = atoi(argv[++i]);
		} else if (!strcmp(argv[i], "-thnum")) {
			thnum = atoi(argv[++i]);
			if (thnum < 1)
				die("Invalid -thnum option");
		} else if (!strcmp(argv[i], "-key")) {
			keygen_set_generator(argv[++i]);
		} else if (!strcmp(argv[i], "-debug")) {
			debug = true;
		} else if (!strcmp(argv[i], "-share")) {
			share = atoi(argv[++i]);
		} else {
			die("Invalid command option: %s", argv[i]);
		}
	}
}

static void put_test(TCRDB *rdb, int num, int vsiz, unsigned int seed)
{
	struct keygen keygen;
	char *value = xmalloc(vsiz);
	int i;

	keygen_init(&keygen, seed);

	for (i = 0; i < num; i++) {
		const char *key = keygen_next_key(&keygen);

		tcrdbput(rdb, key, strlen(key), value, vsiz);
	}

	free(value);
}

static void get_test(TCRDB *rdb, int num, int vsiz, unsigned int seed)
{
	struct keygen keygen;
	int i;

	keygen_init(&keygen, seed);

	for (i = 0; i < num; i++) {
		const char *key = keygen_next_key(&keygen);
		void *value;
		int siz;

		value = tcrdbget(rdb, key, strlen(key), &siz);
		if (debug && vsiz != siz)
			die("Unexpected value size");
			
		free(value);
	}
}

static TCLIST *do_tcrdbmisc(TCRDB *rdb, const char *name, const TCLIST *args)
{
	TCLIST *rv = tcrdbmisc(rdb, name, 0, args);

	if (rv == NULL)
		die("tcrdbmisc returned NULL");

	return rv;
}

static void putlist_test(TCRDB *rdb, const char *command, int num, int vsiz,
			unsigned int seed)
{
	struct keygen keygen;
	char *value = xmalloc(vsiz);
	TCLIST *list = tclistnew();
	int i;

	keygen_init(&keygen, seed);

	for (i = 0; i < num; i++) {
		tclistpush2(list, keygen_next_key(&keygen));
		tclistpush(list, value, vsiz);

		if (tclistnum(list) / 2 >= batch) {
			tclistdel(do_tcrdbmisc(rdb, command, list));
			tclistclear(list);
		}
	}
	tclistdel(do_tcrdbmisc(rdb, command, list));

	tclistdel(list);
	free(value);
}

static void check_keys(TCLIST *list, int num, unsigned int seed)
{
	int i;
	struct keygen keygen;

	if (!debug)
		return;

	keygen_init(&keygen, seed);

	if (num != tclistnum(list))
		die("Unexpected key num");

	for (i = 0; i < num; i++) {
		const char *key;
		int ksiz;

		key = tclistval(list, i, &ksiz);
		if (strncmp(keygen_next_key(&keygen), key, ksiz))
			die("Unexpected key"); 
	}
}

static void fwmkeys_test(TCRDB *rdb, int num, unsigned int seed)
{
	struct keygen keygen;
	char prefix[KEYGEN_PREFIX_SIZE + 1];
	TCLIST *list;

	keygen_init(&keygen, seed);

	list = tcrdbfwmkeys2(rdb, keygen_prefix(&keygen, prefix), -1);
	check_keys(list, num, seed);

	tclistdel(list);
}

static void check_records(TCLIST *recs, struct keygen *keygen, int vsiz,
			int batch)
{
	int i;
	int recnum;

	if (!debug)
		return;

	if (!recs && batch > 0)
		die("No records returned");

	recnum = tclistnum(recs);

	if (recnum != batch * 2)
		die("Unexpected list size %d", recnum);

	for (i = 0; i < recnum; i += 2) {
		const void *key;
		const void *val;
		int keysiz;
		int valsiz;

		key = tclistval(recs, i, &keysiz);
		val = tclistval(recs, i + 1, &valsiz);

		if (strncmp(keygen_next_key(keygen), key, keysiz))
			die("Unexpected key");
		if (valsiz != vsiz)
			die("Unexpected value size %d", valsiz);
	}
}

static void getlist_test(TCRDB *rdb, const char *command, int num, int vsiz,
			unsigned int seed)
{
	struct keygen keygen;
	struct keygen keygen_for_check;
	TCLIST *list = tclistnew();
	TCLIST *recs;
	int i;

	keygen_init(&keygen, seed);
	keygen_init(&keygen_for_check, seed);

	for (i = 0; i < num; i++) {
		tclistpush2(list, keygen_next_key(&keygen));

		if (tclistnum(list) >= batch) {
			recs = do_tcrdbmisc(rdb, command, list);
			check_records(recs, &keygen_for_check, vsiz,
					tclistnum(list));
			tclistdel(recs);
			tclistclear(list);
		}
	}
	recs = do_tcrdbmisc(rdb, command, list);
	check_records(recs, &keygen_for_check, vsiz, tclistnum(list));
	tclistdel(recs);

	tclistdel(list);
}

static void range_test(TCRDB *rdb, int num, int vsiz, unsigned int seed)
{
	struct keygen keygen;
	TCLIST *args = tclistnew();
	char start_key[KEYGEN_PREFIX_SIZE + 1];
	char max[100];
	char end_key[KEYGEN_PREFIX_SIZE + 1];

	keygen_init(&keygen, seed);

	keygen_prefix(&keygen, start_key);
	sprintf(max, "%d", batch);
	keygen_prefix(&keygen, end_key);
	end_key[KEYGEN_PREFIX_SIZE - 1] = '-' + 1;

	tclistpush2(args, start_key);
	tclistpush2(args, max);
	tclistpush2(args, end_key);

	while (1) {
		TCLIST *recs;
		int num_recs;

		recs = do_tcrdbmisc(rdb, "range", args);
		num_recs = tclistnum(recs) / 2;
		if (!num_recs)
			break;

		check_records(recs, &keygen, vsiz, num_recs);
		/* overwrite start_key by the last one + '\0' */
		tclistover(args, 0, tclistval2(recs, 2 * (num_recs - 1)), KEYGEN_KEY_SIZE + 1);
		tclistdel(recs);
		num -= num_recs;
	}
	if (num)
		die("Unexpected record num");

	tclistdel(args);
}

static void range2_test(TCRDB *rdb, int num, int vsiz, unsigned int seed)
{
	struct keygen keygen;
	TCLIST *args = tclistnew();
	char start_key[KEYGEN_PREFIX_SIZE + 1];
	char max[100];
	char end_key[KEYGEN_PREFIX_SIZE + 1];
	char binc[2];

	keygen_init(&keygen, seed);

	keygen_prefix(&keygen, start_key);
	sprintf(max, "%d", batch);
	keygen_prefix(&keygen, end_key);
	end_key[KEYGEN_PREFIX_SIZE - 1] = '-' + 1;
	sprintf(binc, "0");

	tclistpush2(args, start_key);
	tclistpush2(args, max);
	tclistpush2(args, end_key);
	tclistpush2(args, binc);

	while (1) {
		TCLIST *recs;
		int num_recs;

		recs = do_tcrdbmisc(rdb, "range2", args);
		num_recs = tclistnum(recs) / 2;
		if (!num_recs)
			break;

		check_records(recs, &keygen, vsiz, num_recs);
		tclistover2(args, 0, tclistval2(recs, 2 * (num_recs - 1)));
		tclistdel(recs);
		num -= num_recs;
	}
	if (num)
		die("Unexpected record num");

	tclistdel(args);
}

static void outlist_test(TCRDB *rdb, const char *command, int num, unsigned int seed)
{
	struct keygen keygen;
	TCLIST *list = tclistnew();
	int i;

	keygen_init(&keygen, seed);

	for (i = 0; i < num; i++) {
		tclistpush2(list, keygen_next_key(&keygen));

		if (tclistnum(list) >= batch) {
			tclistdel(do_tcrdbmisc(rdb, command, list));
			tclistclear(list);
		}
	}
	tclistdel(do_tcrdbmisc(rdb, command, list));

	tclistdel(list);
}

struct thread_data {
	pthread_t tid;
	pthread_barrier_t *barrier;
	TCRDB *rdb;
	unsigned int seed;
	unsigned long long elapsed;
};

static void *benchmark_thread(void *arg)
{
	struct thread_data *data = arg;
	unsigned long long start;
	TCRDB *rdb = data->rdb;
	unsigned int seed = data->seed;

	pthread_barrier_wait(data->barrier);

	start = stopwatch_start();

	if (!strcmp(command, "putlist") || !strcmp(command, "putlist2")) {
		putlist_test(rdb, command, num, vsiz, seed);
	} else if (!strcmp(command, "fwmkeys")) {
		fwmkeys_test(rdb, num, seed);
	} else if (!strcmp(command, "range")) {
		range_test(rdb, num, vsiz, seed);
	} else if (!strcmp(command, "range2")) {
		range2_test(rdb, num, vsiz, seed);
	} else if (!strcmp(command, "getlist") || !strcmp(command, "getlist2")) {
		getlist_test(rdb, command, num, vsiz, seed);
	} else if (!strcmp(command, "fwmkeys-getlist")) {
		fwmkeys_test(rdb, num, seed);
		getlist_test(rdb, "getlist", num, vsiz, seed);
	} else if (!strcmp(command, "fwmkeys-getlist2")) {
		fwmkeys_test(rdb, num, seed);
		getlist_test(rdb, "getlist2", num, vsiz, seed);
	} else if (!strcmp(command, "outlist") || !strcmp(command, "outlist2")) {
		outlist_test(rdb, command, num, seed);
	} else if (!strcmp(command, "fwmkeys-outlist")) {
		fwmkeys_test(rdb, num, seed);
		outlist_test(rdb, "outlist", num, seed);
	} else if (!strcmp(command, "fwmkeys-outlist2")) {
		fwmkeys_test(rdb, num, seed);
		outlist_test(rdb, "outlist2", num, seed);
	} else if (!strcmp(command, "put")) {
		put_test(rdb, num, vsiz, seed);
	} else if (!strcmp(command, "get")) {
		get_test(rdb, num, vsiz, seed);
	} else {
		die("Invalid command %s", command);
	}

	data->elapsed = stopwatch_stop(start);

	return NULL;
}

static void open_rdb(struct thread_data *data, int thnum)
{
	int i;

	if (share) {
		for (i = 0; i < thnum; i++) {
			if ((i % share) == i)
				data[i].rdb = open_db(host, port);
			else
				data[i].rdb = data[i % share].rdb;
		}
	} else {
		for (i = 0; i < thnum; i++)
			data[i].rdb = open_db(host, port);
	}
}

static void close_rdb(struct thread_data *data, int thnum)
{
	int i;

	if (share) {
		for (i = 0; i < thnum; i++)
			if ((i % share) == i)
				close_db(data[i].rdb);
	} else {
		for (i = 0; i < thnum; i++)
			close_db(data[i].rdb);
	}
}

static void benchmark(void)
{
	int i;
	unsigned long long sum = 0, min = ULONG_MAX, max = 0, avg;
	pthread_barrier_t barrier;
	struct thread_data *data;

	pthread_barrier_init(&barrier, NULL, thnum);
	data = xmalloc(sizeof(*data) * thnum);

	open_rdb(data, thnum);

	for (i = 0; i < thnum; i++) {
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

	close_rdb(data, thnum);

	free(data);
}

int main(int argc, char **argv)
{
	parse_options(argc, argv);
	benchmark();

	return 0;
}
