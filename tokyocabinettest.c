#include <tcutil.h>
#include <tcadb.h>
#include <string.h>
#include <stdio.h>
#include <limits.h>
#include "testutil.h"

static TCADB *open_db(const char *path)
{
	TCADB *adb;

	adb = tcadbnew();

	if (!tcadbopen(adb, path)) {
		die("open error: %s", path);
	}
	return adb;
}

static void close_db(TCADB *adb)
{
	if (!tcadbclose(adb)){
		die("close error: %s", tcadbpath(adb));
	}
	tcadbdel(adb);
}

static char *command = "";
static char *path = "data.tcb";
static int num = 5000000;
static int vsiz = 100;
static unsigned int seed;
static int batch = 1000;
static int thnum = 1;
static bool debug = false;

static void parse_options(int argc, char **argv)
{
	int i;

	for (i = 1; i < argc; i++) {
		if (!strcmp(argv[i], "-command")) {
			command = argv[++i];
		} else if (!strcmp(argv[i], "-path")) {
			path = argv[++i];
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
		} else {
			die("Invalid command option");
		}
	}
}

static void put_test(TCADB *adb, int num, int vsiz, unsigned int seed)
{
	struct keygen keygen;
	char *value = xmalloc(vsiz);
	int i;

	keygen_init(&keygen, seed);

	for (i = 0; i < num; i++) {
		const char *key = keygen_next_key(&keygen);

		tcadbput(adb, key, strlen(key), value, vsiz);
	}

	free(value);
}

static void get_test(TCADB *adb, int num, int vsiz, unsigned int seed)
{
	struct keygen keygen;
	int i;

	keygen_init(&keygen, seed);

	for (i = 0; i < num; i++) {
		const char *key = keygen_next_key(&keygen);
		void *value;
		int siz;

		value = tcadbget(adb, key, strlen(key), &siz);
		if (debug && vsiz != siz)
			die("Unexpected value size");
			
		free(value);
	}
}

static TCLIST *do_tcadbmisc(TCADB *adb, const char *name, const TCLIST *args)
{
	TCLIST *rv = tcadbmisc(adb, name, args);

	if (rv == NULL)
		die("tcadbmisc returned NULL");

	return rv;
}

static void putlist_test(TCADB *adb, const char *command, int num, int vsiz,
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
			tclistdel(do_tcadbmisc(adb, command, list));
			tclistclear(list);
		}
	}
	tclistdel(do_tcadbmisc(adb, command, list));

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

static void fwmkeys_test(TCADB *adb, int num, unsigned int seed)
{
	struct keygen keygen;
	char prefix[KEYGEN_PREFIX_SIZE + 1];
	TCLIST *list;

	keygen_init(&keygen, seed);

	list = tcadbfwmkeys2(adb, keygen_prefix(&keygen, prefix), -1);
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

static void getlist_test(TCADB *adb, const char *command, int num, int vsiz,
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
			recs = do_tcadbmisc(adb, command, list);
			check_records(recs, &keygen_for_check, vsiz,
					tclistnum(list));
			tclistdel(recs);
			tclistclear(list);
		}
	}
	recs = do_tcadbmisc(adb, command, list);
	check_records(recs, &keygen_for_check, vsiz, tclistnum(list));
	tclistdel(recs);

	tclistdel(list);
}

static void range_test(TCADB *adb, int num, int vsiz, unsigned int seed)
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

		recs = do_tcadbmisc(adb, "range", args);
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

static void range2_test(TCADB *adb, int num, int vsiz, unsigned int seed)
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

		recs = do_tcadbmisc(adb, "range2", args);
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

static void outlist_test(TCADB *adb, const char *command, int num, unsigned int seed)
{
	struct keygen keygen;
	TCLIST *list = tclistnew();
	int i;

	keygen_init(&keygen, seed);

	for (i = 0; i < num; i++) {
		tclistpush2(list, keygen_next_key(&keygen));

		if (tclistnum(list) >= batch) {
			tclistdel(do_tcadbmisc(adb, command, list));
			tclistclear(list);
		}
	}
	tclistdel(do_tcadbmisc(adb, command, list));

	tclistdel(list);
}

struct thread_data {
	pthread_t tid;
	pthread_barrier_t *barrier;
	TCADB *adb;
	unsigned int seed;
	unsigned long long elapsed;
};

static void *benchmark_thread(void *arg)
{
	struct thread_data *data = arg;
	unsigned long long start;
	TCADB *adb = data->adb;
	unsigned int seed = data->seed;

	pthread_barrier_wait(data->barrier);

	start = stopwatch_start();

	if (!strcmp(command, "putlist") || !strcmp(command, "putlist2")) {
		putlist_test(adb, command, num, vsiz, seed);
	} else if (!strcmp(command, "fwmkeys")) {
		fwmkeys_test(adb, num, seed);
	} else if (!strcmp(command, "range")) {
		range_test(adb, num, vsiz, seed);
	} else if (!strcmp(command, "range2")) {
		range2_test(adb, num, vsiz, seed);
	} else if (!strcmp(command, "getlist") || !strcmp(command, "getlist2")) {
		getlist_test(adb, command, num, vsiz, seed);
	} else if (!strcmp(command, "fwmkeys-getlist")) {
		fwmkeys_test(adb, num, seed);
		getlist_test(adb, "getlist", num, vsiz, seed);
	} else if (!strcmp(command, "fwmkeys-getlist2")) {
		fwmkeys_test(adb, num, seed);
		getlist_test(adb, "getlist2", num, vsiz, seed);
	} else if (!strcmp(command, "outlist") || !strcmp(command, "outlist2")) {
		outlist_test(adb, command, num, seed);
	} else if (!strcmp(command, "put")) {
		put_test(adb, num, vsiz, seed);
	} else if (!strcmp(command, "get")) {
		get_test(adb, num, vsiz, seed);
	} else {
		die("Invalid command %s", command);
	}

	data->elapsed = stopwatch_stop(start);

	return NULL;
}

static void benchmark(TCADB *adb)
{
	int i;
	unsigned long long sum = 0, min = ULONG_MAX, max = 0, avg;
	pthread_barrier_t barrier;
	struct thread_data *data;

	pthread_barrier_init(&barrier, NULL, thnum);
	data = xmalloc(sizeof(*data) * thnum);

	for (i = 0; i < thnum; i++) {
		data[i].adb = adb;
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

	free(data);
}

int main(int argc, char **argv)
{
	TCADB *adb;

	parse_options(argc, argv);
	adb = open_db(path);
	benchmark(adb);
	close_db(adb);

	return 0;
}
