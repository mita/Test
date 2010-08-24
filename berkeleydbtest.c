#include <db.h>
#include <string.h>
#include <stdio.h>
#include <limits.h>
#include <unistd.h>
#include <stdbool.h>
#include "testutil.h"

/* Disable memory pool trickle thread by default */
#undef USE_MEMP_TRICKLE_THREAD

struct BDB {
	DB_ENV *dbenv;
	DB *db;

	bool stop;
	pthread_mutex_t stop_mutex;

	pthread_t deadlock_tid;
#ifdef USE_MEMP_TRICKLE_THREAD
	pthread_t trickle_tid;
#endif
};

static void *deadlock_thread(void *arg)
{
	struct BDB *bdb = arg;
	bool stop = false;

	while (!stop) {
		DB_ENV *dbenv = bdb->dbenv;

		dbenv->lock_detect(dbenv, 0, DB_LOCK_YOUNGEST, NULL);
		usleep(100 * 1000);

		pthread_mutex_lock(&bdb->stop_mutex);
		stop = bdb->stop;
		pthread_mutex_unlock(&bdb->stop_mutex);
	}

	return NULL;
}

#ifdef USE_MEMP_TRICKLE_THREAD

static void *trickle_thread(void *arg)
{
	struct BDB *bdb = arg;
	bool stop = false;

	while (!stop) {
		int wrote;
		DB_ENV *dbenv = bdb->dbenv;

		dbenv->memp_trickle(dbenv, 10, &wrote);
		if (wrote == 0) {
			sleep(1);
			sched_yield();
		}
		pthread_mutex_lock(&bdb->stop_mutex);
		stop = bdb->stop;
		pthread_mutex_unlock(&bdb->stop_mutex);
	}

	return NULL;
}

#endif /* USE_MEMP_TRICKLE_THREAD */

static DB_ENV *init_env(const char *home)
{
	DB_ENV *env;
	int ret;

	ret = db_env_create(&env, 0);
	if (ret)
		die("db_env_create: %s", db_strerror(ret));

	env->set_flags(env, DB_TXN_NOSYNC, 1);
	env->set_cachesize(env, 8, 0, 2);
	env->set_errfile(env, stderr);

	ret = env->open(env, home,
		DB_CREATE | DB_INIT_LOCK | DB_THREAD | DB_INIT_MPOOL, 0);
	if (ret) {
		env->err(env, ret, "env->open");
		goto error;
	}

	return env;
error:
	env->close(env, 0);
	exit(EXIT_FAILURE);

	return NULL;
}

static void exit_env(DB_ENV *env)
{
	env->close(env, 0);
}

static struct BDB *open_db(const char *path)
{
	int ret;
	struct BDB *bdb = xmalloc(sizeof(*bdb));
	DB *db;
	DB_ENV *dbenv;

	dbenv = init_env(path);

	ret = db_create(&db, dbenv, 0);
	if (ret) {
		dbenv->err(dbenv, ret, "db_create");
		return NULL;
	}
	db->set_errfile(db, stderr);

	ret = db->set_flags(db, DB_DUP);
	if (ret)
		die("dbp->set_flags: %s", db_strerror(ret));

	ret = db->open(db, NULL, "data.db", NULL, DB_BTREE, DB_CREATE, 0664);
	if (ret) {
		db->err(db, ret, "open");
		return NULL;
	}

	bdb->dbenv = dbenv;
	bdb->db = db;

	bdb->stop = false;
	pthread_mutex_init(&bdb->stop_mutex, NULL);

#ifdef USE_MEMP_TRICKLE_THREAD
	xpthread_create(&bdb->trickle_tid, trickle_thread, bdb);
#endif
	xpthread_create(&bdb->deadlock_tid, deadlock_thread, bdb);

	return bdb;
}

static void close_db(struct BDB *bdb)
{
	DB_ENV *dbenv = bdb->dbenv;
	DB *db = bdb->db;

	pthread_mutex_lock(&bdb->stop_mutex);
	bdb->stop = true;
	pthread_mutex_unlock(&bdb->stop_mutex);

	xpthread_join(bdb->deadlock_tid);
#ifdef USE_MEMP_TRICKLE_THREAD
	xpthread_join(bdb->trickle_tid);
#endif

	db->close(db, 0);
	exit_env(dbenv);

	free(bdb);
}

static char *command = "";
static char *path = "data";
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
		} else if (!strcmp(argv[i], "-path")) {
			path = argv[++i];
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

#define KSIZ sizeof("0x0000000000000000-0x0000000000000000")

static void db_put(DB *db, DBT *key, DBT *data, u_int32_t flags)
{
	int ret;
retry:
	ret = db->put(db, NULL, key, data, flags);

	switch (ret) {
		case 0:
			break;
		case DB_LOCK_DEADLOCK:
			goto retry;
		default:
			db->err(db, ret, "DB->put");
			exit(EXIT_FAILURE);
			break;
	}
}

static void putlist_test(DB *db, int num, int vsiz, long seed)
{
	struct keygen *keygen = keygen_alloc(seed);
	char *value = xmalloc(vsiz);
	DBT key, data;
	void *ptrk, *ptrd;
	int i;

	memset(&key, 0, sizeof(key));
	memset(&data, 0, sizeof(data));

	key.ulen = batch * KSIZ * 1024;
	key.flags = DB_DBT_USERMEM | DB_DBT_BULK;
	key.data = xmalloc(key.ulen);

	data.ulen = batch * vsiz * 1024;
	data.flags = DB_DBT_USERMEM | DB_DBT_BULK;
	data.data = xmalloc(data.ulen);

	DB_MULTIPLE_WRITE_INIT(ptrk, &key);
	DB_MULTIPLE_WRITE_INIT(ptrd, &data);

	for (i = 0; i < num; i++) {
		DB_MULTIPLE_WRITE_NEXT(ptrk, &key,
					keygen_next_key(keygen), KSIZ);
		DB_MULTIPLE_WRITE_NEXT(ptrd, &data, value, vsiz);
		if (ptrk == NULL || ptrd == NULL)
			die("DB_MULTIPLE_WRITE_NEXT failed");

		if ((i + 1) % batch == 0) {
			db_put(db, &key, &data, DB_MULTIPLE);

			free(key.data);
			free(data.data);

			memset(&key, 0, sizeof(key));
			memset(&data, 0, sizeof(data));

			key.ulen = batch * KSIZ * 1024;
			key.flags = DB_DBT_USERMEM | DB_DBT_BULK;
			key.data = xmalloc(key.ulen);

			data.ulen = batch * vsiz * 1024;
			data.flags = DB_DBT_USERMEM | DB_DBT_BULK;
			data.data = xmalloc(data.ulen);
			
			DB_MULTIPLE_WRITE_INIT(ptrk, &key);
			DB_MULTIPLE_WRITE_INIT(ptrd, &data);
		}
	}

	if (num % batch) {
		db_put(db, &key, &data, DB_MULTIPLE);
		free(key.data);
		free(data.data);
	}

	free(value);
	keygen_free(keygen);
}

static void db_del(DB *db, DBT *key, u_int32_t flags)
{
	int ret;
retry:
	ret = db->del(db, NULL, key, flags);

	switch (ret) {
		case 0:
			break;
		case DB_LOCK_DEADLOCK:
			goto retry;
		default:
			db->err(db, ret, "DB->del");
			exit(EXIT_FAILURE);
			break;
	}
}

static void outlist_test(DB *db, int num, long seed)
{
	struct keygen *keygen = keygen_alloc(seed);
	DBT key;
	void *ptrk;
	int i;

	memset(&key, 0, sizeof(key));

	key.ulen = batch * KSIZ * 1024;
	key.flags = DB_DBT_USERMEM | DB_DBT_BULK;
	key.data = xmalloc(key.ulen);

	DB_MULTIPLE_WRITE_INIT(ptrk, &key);

	for (i = 0; i < num; i++) {
		DB_MULTIPLE_WRITE_NEXT(ptrk, &key,
					keygen_next_key(keygen), KSIZ);
		if (ptrk == NULL)
			die("DB_MULTIPLE_WRITE_NEXT failed");

		if ((i + 1) % batch == 0) {
			db_del(db, &key, DB_MULTIPLE);

			free(key.data);

			memset(&key, 0, sizeof(key));

			key.ulen = batch * KSIZ * 1024;
			key.flags = DB_DBT_USERMEM | DB_DBT_BULK;
			key.data = xmalloc(key.ulen);

			DB_MULTIPLE_WRITE_INIT(ptrk, &key);
		}
	}
	if (num % batch) {
		db_del(db, &key, DB_MULTIPLE);
		free(key.data);
	}

	keygen_free(keygen);
}

struct thread_data {
	pthread_t tid;
	pthread_barrier_t *barrier;
	DB *db;
	long seed;
	unsigned long long elapsed;
};

static void *benchmark_thread(void *arg)
{
	struct thread_data *data = arg;
	unsigned long long start;
	DB *db = data->db;
	long seed = data->seed;

	pthread_barrier_wait(data->barrier);

	start = stopwatch_start();

	if (!strcmp(command, "putlist")) {
		putlist_test(db, num, vsiz, seed);
	} else if (!strcmp(command, "outlist")) {
		outlist_test(db, num, seed);
	} else {
		die("Invalid command %s", command);
	}

	data->elapsed = stopwatch_stop(start);

	return NULL;
}

static void benchmark(DB *db)
{
	int i;
	unsigned long long sum = 0, min = ULONG_MAX, max = 0, avg;
	pthread_barrier_t barrier;
	struct thread_data *data;

	pthread_barrier_init(&barrier, NULL, thnum);
	data = xmalloc(sizeof(*data) * thnum);

	for (i = 0; i < thnum; i++) {
		data[i].db = db;
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
	struct BDB *bdb;

	parse_options(argc, argv);
	bdb = open_db(path);
	benchmark(bdb->db);
	close_db(bdb);

	return 0;
}
