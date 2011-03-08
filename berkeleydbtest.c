#include <db.h>
#include <string.h>
#include <stdlib.h>
#include <stdbool.h>
#include "testutil.h"

static bool debug = false;

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

/* Shared by all benchmark threads */
static struct BDB *bdb;

static void *open_db(struct benchmark_config *config)
{
	int ret;
	DB *db;
	DB_ENV *dbenv;

	if (bdb)
		return bdb;

	bdb = xmalloc(sizeof(*bdb));

	dbenv = init_env(config->path);

	ret = db_create(&db, dbenv, 0);
	if (ret) {
		dbenv->err(dbenv, ret, "db_create");
		exit(1);
	}
	db->set_errfile(db, stderr);

	ret = db->set_flags(db, DB_DUP);
	if (ret)
		die("dbp->set_flags: %s", db_strerror(ret));

	ret = db->open(db, NULL, "data.db", NULL, DB_BTREE, DB_CREATE, 0664);
	if (ret) {
		db->err(db, ret, "open");
		exit(1);
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

static void close_db(void *db)
{
	DB_ENV *dbenv = bdb->dbenv;

	if (bdb != db)
		return;

	pthread_mutex_lock(&bdb->stop_mutex);
	bdb->stop = true;
	pthread_mutex_unlock(&bdb->stop_mutex);

	xpthread_join(bdb->deadlock_tid);
#ifdef USE_MEMP_TRICKLE_THREAD
	xpthread_join(bdb->trickle_tid);
#endif

	bdb->db->close(bdb->db, 0);
	exit_env(dbenv);

	free(bdb);
	bdb = NULL;
}

#define KSIZ KEYGEN_KEY_SIZE

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

static void putlist_test(void *db, const char *command, int num, int vsiz,
			int batch, unsigned int seed)
{
	DB *bdb = ((struct BDB *)db)->db;
	struct keygen keygen;
	char *value = xmalloc(vsiz);
	DBT key, data;
	void *ptrk, *ptrd;
	int i;

	keygen_init(&keygen, seed);

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
					keygen_next_key(&keygen), KSIZ);
		DB_MULTIPLE_WRITE_NEXT(ptrd, &data, value, vsiz);
		if (ptrk == NULL || ptrd == NULL)
			die("DB_MULTIPLE_WRITE_NEXT failed");

		if ((i + 1) % batch == 0) {
			db_put(bdb, &key, &data, DB_MULTIPLE);

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
		db_put(bdb, &key, &data, DB_MULTIPLE);
		free(key.data);
		free(data.data);
	}

	free(value);
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

static void outlist_test(void *db, const char *command, int num, int batch,
			unsigned int seed)
{
	DB *bdb = ((struct BDB *)db)->db;
	struct keygen keygen;
	DBT key;
	void *ptrk;
	int i;

	keygen_init(&keygen, seed);

	memset(&key, 0, sizeof(key));

	key.ulen = batch * KSIZ * 1024;
	key.flags = DB_DBT_USERMEM | DB_DBT_BULK;
	key.data = xmalloc(key.ulen);

	DB_MULTIPLE_WRITE_INIT(ptrk, &key);

	for (i = 0; i < num; i++) {
		DB_MULTIPLE_WRITE_NEXT(ptrk, &key,
					keygen_next_key(&keygen), KSIZ);
		if (ptrk == NULL)
			die("DB_MULTIPLE_WRITE_NEXT failed");

		if ((i + 1) % batch == 0) {
			db_del(bdb, &key, DB_MULTIPLE);

			free(key.data);

			memset(&key, 0, sizeof(key));

			key.ulen = batch * KSIZ * 1024;
			key.flags = DB_DBT_USERMEM | DB_DBT_BULK;
			key.data = xmalloc(key.ulen);

			DB_MULTIPLE_WRITE_INIT(ptrk, &key);
		}
	}
	if (num % batch) {
		db_del(bdb, &key, DB_MULTIPLE);
		free(key.data);
	}
}

static void put_test(void *db, int num, int vsiz, unsigned int seed)
{
	DB *bdb = ((struct BDB *)db)->db;
	struct keygen keygen;
	char *value = xmalloc(vsiz);
	DBT key, data;
	int i;

	keygen_init(&keygen, seed);

	memset(&key, 0, sizeof(key));
	memset(&data, 0, sizeof(data));

	key.ulen = KSIZ;
	key.flags = DB_DBT_USERMEM;
	key.data = xmalloc(key.ulen);

	data.ulen = vsiz;
	data.flags = DB_DBT_USERMEM;
	data.data = xmalloc(data.ulen);

	for (i = 0; i < num; i++) {
		memcpy(key.data, keygen_next_key(&keygen), KSIZ);
		key.size = KSIZ;

		memcpy(data.data, value, vsiz);
		data.size = vsiz;

		db_put(bdb, &key, &data, DB_NOOVERWRITE);
	}

	free(key.data);
	free(data.data);
	free(value);
}

static void get_test(void *db, int num, int vsiz, unsigned int seed)
{
	DB *bdb = ((struct BDB *)db)->db;
	struct keygen keygen;
	char *value = xmalloc(vsiz);
	DBT key, data;
	int i;

	keygen_init(&keygen, seed);

	memset(&key, 0, sizeof(key));
	memset(&data, 0, sizeof(data));

	key.ulen = KSIZ;
	key.size = KSIZ;
	key.flags = DB_DBT_USERMEM;
	key.data = xmalloc(key.ulen);

	data.flags = DB_DBT_MALLOC;

	for (i = 0; i < num; i++) {
		int ret;

		memcpy(key.data, keygen_next_key(&keygen), KSIZ);
		key.size = KSIZ;

		ret = bdb->get(bdb, NULL, &key, &data, 0);
		if (ret) {
			bdb->err(bdb, ret, "DB->get");
			continue;
		}
		if (debug && vsiz != data.size)
			die("Unexpected value size: %d", data.size);

		free(data.data);
	}

	free(key.data);
	free(value);
}

static void fwmkeys_test(void *db, int num, unsigned int seed)
{
	die("fwmkeys_test is not implemented");
}

static void getlist_test(void *db, const char *command, int num, int vsiz,
			int batch, unsigned int seed)
{
	die("getlist_test is not implemented");
}

static void rangeout_test(void *db, const char *command, int num, int vsiz,
			int batch, unsigned int seed)
{
	die("rangeout_test is not implemented");
}

static void range_test(void *db, const char *command, int num, int vsiz,
			int batch, unsigned int seed)
{
	DB *bdb = ((struct BDB *)db)->db;
	struct keygen keygen;
	char *value = xmalloc(vsiz);
	DBT key, data;
	DBC *cursor;
	u_int32_t flags;
	int i;
	int ret;
	char prefix[KEYGEN_PREFIX_SIZE + 1];

	keygen_init(&keygen, seed);

	memset(&key, 0, sizeof(key));
	memset(&data, 0, sizeof(data));

	key.ulen = KSIZ;
	key.flags = DB_DBT_USERMEM;
	key.data = xmalloc(key.ulen);

	data.flags = DB_DBT_MALLOC;

	ret = bdb->cursor(bdb, NULL, &cursor, 0);
	if (ret) {
		bdb->err(bdb, ret, "DB->cursor");
		return;
	}
	keygen_prefix(&keygen, prefix);

	memcpy(key.data, prefix, KEYGEN_PREFIX_SIZE);
	key.size = KEYGEN_PREFIX_SIZE;
	flags = DB_SET_RANGE;

	for (i = 0; i < num; i++) {
		ret = cursor->get(cursor, &key, &data, flags);

		if (ret == DB_NOTFOUND) {
			break;
		} else if (ret) {
			bdb->err(bdb, ret, "cursor->get");
			break;
		}
		if (debug && memcmp(key.data, keygen_next_key(&keygen), KSIZ))
			die("Unexpected key");
		if (debug && vsiz != data.size)
			die("Unexpected value size %d", data.size);

		free(data.data);
		flags = DB_NEXT;
	}
	if (debug && i != num)
		die("Unexpected record num");

	cursor->close(cursor);

	free(key.data);
	free(value);
}

struct benchmark_config config = {
	.producer = "nop",
	.consumer = "nop",
	.path = "data",
	.num = 5000000,
	.vsiz = 100,
	.batch = 1000,
	.producer_thnum = 1,
	.consumer_thnum = 1,
	.debug = false,
	.verbose = 1,
	.ops = {
		.open_db = open_db,
		.close_db = close_db,
		.put_test = put_test,
		.get_test = get_test,
		.putlist_test = putlist_test,
		.fwmkeys_test = fwmkeys_test,
		.getlist_test = getlist_test,
		.range_test = range_test,
		.rangeout_test = rangeout_test,
		.outlist_test = outlist_test,
	},
};

int main(int argc, char **argv)
{
	parse_options(&config, argc, argv);

	debug = config.debug;

	benchmark(&config);

	return 0;
}
