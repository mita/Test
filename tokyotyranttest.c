#include <tcutil.h>
#include <tcrdb.h>
#include <string.h>
#include "testutil.h"

static bool debug = false;

static void *open_db(struct benchmark_config *config)
{
	TCRDB *rdb;
	const char *host = config->host;
	int port = config->port;

	rdb = tcrdbnew();

	if (!tcrdbopen(rdb, host, port)){
		int ecode = tcrdbecode(rdb);
		die("open error: %s", tcrdberrmsg(ecode));
	}
	return rdb;
}

static void close_db(void *db)
{
	TCRDB *rdb = db;

	if (!tcrdbclose(rdb)){
		int ecode = tcrdbecode(rdb);
		die("close error: %s", tcrdberrmsg(ecode));
	}
	tcrdbdel(rdb);
}

static void put_test(void *db, int num, int vsiz, unsigned int seed)
{
	TCRDB *rdb = db;
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

static void get_test(void *db, int num, int vsiz, unsigned int seed)
{
	TCRDB *rdb = db;
	struct keygen keygen;
	int i;

	keygen_init(&keygen, seed);

	for (i = 0; i < num; i++) {
		const char *key = keygen_next_key(&keygen);
		void *value;
		int siz;

		value = tcrdbget(rdb, key, strlen(key), &siz);
		if (debug && vsiz != siz)
			die("Unexpected value size: %d", siz);
			
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

static void putlist_test(void *db, const char *command, int num, int vsiz,
			int batch, unsigned int seed)
{
	TCRDB *rdb = db;
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
	if (tclistnum(list))
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
		die("Unexpected key num: %d", tclistnum(list));

	for (i = 0; i < num; i++) {
		int ksiz;
		const char *key = tclistval(list, i, &ksiz);

		if (strncmp(keygen_next_key(&keygen), key, ksiz))
			die("Unexpected key");
	}
}

static void fwmkeys_test(void *db, int num, unsigned int seed)
{
	TCRDB *rdb = db;
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
		int keysiz;
		const char *key = tclistval(recs, i, &keysiz);
		int valsiz;

		if (strncmp(keygen_next_key(keygen), key, keysiz))
			die("Unexpected key");

		tclistval(recs, i + 1, &valsiz);
		if (valsiz != vsiz)
			die("Unexpected value size %d", valsiz);
	}
}

static void getlist_test(void *db, const char *command, int num, int vsiz,
			int batch, unsigned int seed)
{
	TCRDB *rdb = db;
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
	if (tclistnum(list)) {
		recs = do_tcrdbmisc(rdb, command, list);
		check_records(recs, &keygen_for_check, vsiz, tclistnum(list));
		tclistdel(recs);
	}

	tclistdel(list);
}

static void range_nonatomic_test(void *db, int num, int vsiz, int batch,
			unsigned int seed)
{
	TCRDB *rdb = db;
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

		check_records(recs, &keygen, vsiz, num < batch ? num : batch);
		/* overwrite start_key by the last one + '\0' */
		tclistover(args, 0, tclistval2(recs, 2 * (num_recs - 1)), KEYGEN_KEY_SIZE + 1);
		tclistdel(recs);
		num -= num_recs;
	}
	if (debug && num)
		die("Unexpected record num: %d", num);

	tclistdel(args);
}

static void range_atomic_test(void *db, int num, int vsiz, int batch,
			unsigned int seed)
{
	TCRDB *rdb = db;
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

		recs = do_tcrdbmisc(rdb, "range_atomic", args);
		num_recs = tclistnum(recs) / 2;
		if (!num_recs)
			break;

		check_records(recs, &keygen, vsiz, num < batch ? num : batch);
		tclistover2(args, 0, tclistval2(recs, 2 * (num_recs - 1)));
		tclistdel(recs);
		num -= num_recs;
	}
	if (debug && num)
		die("Unexpected record num: %d", num);

	tclistdel(args);
}

static void range_test(void *db, const char *command, int num, int vsiz,
			int batch, unsigned int seed)
{
	if (!strcmp(command, "range"))
		return range_nonatomic_test(db, num, vsiz, batch, seed);
	else if (!strcmp(command, "range_atomic"))
		return range_atomic_test(db, num, vsiz, batch, seed);

	die("invalid range command");
}

static void rangeout_test(void *db, const char *command, int num, int vsiz,
			int batch, unsigned int seed)
{
	TCRDB *rdb = db;
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

		recs = do_tcrdbmisc(rdb, command, args);
		if (tclistnum(recs) == 0)
			break;
		if (debug) {
			const char *num_recs = tclistval2(recs, 0);
			num -= atoi(num_recs);
			if (num != 0 && atoi(num_recs) != batch)
				die("Unexpected number of records are deleted");
		}
		tclistdel(recs);
	}
	if (debug && num != 0)
		die("Unexpected number of records are deleted");

	tclistdel(args);
}

static void outlist_test(void *db, const char *command, int num, int batch,
			unsigned int seed)
{
	TCRDB *rdb = db;
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
	if (tclistnum(list))
		tclistdel(do_tcrdbmisc(rdb, command, list));

	tclistdel(list);
}

static struct benchmark_config config = {
	.producer = "nop",
	.consumer = "nop",
	.host = "localhost",
	.port = 1978,
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
