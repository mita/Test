#include <tcutil.h>
#include <tcrdb.h>
#include <stdlib.h>
#include <stdbool.h>
#include <stdint.h>
#include <string.h>
#include <stdarg.h>
#include <getopt.h>
#include <err.h>

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

static TCRDB *open_db(char *host)
{
	TCRDB *rdb;

	rdb = tcrdbnew();

	if (!tcrdbopen2(rdb, host)){
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

static void read_value(const char *value)
{
}

static void populate_db(TCRDB *rdb, unsigned long count, int value_length)
{
	unsigned long i;
	char key[20];
	char *value = xmalloc(value_length);

	memset(value, '*', value_length);
	value[value_length - 1] = 0;

	for (i = 0; i < count; i++) {
		sprintf(key, "0x%016lu", i);

		if (!tcrdbput2(rdb, key, value)) {
			int ecode = tcrdbecode(rdb);
			fprintf(stderr, "put error: %s\n", tcrdberrmsg(ecode));
		}

		/* put noises */
		sprintf(key, "0w%016lu", i);
		tcrdbput2(rdb, key, value);

		sprintf(key, "0y%016lu", i);
		tcrdbput2(rdb, key, value);
	}
	free(value);
}

static void get_db(TCRDB *rdb, unsigned long count)
{
	unsigned long i;

	for (i = 0; i < count; i++) {
		char key[20];
		char *value;

		sprintf(key, "0x%016lu", i);
		value = tcrdbget2(rdb, key);
		read_value(value);
		free(value);
	}
}

static void mget(TCRDB *rdb, TCMAP *map)
{
	const char *key;

	if (!tcrdbget3(rdb, map)) {
		int ecode = tcrdbecode(rdb);
		fprintf(stderr, "get3 error: %s\n", tcrdberrmsg(ecode));
	}
	tcmapiterinit(map);
	while ((key = tcmapiternext2(map)) != NULL) {
		const char *value = tcmapiterval2(key);
		read_value(value);
	}
}

static void get3_db(TCRDB *rdb, unsigned long count)
{
	unsigned long index = 0;

	while (index < count) {
		int i;
		const int batch_count = (count - index < 1000) ? : 1000;
		TCMAP *map = tcmapnew();

		for (i = 0; i < batch_count; i++) {
			char key[20];

			sprintf(key, "0x%016lu", index);
			tcmapput2(map, key, "");
			index++;
		}
		mget(rdb, map);
		tcmapdel(map);
	}
}

static void iter_db(TCRDB *rdb, unsigned long count)
{
	char *key;

	if (!tcrdbiterinit(rdb)) {
		int ecode = tcrdbecode(rdb);
		fprintf(stderr, "iter init error: %s\n", tcrdberrmsg(ecode));
		return;
	}

	while ((key = tcrdbiternext2(rdb)) != NULL) {
		if (strncmp(key, "0x", 2) != 0)
			continue;
		free(key);
	}
}

static void iter_get_db(TCRDB *rdb, unsigned long count)
{
	char *key;

	if (!tcrdbiterinit(rdb)) {
		int ecode = tcrdbecode(rdb);
		fprintf(stderr, "iter init error: %s\n", tcrdberrmsg(ecode));
		return;
	}

	while ((key = tcrdbiternext2(rdb)) != NULL) {
		char *value;

		if (strncmp(key, "0x", 2) != 0)
			continue;

		value = tcrdbget2(rdb, key);
		read_value(value);
		free(key);
		free(value);
	}
}

static void iter_get3_db(TCRDB *rdb, unsigned long count)
{
	char *key;
	const int batch_count = 1000;
	TCMAP *map = tcmapnew();

	if (!tcrdbiterinit(rdb)) {
		int ecode = tcrdbecode(rdb);
		fprintf(stderr, "iter init error: %s\n", tcrdberrmsg(ecode));
		return;
	}

	while ((key = tcrdbiternext2(rdb)) != NULL) {
		if (strncmp(key, "0x", 2) != 0)
			continue;

		tcmapput2(map, key, "");
		free(key);

		if (tcmaprnum(map) < batch_count)
			continue;
		mget(rdb, map);
		tcmapdel(map);
		map = tcmapnew();
	}
	if (tcmaprnum(map) > 0)
		mget(rdb, map);
	tcmapdel(map);
}

void fwmkeys_db(TCRDB *rdb, unsigned long count)
{
	TCLIST *list;

	list = tcrdbfwmkeys2(rdb, "0x", -1);

	tclistdel(list);
}

void fwmkeys_get_db(TCRDB *rdb, unsigned long count)
{
	TCLIST *list;
	char *key;

	list = tcrdbfwmkeys2(rdb, "0x", -1);

	while ((key = tclistshift2(list)) != NULL) {
		char *value;

		value = tcrdbget2(rdb, key);
		read_value(value);
		free(key);
		free(value);
	}
	tclistdel(list);
}

void fwmkeys_get3_db(TCRDB *rdb, unsigned long count)
{
	TCLIST *list;
	const int batch_count = 1000;

	list = tcrdbfwmkeys2(rdb, "0x", -1);

	while (tclistnum(list) > 0) {
		int i;
		const char *key;
		TCMAP *map = tcmapnew();

		for (i = 0; i < batch_count; i++) {
			key = tclistshift2(list);
			if (!key)
				break;
			tcmapput2(map, key, "");
		}
		mget(rdb, map);
		tcmapdel(map);
	}
	tclistdel(list);
}

static unsigned long count = 10000000;
static int value_length = 100;
static char *host = "localhost:1978";

/*
 * 'p': populate database
 * 'g': get from database
 * 'G': get3 from database
 * 'k': fwmkeys
 * 'K': iter
 * 'i': iter get from database
 * 'I': iter get3 from database
 * 'f': fwmkeys and get from database
 * 'F': fwmkeys and get3 from database
 */
static int command = 'g';

static void parse_options(int argc, char **argv)
{
	int c;

	while ((c = getopt(argc, argv, "n:l:s:pgGiIkKfF")) != -1) {
		switch (c) {
		case 'n':
			count = atol(optarg);
			break;
		case 'l':
			value_length = atoi(optarg);
			if (value_length <= 0)
				die("invalid value length");
			break;
		case 's':
			host = optarg;
			break;
		case 'p':
		case 'g':
		case 'G':
		case 'i':
		case 'I':
		case 'k':
		case 'K':
		case 'f':
		case 'F':
			command = c;
			break;
		default:
			die("invalid option: %c", c);
		}
	}
}

int main(int argc, char **argv){
	TCRDB *rdb;

	parse_options(argc, argv);

	rdb = open_db(host);

	switch (command) {
	case 'p':
		populate_db(rdb, count, value_length);
		break;
	case 'g':
		get_db(rdb, count);
		break;
	case 'G':
		get3_db(rdb, count);
		break;
	case 'i':
		iter_get_db(rdb, count);
		break;
	case 'I':
		iter_get3_db(rdb, count);
		break;
	case 'k':
		fwmkeys_db(rdb, count);
		break;
	case 'K':
		iter_db(rdb, count);
		break;
	case 'f':
		fwmkeys_get_db(rdb, count);
		break;
	case 'F':
		fwmkeys_get3_db(rdb, count);
		break;
	}
	close_db(rdb);

	return 0;
}
