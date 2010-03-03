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

static void get_db(TCRDB *rdb, unsigned long count)
{
	unsigned long i;

	for (i = 0; i < count; i++) {
		char key[20];
		char *value;

		sprintf(key, "0x%016lu", i);

		value = tcrdbget2(rdb, key);
		if (!value) {
			int ecode = tcrdbecode(rdb);
			fprintf(stderr, "get error: %s\n", tcrdberrmsg(ecode));
		}
		read_value(value);
		free(value);
	}
}

static void iter_get_db(TCRDB *rdb, unsigned long count)
{
	char *key, *value;

	if (!tcrdbiterinit(rdb)) {
		int ecode = tcrdbecode(rdb);
		fprintf(stderr, "iter init error: %s\n", tcrdberrmsg(ecode));
		return;
	}

	while ((key = tcrdbiternext2(rdb)) != NULL) {
		if (strncmp(key, "0x", 2) != 0)
			continue;

		value = tcrdbget2(rdb, key);
		if (!value) {
			int ecode = tcrdbecode(rdb);
			fprintf(stderr, "get error: %s\n", tcrdberrmsg(ecode));
		}
		read_value(value);

		free(key);
		free(value);
	}
}

void fwmkeys_db(TCRDB *rdb, unsigned long count)
{
	TCLIST *list;

	list = tcrdbfwmkeys2(rdb, "0x", -1);

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
		if (!tcrdbget3(rdb, map)) {
			int ecode = tcrdbecode(rdb);
			fprintf(stderr, "get3 error: %s\n", tcrdberrmsg(ecode));
		}
		tcmapiterinit(map);
		while ((key = tcmapiternext2(map)) != NULL) {
			const char *value = tcmapiterval2(key);
			read_value(value);
		}
		tcmapdel(map);
	}
	tclistdel(list);
}

void fwmkeys_get_db(TCRDB *rdb, unsigned long count)
{
	TCLIST *list;
	char *key;

	list = tcrdbfwmkeys2(rdb, "0x", -1);

	while ((key = tclistshift2(list)) != NULL) {
		char *value = tcrdbget2(rdb, key);

		read_value(value);
		free(key);
		free(value);
	}
	tclistdel(list);
}

static unsigned long count = 10000000;
static int value_length = 100;
static char *host = "localhost:1978";

/*
 * 'g': get from database
 * 'i': iter get from database
 * 'k': fwmkeys
 * 'f': fwmkeys and get from database
 * 'F': fwmkeys and get3 from database
 */
static int command = 'g';

static void parse_options(int argc, char **argv)
{
	int c;

	while ((c = getopt(argc, argv, "n:l:s:gikfF")) != -1) {
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
		case 'g':
		case 'i':
		case 'k':
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
	case 'g':
		get_db(rdb, count);
		break;
	case 'i':
		iter_get_db(rdb, count);
		break;
	case 'k':
		fwmkeys_db(rdb, count);
		break;
	case 'f':
		fwmkeys_get_db(rdb, count);
	case 'F':
		fwmkeys_get3_db(rdb, count);
		break;
	}
	close_db(rdb);

	return 0;
}
