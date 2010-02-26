#include <tcutil.h>
#include <tcbdb.h>
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

static TCBDB *open_db(void)
{
	TCBDB *bdb;

	bdb = tcbdbnew();

	if (!tcbdbopen(bdb, "casket.tcb", BDBOWRITER | BDBOCREAT)){
		int ecode = tcbdbecode(bdb);
		fprintf(stderr, "open error: %s\n", tcbdberrmsg(ecode));
	}
	return bdb;
}

static void close_db(TCBDB *bdb)
{
	if (!tcbdbclose(bdb)){
		int ecode = tcbdbecode(bdb);
		fprintf(stderr, "close error: %s\n", tcbdberrmsg(ecode));
	}
	tcbdbdel(bdb);
}

static void populate_db(TCBDB *bdb, unsigned long count, int value_length)
{
	unsigned long i;
	char key[20];
	char *value = xmalloc(value_length);

	memset(value, '*', value_length);
	value[value_length - 1] = 0;

	for (i = 0; i < count; i++) {
		sprintf(key, "0x%016lu", i);

		if (!tcbdbput2(bdb, key, value)) {
			int ecode = tcbdbecode(bdb);
			fprintf(stderr, "put error: %s\n", tcbdberrmsg(ecode));
		}

		/* put noises */
		sprintf(key, "0w%016lu", i);
		tcbdbput2(bdb, key, value);

		sprintf(key, "0y%016lu", i);
		tcbdbput2(bdb, key, value);
	}
	free(value);
}

static void read_value(char *value)
{
}

static void get_db(TCBDB *bdb, unsigned long count)
{
	unsigned long i;

	for (i = 0; i < count; i++) {
		char key[20];
		char *value;

		sprintf(key, "0x%016lu", i);

		value = tcbdbget2(bdb, key);
		if (!value) {
			int ecode = tcbdbecode(bdb);
			fprintf(stderr, "get error: %s\n", tcbdberrmsg(ecode));
		}
		read_value(value);
		free(value);
	}
}

static void cursor_db(TCBDB *bdb, unsigned long count)
{
	BDBCUR *cur;
	char *key, *value;

	cur = tcbdbcurnew(bdb);
	tcbdbcurjump2(cur, "0x");

	while ((key = tcbdbcurkey2(cur)) != NULL) {
		if (strncmp(key, "0x", 2) != 0)
			break;

		value = tcbdbcurval2(cur);
		read_value(value);

		free(key);
		free(value);
		tcbdbcurnext(cur);
	}
	tcbdbcurdel(cur);
}

void range_db(TCBDB *bdb, unsigned long count)
{
	TCLIST *list;
	const int batch_count = 1000;
	char start_key[20];
	char end_key[20];

	sprintf(start_key, "0x");
	sprintf(end_key, "0y");

	while ((list = tcbdbrange2(bdb, start_key, false, end_key, false,
				batch_count)) != NULL) {
		char *key, *value;
		int num = tclistnum(list);

		if (num == 0) {
			tclistdel(list);
			break;
		}
		snprintf(start_key, sizeof(start_key), "%s",
			tclistval2(list, num - 1));

		while ((key = tclistshift2(list)) != NULL) {
			value = tcbdbget2(bdb, key);
			read_value(value);

			free(key);
			free(value);
		}
		tclistdel(list);
	}
}

static unsigned long count = 10000000;
static int value_length = 100;

/*
 * 'p': populate database
 * 'g': get from database
 * 'c': cursor get from database
 * 'r': range get from database
 */
static int command = 'p';

static void parse_options(int argc, char **argv)
{
	int c;

	while ((c = getopt(argc, argv, "n:l:b:pgcr")) != -1) {
		switch (c) {
		case 'n':
			count = atol(optarg);
			break;
		case 'l':
			value_length = atoi(optarg);
			if (value_length <= 0)
				die("invalid value length");
			break;
		case 'p':
		case 'g':
		case 'c':
		case 'r':
			command = c;
			break;
		default:
			die("invalid option: %c", c);
		}
	}
}

int main(int argc, char **argv){
	TCBDB *bdb;

	parse_options(argc, argv);

	bdb = open_db();

	switch (command) {
	case 'p':
		populate_db(bdb, count, value_length);
		break;
	case 'g':
		get_db(bdb, count);
		break;
	case 'c':
		cursor_db(bdb, count);
		break;
	case 'r':
		range_db(bdb, count);
		break;
	}
	close_db(bdb);

	return 0;
}

