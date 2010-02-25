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

	if(!tcbdbopen(bdb, "casket.tcb", BDBOWRITER | BDBOCREAT)){
		int ecode = tcbdbecode(bdb);
		fprintf(stderr, "open error: %s\n", tcbdberrmsg(ecode));
	}
	return bdb;
}

static void close_db(TCBDB *bdb)
{
	if(!tcbdbclose(bdb)){
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
	}
	free(value);
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
		free(value);
	}
}

static void cursor_db(TCBDB *bdb, unsigned long count)
{
	BDBCUR *cur;
	char *key, *value;
	char start_key[20];
	char end_key[20];

	sprintf(start_key, "0x%016lu", 0UL);
	sprintf(end_key, "0x%016lu", count - 1);

	cur = tcbdbcurnew(bdb);
	tcbdbcurjump2(cur, start_key);

	while ((key = tcbdbcurkey2(cur)) != NULL) {
		value = tcbdbcurval2(cur);
		free(value);
		free(key);
		tcbdbcurnext(cur);
	}
	tcbdbcurdel(cur);
}

void range_db(TCBDB *bdb, unsigned long count)
{
	TCLIST *list;
	const int batch_count = 10;
	char *key, *value;
	char start_key[20];
	char end_key[20];

	sprintf(start_key, "0x");
	sprintf(end_key, "0xf");

	while ((list = tcbdbrange2(bdb, start_key, true, end_key, true, batch_count)) != NULL) {
		while ((value = tclistpop2(list)) != NULL) {
			free(value);
		}
		tclistdel(list);
	}
}

static unsigned long count = 100000;
static int value_length = 1000;
static int batch_count = 10;

/*
 * 'p': populate db
 * 'g': get
 * 'c': cursor
 * 'r': range
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
		case 'b':
			batch_count = atoi(optarg);
			if (batch_count <= 0)
				die("invalid batch count");
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

