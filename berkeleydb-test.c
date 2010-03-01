#include <db.h>
#include <stdio.h>
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

static DB *open_db(void)
{
	DB *db;
	int ret;

	ret = db_create(&db, NULL, 0);
	if (ret) {
		fprintf(stderr, "db_create: %s\n", db_strerror(ret));
		return NULL;
	}
	db->set_errfile(db, stderr);
	db->set_flags(db, DB_TXN_NOSYNC);

	ret = db->open(db, NULL, "casket.db", NULL, DB_BTREE, DB_CREATE, 0664);
	if (ret) {
		db->err(db, ret, "open");
		return NULL;
	}

	return db;
}

static void close_db(DB *db)
{
	db->close(db, 0);
}

static void populate_db(DB *db, unsigned long count, int value_length)
{
	unsigned long i;
	DBT key, data;
	char keybuf[20];
	char *value = xmalloc(value_length);

	memset(value, '*', value_length);
	value[value_length - 1] = 0;

	memset(&key, 0, sizeof(key));
	key.data = keybuf;
	key.size = 18;
	memset(&data, 0, sizeof(data));
	data.data = value;
	data.size = value_length;

	for (i = 0; i < count; i++) {
		int ret;

		sprintf(keybuf, "0x%016lu", i);
		ret = db->put(db, NULL, &key, &data, DB_NOOVERWRITE);
		if (ret)
			db->err(db, ret, "DB->put");

		/* put noises */
		sprintf(keybuf, "0w%016lu", i);
		db->put(db, NULL, &key, &data, DB_NOOVERWRITE);

		sprintf(keybuf, "0y%016lu", i);
		db->put(db, NULL, &key, &data, DB_NOOVERWRITE);
	}
	free(value);
}

static void read_value(char *value)
{
}

static void get_db(DB *db, unsigned long count)
{
	unsigned long i;
	DBT key, data;
	char keybuf[20];

	memset(&key, 0, sizeof(key));
	key.data = keybuf;
	key.size = 18;

	memset(&data, 0, sizeof(data));
	data.flags = DB_DBT_MALLOC;

	for (i = 0; i < count; i++) {
		int ret;

		sprintf(keybuf, "0x%016lu", i);

		ret = db->get(db, NULL, &key, &data, 0);
		if (ret) {
			db->err(db, ret, "DB->get");
			continue;
		}
		read_value(data.data);
		free(data.data);
	}
}

static void cursor_db(DB *db, unsigned long count)
{
	int ret;
	u_int32_t flags;
	DBC *cursor;
	DBT key, data;
	char keybuf[20];

	ret = db->cursor(db, NULL, &cursor, 0);
	if (ret) {
		db->err(db, ret, "DB->cursor");
		return;
	}
	memset(&key, 0, sizeof(key));
	sprintf(keybuf, "0x");
	key.data = keybuf;
	key.size = 2;

	memset(&data, 0, sizeof(data));
	data.flags = DB_DBT_MALLOC;

	flags = DB_SET_RANGE;
	while (1) {
		ret = cursor->get(cursor, &key, &data, flags);
		if (ret == DB_NOTFOUND) {
			break;
		} else if (ret) {
			db->err(db, ret, "cursor->get");
			break;
		}
		if (strncmp(key.data, "0x", 2)) {
			free(data.data);
			break;
		}
		read_value(data.data);
		free(data.data);
		flags = DB_NEXT;
	}
	cursor->close(cursor);
}

static unsigned long count = 10000000;
static int value_length = 100;

/*
 * 'p': populate database
 * 'g': get from database
 * 'c': cursor get from database
 */
static int command = 'p';

static void parse_options(int argc, char **argv)
{
	int c;

	while ((c = getopt(argc, argv, "n:l:b:pgc")) != -1) {
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
			command = c;
			break;
		default:
			die("invalid option: %c", c);
		}
	}
}

int main(int argc, char **argv){
	DB *db;

	parse_options(argc, argv);

	db = open_db();

	switch (command) {
	case 'p':
		populate_db(db, count, value_length);
		break;
	case 'g':
		get_db(db, count);
		break;
	case 'c':
		cursor_db(db, count);
		break;
	}
	close_db(db);

	return 0;
}
