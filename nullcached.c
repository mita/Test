#define _GNU_SOURCE

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <err.h>
#include <stdarg.h>
#include <syslog.h>

static void die(const char *err, ...)
{
	va_list params;

	va_start(params, err);
	vsyslog(LOG_USER | LOG_ERR, err, params);
	va_end(params);

	exit(EXIT_FAILURE);
}

static void print_error(FILE *output)
{
	fprintf(output, "ERROR\r\n");
	fflush(output);
}

static char *db_path;

static void parse_options(int argc, char **argv)
{
	if (argc < 2)
		exit(1);

	db_path = argv[1];
}

static void swallow(FILE *input, unsigned long bytes)
{
	char buf[BUFSIZ];

	while (bytes > 0) {
		size_t ret = fread(buf, 1, BUFSIZ, input);

		if (!ret)
			return;
		bytes -= ret;
	}
}

static void process_quit_command(FILE *input, FILE *output, FILE *db, char *cmd)
{
	fclose(db);
	fclose(output);
	exit(0);
}

static void *value_buffer(size_t size)
{
	static void *value_buffer;
	static size_t value_buffer_size;

	if (value_buffer_size < size) {
		free(value_buffer);
		value_buffer = NULL;
		value_buffer_size = 0;

		value_buffer = malloc(size);
		if (!value_buffer)
			return NULL;

		value_buffer_size = size;
	}
	return value_buffer;
}

static ssize_t db_get(FILE *db, void *value, size_t bytes)
{
	return -1;
}

static ssize_t db_put(FILE *db, void *value, size_t bytes)
{
	return bytes;
}

static void process_get_command(FILE *input, FILE *output, FILE *db,
		char *cmd, char *key)
{
	char buf[BUFSIZ];
	size_t len;

	len = db_get(db, buf, BUFSIZ);
	if (len >= 0 && len < BUFSIZ) {
		fwrite(buf, 1, len, output);
		fprintf(output, "\r\n");
	}
	fprintf(output, "END\r\n");
	fflush(output);
}

static void process_set_command(FILE *input, FILE *output, FILE *db,
		char *cmd, char *key, unsigned long flags,
		unsigned long exptime, unsigned long bytes)
{
	unsigned long total_size = bytes + strlen("\r\n");
	void *value;
	size_t ret;

	value = value_buffer(total_size);
	if (!value) {
		swallow(input, total_size);
		goto error;
	}
	ret = fread(value, 1, total_size, input);
	if (ret != total_size)
		goto error;

	if (strcmp(value + bytes, "\r\n"))
		goto error;

	db_put(db, value, bytes);

	fprintf(output, "STORED\r\n");
	fflush(output);

	return;
error:
	print_error(output);
}

static void memcachedb(FILE *input, FILE *output, FILE *db)
{
	while (1) {
		char *line = NULL;
		ssize_t ret;
		size_t len = 0;
		char cmd[BUFSIZ];
		char key[BUFSIZ];
		unsigned long flags;
		unsigned long exptime;
		unsigned long bytes;

		ret = getline(&line, &len, input);
		if (ret < 0)
			break;

		ret = sscanf(line, "%s %s %ld %ld %ld\r\n",
				cmd, key, &flags, &exptime, &bytes);

		if (!strcmp(cmd, "quit") && ret == 1) {
			process_quit_command(input, output, db, cmd);
		} else if (!strcmp(cmd, "set") && ret == 5) {
			process_set_command(input, output, db,
					cmd, key, flags, exptime, bytes);
		} else if (!strcmp(cmd, "get") && ret == 2) {
			process_get_command(input, output, db, cmd, key);
		} else {
			print_error(output);
		}
		free(line);
	}
}

int main(int argc, char **argv)
{
	FILE *db;
	FILE *input;
	FILE *output;

	parse_options(argc, argv);

	db = fopen(db_path, "w");
	if (!db)
		die("fopen %s", db_path);

	input = fdopen(0, "r");
	if (!input)
		die("fdopen 0");
	output = fdopen(1, "w");
	if (!output)
		die("fdopen 1");

	memcachedb(input, output, db);

	return 0;
}
