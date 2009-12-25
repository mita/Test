#define _GNU_SOURCE

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <err.h>
#include <stdarg.h>
#include <syslog.h>
#include <stdbool.h>

static void die(const char *err, ...)
{
	va_list params;

	va_start(params, err);
	vsyslog(LOG_USER | LOG_ERR, err, params);
	va_end(params);

	exit(EXIT_FAILURE);
}

static void output_error(FILE *output)
{
	fprintf(output, "ERROR\r\n");
	fflush(output);
}

#if 0

static void output_client_error(FILE *output, const char *error)
{
	fprintf(output, "CLIENT_ERROR %s\r\n", error);
	fflush(output);
}

static void output_server_error(FILE *output, const char *error)
{
	fprintf(output, "SERVER_ERROR %s\r\n", error);
	fflush(output);
}

#endif

static char *db_pathname = "/dev/null";

static void parse_options(int argc, char **argv)
{
	if (argc >= 2)
		db_pathname = argv[1];
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

static void process_quit_command(FILE *input, FILE *output, FILE *db)
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

static void process_get_command(FILE *input, FILE *output, FILE *db, char *key)
{
	char buf[BUFSIZ];
	size_t len;

	len = db_get(db, buf, BUFSIZ);
	if (len >= 0 && len < BUFSIZ) {
		fwrite(buf, 1, len, output);
		fprintf(output, "\r\n");
	}
}

static void process_set_command(FILE *input, FILE *output, FILE *db,
		char *key, unsigned long flags,
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
	output_error(output);
}

enum {
	/* Storage commands */
	SET_OP,
	ADD_OP,
	REPLACE_OP,
	APPEND_OP,
	PREPEND_OP,
	CAS_OP,
	/* Retrieval commands */
	GET_OP,
	GETS_OP,
	/* Deletion commands */
	DELETE_OP,
	/* Increment/Decrement commands */
	INCR_OP,
	DECR_OP,
	/* Statistics commands */
	STATS_OP,
	/* Other commands */
	FLUSH_ALL_OP,
	VERSION_OP,
	QUIT_OP,
};

enum {
	INVALID_COMMAND = -1,
	STORAGE_COMMAND,
	RETRIEVAL_COMMAND,
	DELETION_COMMAND,
	INCR_DECR_COMMAND,
	STATS_COMMAND,
	OTHER_COMMAND,
};

static int command_type(const char *command)
{
	if (!strcmp(command, "set") || !strcmp(command, "add") ||
		!strcmp(command, "replace") || !strcmp(command, "append") ||
		!strcmp(command, "prepend") || !strcmp(command, "cas"))
		return STORAGE_COMMAND;
	else if (!strcmp(command, "get") || !strcmp(command, "gets"))
		return RETRIEVAL_COMMAND;
	else if (!strcmp(command, "delete"))
		return DELETION_COMMAND;
	else if (!strcmp(command, "incr") || !strcmp(command, "decr"))
		return INCR_DECR_COMMAND;
	else if (!strcmp(command, "stats"))
		return STATS_COMMAND;
	else if (!strcmp(command, "flush_all") || !strcmp(command, "version") ||
		!strcmp(command, "quit"))
		return OTHER_COMMAND;
	else
		return INVALID_COMMAND;
}

static bool check_line(const char *line, size_t n)
{
	return (line[n - 1] == '\r') && (line[n] == '\n');
}

static void split_line(char *line, char **command, char **arguments)
{
	*arguments = line;

	*command = strsep(arguments, " \r");
}

static void process_storage_command(FILE *input, FILE *output, FILE *db,
				const char *command, const char *arguments)
{
	int ret;
	char key[BUFSIZ];
	unsigned long flags;
	unsigned long exptime;
	unsigned long bytes;

	ret = sscanf(arguments, "%250s %ld %ld %ld\r\n",
			key, &flags, &exptime, &bytes);

	if (!strcmp(command, "set") && ret == 4) {
		process_set_command(input, output, db,
				key, flags, exptime, bytes);
	} else {
		output_error(output);
	}
}

static void process_retrieval_command(FILE *input, FILE *output, FILE *db,
				const char *command, char *arguments)
{
	if (!strcmp(command, "get")) {
		char *key;

		while ((key = strsep(&arguments, " \r")) != NULL) {
			process_get_command(input, output, db, key);
		}
		fprintf(output, "END\r\n");
		fflush(output);
	} else {
		output_error(output);
	}
}

static void process_other_command(FILE *input, FILE *output, FILE *db,
				const char *command, const char *arguments)
{
	if (strcmp(command, "quit"))
		process_quit_command(input, output, db);
	else
		output_error(output);
}

static void memcachedb(FILE *input, FILE *output, FILE *db)
{
	char *line = NULL;
	size_t len = 0;

	while (1) {
		ssize_t ret;
		char *command;
		char *arguments;

		ret = getline(&line, &len, input);
		if (ret < 0)
			break;

		if (!check_line(line, ret)) {
			output_error(output);
			continue;
		}
		split_line(line, &command, &arguments);

		switch (command_type(command)) {
		case STORAGE_COMMAND:
			process_storage_command(input, output, db,
						command, arguments);
			break;
		case RETRIEVAL_COMMAND:
			process_retrieval_command(input, output, db,
						command, arguments);
			break;
		case DELETION_COMMAND:
			break;
		case INCR_DECR_COMMAND:
			break;
		case STATS_COMMAND:
			break;
		case OTHER_COMMAND:
			process_other_command(input, output, db,
						command, arguments);
			break;
		default:
			output_error(output);
			break;
		}
	}
	if (line)
		free(line);
}

int main(int argc, char **argv)
{
	FILE *db;

	parse_options(argc, argv);

	db = fopen(db_pathname, "w");
	if (!db)
		die("fopen %s", db_pathname);

	memcachedb(stdin, stdout, db);

	fclose(db);

	return 0;
}
