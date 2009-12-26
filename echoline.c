#define _GNU_SOURCE

#include <stdio.h>
#include <stdlib.h>

int echoline(FILE *input, FILE *output)
{
	char *line = NULL;
	size_t len = 0;

	while (1) {
		ssize_t ret;
		size_t bytes;

		ret = getline(&line, &len, stdin);
		if (ret < 0) {
			fprintf(stderr, "getline\n");
			break;
		}

		bytes = fwrite(line, 1, ret, stdout);
		if (bytes < ret) {
			if (ferror(stdout))
				fprintf(stderr, "ferror\n");
			break;
		}
	}
	if (line)
		free(line);

	return 0;
}

int main(int argc, char **argv)
{
	return echoline(stdin, stdout);
}
