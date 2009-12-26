#include <stdio.h>
#include <unistd.h>
#include <stdlib.h>

unsigned long bufsiz = BUFSIZ;

static void parse_options(int argc, char **argv)
{
	int c;

	while ((c = getopt(argc, argv, "z:")) != -1) {
		switch(c) {
		case 'z':
			bufsiz = atol(optarg);
			break;
		default:
			break;
		}
	}
}

static int cat(FILE *input, FILE *output)
{
	while (1) {
		int c;

		c = fgetc(input);
		if (c == EOF)
			return -1;
		c = fputc(c, stdout);
		if (c == EOF)
			return -1;
	}
	return 0;
}

int main(int argc, char **argv)
{
	char *buf;
	int ret;

	parse_options(argc, argv);

	buf = malloc(bufsiz);
	if (!buf)
		return -1;

	ret = setvbuf(stdin, buf, _IOLBF, bufsiz);
	if (ret)
		return -1;

	return cat(stdin, stdout);
}
