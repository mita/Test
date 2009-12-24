#define _GNU_SOURCE
#define _LARGEFILE64_SOURCE

#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <sys/mman.h>
#include <sys/types.h>
#include <fcntl.h>
#include <sys/vfs.h>
#include <errno.h>
#include <err.h>

#define DEFAULT_TMP_DIR "/tmp"
#define BIGMALLOC_TMP_TEMPLATE "bigmalloc-XXXXXX"

static const char *bigmalloc_tmp_dir(void)
{
	const char *tmp_dir;

	tmp_dir = getenv("BIGMALLOC_TMP_DIR");

	return !tmp_dir ? DEFAULT_TMP_DIR : tmp_dir;
}

#ifndef HAVE_MKOSTEMP

static int mkostemp(char *template, int flags)
{

	mktemp(template);
	return open(template, O_CREAT | O_EXCL | O_RDWR | flags);
}

#endif

static int mktempfd(void)
{
	char *tmpfile;
	int fd;
	int ret;

	ret = asprintf(&tmpfile, "%s/%s",
			bigmalloc_tmp_dir(), BIGMALLOC_TMP_TEMPLATE);
	if (ret < 0)
		return -1;

	while (1) {
		fd = mkostemp(tmpfile, O_LARGEFILE);
		if (fd >= 0)
			break;

		if (errno != EEXIST)
			err(EXIT_FAILURE, "bigmalloc: mkostemp(%s/%s)",
				bigmalloc_tmp_dir(), BIGMALLOC_TMP_TEMPLATE);

		sprintf(tmpfile, "%s/%s",
			bigmalloc_tmp_dir(), BIGMALLOC_TMP_TEMPLATE);
	}
	ret = unlink(tmpfile);
	if (ret)
		warn("bigmalloc: unlink(%s)", tmpfile);

	free(tmpfile);

	return fd;
}

struct bigmalloc_chunk {
	int magic;
	size_t size;
};

#define BIGMALLOC_HEADER_SIZE ((sizeof(struct bigmalloc_chunk) + 15) & ~15)
#define BIGMALLOC_MAGIC (0xb19)

static inline void *bigmalloc_chunk_to_mem(struct bigmalloc_chunk *chunk)
{
	return (char *)chunk + BIGMALLOC_HEADER_SIZE;
}

static inline struct bigmalloc_chunk *mem_to_bigmalloc_chunk(void *ptr)
{
	return ptr - BIGMALLOC_HEADER_SIZE;
}

static int stretch_file(int fd, size_t size)
{
	off64_t offset;
	ssize_t count;

	offset = lseek64(fd, size, SEEK_SET);
	if (offset != size) {
		warn("bigmalloc: lseek64(%d, %zu, SEEK_SET) = %lld",
			fd, size, (long long)offset);
		return -1;
	}

	count = write(fd, "", 1);
	if (count != 1) {
		warn("bigmalloc: write(%d, \"\", 1) = %zu", fd, count);
		return -1;
	}

	return 0;
}

static void xclose(int fd)
{
	int ret = close(fd);

	if (ret)
		warn("bigmalloc: close(%d)", fd);
}

static int overcommit_file(int fd, size_t size)
{
	struct statfs statfs;
	int ret;

	ret = fstatfs(fd, &statfs);
	if (ret) {
		warn("fstatfs(%d, &statfs) = %d", fd, ret);
		return 1;
	}
	if (size / statfs.f_bsize >= statfs.f_bavail)
		return 1;

	return 0;
}

void *bigmalloc(size_t size)
{
	int fd;
	int ret;
	struct bigmalloc_chunk *chunk;
	size_t total_size;

	fd = mktempfd();
	if (fd < 0)
		return NULL;

	total_size = size + BIGMALLOC_HEADER_SIZE;
	if (total_size < size)
		goto error;

	if (overcommit_file(fd, total_size))
		goto error;

	ret = stretch_file(fd, total_size);
	if (ret)
		goto error;

	chunk = mmap(NULL, total_size,
			PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0);
	if (chunk == MAP_FAILED)
		goto error;

	xclose(fd);
	chunk->magic = BIGMALLOC_MAGIC;
	chunk->size = total_size;

	return bigmalloc_chunk_to_mem(chunk);
error:
	xclose(fd);

	return NULL;
}

void bigfree(void *ptr)
{
	int ret;
	struct bigmalloc_chunk *chunk = mem_to_bigmalloc_chunk(ptr);

	if (chunk->magic != BIGMALLOC_MAGIC)
		err(EXIT_FAILURE, "bigfree: wrong magic %#x", chunk->magic);

	ret = munmap(chunk, chunk->size);
	if (ret)
		warn("bigfree: munmap(%p)", ptr);
}

// Test program

#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <limits.h>

static size_t bytes = INT_MAX;
static int use_malloc;

static void *BIGMALLOC(size_t size)
{
	if (use_malloc)
		return malloc(size);

	return bigmalloc(size);
}

static void BIGFREE(void *ptr)
{
	if (use_malloc)
		free(ptr);
	else
		bigfree(ptr);
}

static void parse_options(int argc, char **argv)
{
	int c;

	while ((c = getopt(argc, argv, "n:m")) != -1) {
		switch(c) {
		case 'n':
			bytes = atol(optarg);
			break;
		case 'm':
			use_malloc = 1;
			break;
		default:
			err(EXIT_FAILURE, "invalid option: %c", c);
		}
	}
}

int main(int argc, char **argv)
{
	void *ptr;
	size_t ret;

	parse_options(argc, argv);

	ptr = BIGMALLOC(bytes);
	if (!ptr)
		err(EXIT_FAILURE, "bigmalloc failed");

	ret = fread(ptr, 1, bytes, stdin);
	if (ret < 0)
		err(EXIT_FAILURE, "bigread failed");

	if (ret != bytes)
		err(EXIT_FAILURE, "oops ret (%zu) != bytes (%zd)", ret, bytes);

	ret = fwrite(ptr, 1, bytes, stdout);
	if (ret < 0)
		err(EXIT_FAILURE, "bigwrite failed");

	BIGFREE(ptr);

	return 0;
}

