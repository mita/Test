#include <stdbool.h>
#include <pthread.h>

/*
 * No error check wrapper functions
 */
void die(const char *err, ...);
void *xmalloc(size_t size);
void xpthread_create(pthread_t *thread, void *(*routine)(void *), void *arg);
void xpthread_join(pthread_t th);

/*
 * Key generator library
 */
#define KEYGEN_KEY_SIZE sizeof("0x0000000000000000-0x0000000000000000")
#define KEYGEN_PREFIX_SIZE (sizeof("0x0000000000000000-") - 1)

struct keygen {
	unsigned int prefix;
	char key[KEYGEN_KEY_SIZE];
	unsigned int (*next)(unsigned int *state);
	unsigned int seed;
};

char *keygen_next_key(struct keygen *keygen);
char *keygen_prefix(struct keygen *keygen, char *buf);
void keygen_set_generator(const char *generator);
void keygen_init(struct keygen *keygen, unsigned int seed);

/*
 * Benchmark utilities
 */

struct benchmark_config;

struct benchmark_operations {
	void *(*open_db)(struct benchmark_config *config);
	void (*close_db)(void *db);
	void (*put_test)(void *db, int num, int vsiz, unsigned int seed);
	void (*get_test)(void *db, int num, int vsiz, unsigned int seed);
	void (*putlist_test)(void *db, const char *command, int num, int vsiz,
				int batch, unsigned int seed);
	void (*fwmkeys_test)(void *db, int num, unsigned int seed);
	void (*getlist_test)(void *db, const char *command, int num, int vsiz,
				int batch, unsigned int seed);
	void (*range_test)(void *db, const char *command, int num, int vsiz,
				int batch, unsigned int seed);
	void (*outlist_test)(void *db, const char *command, int num, int batch,
				unsigned int seed);
};

struct benchmark_config {
	const char *producer;
	const char *consumer;
	const char *host;
	const char *path;
	int port;
	int num;
	int vsiz;
	unsigned int seed_offset;
	int batch;
	int thnum;
	int num_works;
	bool debug;
	int share;
	struct benchmark_operations ops;
};

void parse_options(struct benchmark_config *config, int argc, char **argv);
void benchmark(struct benchmark_config *config);
