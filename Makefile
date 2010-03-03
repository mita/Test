CC = gcc
CFLAGS = -g -O2 -Wall -I $(HOME)/include -L $(HOME)/lib
TARGETS = bigmalloc nullcached getsockipmtu echoline cat memcached-benchmark \
		chunkd-benchmark multimap-memcachedb-test tokyocabinet-test \
		berkeleydb-test tokyotyrant-test

all: $(TARGETS)

bigmalloc: bigmalloc.c
	$(CC) $(CFLAGS) -o $@ $<

nullcached: nullcached.c
	$(CC) $(CFLAGS) -o $@ $<

getsockipmtu: getsockipmtu.c
	$(CC) $(CFLAGS) -o $@ $<

echoline: echoline.c
	$(CC) $(CFLAGS) -o $@ $<

cat: cat.c
	$(CC) $(CFLAGS) -o $@ $<

memcached-benchmark: memcached-benchmark.c
	$(CC) $(CFLAGS) -o $@ $< -lmemcached

chunkd-benchmark: memcached-benchmark.c
	$(CC) $(CFLAGS) $(shell pkg-config glib-2.0 --cflags) \
		-DCHUNKD_BENCHMARK -o $@ $< -lpthread -lxml2 -lchunkdc -lssl \
		$(shell pkg-config glib-2.0 gio-2.0 --libs)

multimap-memcachedb-test: multimap-memcachedb-test.c
	$(CC) $(CFLAGS) -o $@ $< -lmemcached

tokyocabinet-test: tokyocabinet-test.c
	$(CC) $(CFLAGS) -o $@ $< -ltokyocabinet

berkeleydb-test: berkeleydb-test.c
	$(CC) $(CFLAGS) -o $@ $< -ldb

tokyotyrant-test: tokyotyrant-test.c
	$(CC) $(CFLAGS) -o $@ $< -ltokyotyrant -ltokyocabinet

clean:
	-rm -f $(TARGETS)
