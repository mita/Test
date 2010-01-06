CC = gcc
CFLAGS = -g -O2 -Wall -I $(HOME)/include -L $(HOME)/lib
TARGETS = bigmalloc nullcached getsockipmtu echoline cat memcached-benchmark \
		chunkd-benchmark

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
	$(CC) $(CFLAGS) -I $(HOME)/include/glib-2.0 -I $(HOME)/lib/glib-2.0/include -DCHUNKD_BENCHMARK -o $@ $< -lpthread -lxml2 -lchunkdc -lssl -lglib-2.0

clean:
	-rm -f $(TARGETS)
