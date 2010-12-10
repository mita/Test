CC = gcc
CFLAGS = -g -O2 -Wall -I $(HOME)/include
CXX = g++44
CXXFLAGS = -g -O2 -Wall -Wno-sign-compare -I$(HOME)/include
LDFLAGS = -L $(HOME)/lib
TARGETS = bigmalloc nullcached getsockipmtu echoline cat memcached-benchmark \
		chunkd-benchmark multimap-memcachedb-test tokyocabinettest \
		berkeleydbtest tokyotyranttest kyototycoontest

all: $(TARGETS)

bigmalloc: bigmalloc.c
	$(CC) $(CFLAGS) $(LDFLAGS) -o $@ $<

nullcached: nullcached.c
	$(CC) $(CFLAGS) $(LDFLAGS) -o $@ $<

getsockipmtu: getsockipmtu.c
	$(CC) $(CFLAGS) $(LDFLAGS) -o $@ $<

echoline: echoline.c
	$(CC) $(CFLAGS) $(LDFLAGS) -o $@ $<

cat: cat.c
	$(CC) $(CFLAGS) $(LDFLAGS) -o $@ $<

memcached-benchmark: memcached-benchmark.c
	$(CC) $(CFLAGS) $(LDFLAGS) -o $@ $< -lmemcached

chunkd-benchmark: memcached-benchmark.c
	$(CC) $(CFLAGS) $(LDFLAGS) $(shell pkg-config glib-2.0 --cflags) \
		-DCHUNKD_BENCHMARK -o $@ $< -lpthread -lxml2 -lchunkdc -lssl \
		$(shell pkg-config glib-2.0 gio-2.0 --libs)

multimap-memcachedb-test: multimap-memcachedb-test.c
	$(CC) $(CFLAGS) $(LDFLAGS) -o $@ $< -lmemcached

testutil.o: testutil.c testutil.h
	$(CC) $(CFLAGS) -c $<

tokyocabinettest: tokyocabinettest.c testutil.o
	$(CC) $(CFLAGS) $(LDFLAGS) -o $@ $< testutil.o -ltokyocabinet

berkeleydbtest: berkeleydbtest.c testutil.o
	$(CC) $(CFLAGS) $(LDFLAGS) -o $@ $< testutil.o -ldb -ltokyocabinet

tokyotyranttest: tokyotyranttest.c testutil.o
	$(CC) $(CFLAGS)  $(LDFLAGS) -o $@ $<  testutil.o -ltokyotyrant -ltokyocabinet

kyototycoontest: kyototycoontest.cc testutil.o
	$(CXX) $(CXXFLAGS) $(LDFLAGS) -o $@ $<  testutil.o -lkyototycoon -ltokyocabinet

clean:
	-rm -f $(TARGETS) *.o
