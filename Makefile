CFLAGS = -g -O2 -Wall -I $(HOME)/include -L $(HOME)/lib
TARGETS = bigmalloc nullcached getsockipmtu echoline cat memcached-benchmark

all: $(TARGETS)

bigmalloc: bigmalloc.c
	gcc $(CFLAGS) -o $@ $<

nullcached: nullcached.c
	gcc $(CFLAGS) -o $@ $<

getsockipmtu: getsockipmtu.c
	gcc $(CFLAGS) -o $@ $<

echoline: echoline.c
	gcc $(CFLAGS) -o $@ $<

cat: cat.c
	gcc $(CFLAGS) -o $@ $<

memcached-benchmark: memcached-benchmark.c
	gcc $(CFLAGS) -o $@ $< -lmemcached

clean:
	-rm -f $(TARGETS)
