CFLAGS = -g -O2 -Wall
TARGETS = bigmalloc nullcached getsockipmtu

all: $(TARGETS)

bigmalloc: bigmalloc.c
	gcc $(CFLAGS) -o $@ $<

nullcached: nullcached.c
	gcc $(CFLAGS) -o $@ $<

getsockipmtu: getsockipmtu.c
	gcc $(CFLAGS) -o $@ $<

clean:
	-rm -f $(TARGETS)
