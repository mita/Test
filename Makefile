CFLAGS = -g -O2 -Wall

all: bigmalloc nullcached

bigmalloc: bigmalloc.c
	gcc $(CFLAGS) -o bigmalloc $<

nullcached: nullcached.c
	gcc $(CFLAGS) -o nullcached $<

clean:
	-rm -f bigmalloc nullcached
