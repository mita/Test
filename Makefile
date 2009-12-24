CFLAGS = -g -O2 -Wall

all: bigmalloc nullcached

bigmalloc: bigmalloc.c
	gcc $(CLAGS) -o bigmalloc $<

nullcached: nullcached.c
	gcc $(CLAGS) -o nullcached $<

clean:
	-rm -f bigmalloc nullcached
