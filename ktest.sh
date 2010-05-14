#!/bin/sh

MODULES="
backtracetest
mtd_nandecctest
test_rodata
testmmiotrace
tcrypt
kmemleak-test
"

for m in $MODULES; do
	modprobe $m
done

modprobe rcutorture
sleep 5 
modprobe -r rcutorture

(cd ~/scm/linux-2.6/scripts/rt-tester; ./check-all.sh)
