#!/bin/sh

MODULES="
backtracetest
mtd_nandecctest
test_rodata
testmmiotrace
tcrypt
kmemleak-test
raid6test
dmatest
ring_buffer_benchmark
"

#
# kernel modules
#
for m in $MODULES; do
	modprobe $m
done

modprobe rcutorture
sleep 5 
modprobe -r rcutorture

(cd ~/scm/linux-2.6/scripts/rt-tester; ./check-all.sh)

#
# userspace tests
#
make -C drivers/md/raid6test/
./drivers/md/raid6test/raid6test

#
# todo
#
cat << EOF > /dev/null
test_suspend
test_kprobes
memtest
pageattr-test
kmemcheck/selftest
atomic64_test
trace_selftest
drivers/net/sfc/selftest.c
Documentations
EOF
