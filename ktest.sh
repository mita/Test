#!/bin/sh

MODULES="
backtracetest
mtd_nandecctest
test_rodata
testmmiotrace
tcrypt
"

for m in $MODULES; do
	modprobe $m
done
