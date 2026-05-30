#!/usr/bin/env bash
set -e

SRC_MAIN=src/main/java
SRC_TEST=src/test/java
OUT=out

echo "=== Compiling ==="
mkdir -p $OUT
find $SRC_MAIN $SRC_TEST -name "*.java" | xargs javac --release 17 -d $OUT

echo ""
echo "=== Running Tests ==="
java -cp $OUT rx.RxTests

echo ""
echo "=== Running Demo ==="
java -cp $OUT demo.Demo
