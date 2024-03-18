#!/bin/bash

# basic map-reduce test

# comment this to run the tests without the Go race detector.
RACE=-race

# run the test in a fresh sub-directory.
rm -rf mr-tmp
mkdir mr-tmp || exit 1
cd mr-tmp || exit 1
rm -f mr-*

echo "Building MR apps..."

(cd ../mrapps && go build $RACE -buildmode=plugin credit.go) || exit 1
(cd .. && go build $RACE mrcoordinator.go) || exit 1
(cd .. && go build $RACE mrworker.go) || exit 1
(cd .. && go build $RACE mrsequential.go) || exit 1

echo "Starting credit score test."

rm -f mr-*

timeout -k 2s 180s ../mrcoordinator ../../data/credit-score/*.csv &
pid=$!

sleep 1

timeout -k 2s 180s ../mrworker ../mrapps/credit.so &
timeout -k 2s 180s ../mrworker ../mrapps/credit.so

wait $pid

sort mr-out* | grep . > mr-credit-all
if cmp mr-credit-all ../../data/credit-score/gt.txt
then
  echo '---' credit score test: PASS
else
  echo '---' credit score test: FAIL
fi
