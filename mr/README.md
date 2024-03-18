# Project 3: MapReduce

## Introduction

In this lab you'll build a MapReduce system. You'll implement a worker process that calls application Map and Reduce functions and handles reading and writing files, and a coordinator process that hands out tasks to workers and copes with failed workers. You'll be building something similar to the [MapReduce paper](http://static.googleusercontent.com/media/research.google.com/en//archive/mapreduce-osdi04.pdf).

## Getting started

Update your existing fork of the `starter-code` repo

```
git fetch upstream
git pull
```

We supply you with a simple sequential mapreduce implementation in `mr-main/mrsequential.go`. It runs the maps and reduces one at a time, in a single process. We also provide you with a couple of MapReduce applications: word-count in `mr-main/mrapps/wc.go`, and a text indexer in `mr-main/mrapps/indexer.go`. You can run word count sequentially as follows:

```
$ cd mr-main
$ go build -buildmode=plugin ./mrapps/wc.go
$ rm mr-out*
$ go run mrsequential.go wc.so ../data/pg*.txt
$ more mr-out-0
A 509
ABOUT 2
ACT 8
...
```

`mrsequential.go` leaves its output in the file `mr-out-0`. The input is from the text files named `pg-xxx.txt` in the `data` folder.

Feel free to borrow code from `mrsequential.go`. You should also have a look at `mr-main/mrapps/wc.go` to see what MapReduce application code looks like.

## Your Job

1. Your first task is to implement a distributed MapReduce, consisting of two programs, the coordinator and the worker. There will be just one coordinator process, and one or more worker processes executing in parallel. In a real system the workers would run on a bunch of different machines, but for this lab you'll run them all on a single machine. The workers will talk to the coordinator via RPC. Each worker process will ask the coordinator for a task, read the task's input from one or more files, execute the task, and write the task's output to one or more files. The coordinator should notice if a worker hasn't completed its task in a reasonable amount of time (for this lab, use ten seconds), and give the same task to a different worker.

2. Once your MapReduce implementation is working as expected, please proceed to [Task 2](#task-2).

## Task 1

We have given you a little code to start you off. The "main" routines for the coordinator and worker are in `mr-main/mrcoordinator.go` and `mr-main/mrworker.go`; **do NOT change these files**. You should put your implementation in `mr/coordinator.go`, `mr/worker.go`, and `mr/rpc.go`.

### Manully build & run your code

In order to run your code on a MapReduce application (e.g. word-count), please navigate to the `mr-main` directory. First, make sure the word-count plugin is freshly built:

```
$ go build -buildmode=plugin ../mr-main/mrapps/wc.go
```

Run the coordinator.

```
$ rm mr-out*
$ go run mrcoordinator.go ../data/pg-*.txt
```

The `../data/pg-*.txt` arguments to `mrcoordinator.go` are the input files; each file corresponds to one "split", and is the input to one Map task.

In one or more **other** windows/terminals, run some workers:

```
$ go run mrworker.go wc.so
```

When the workers and coordinator have finished, look at the output in `mr-out-*`. When you've completed the lab, the sorted union of the output files should match the sequential output, like this:

```
$ cat mr-out-* | sort | more
A 509
ABOUT 2
ACT 8
...
```

### Test your code

We supply you with a test script in `mr-main/test-mr.sh`. The tests check that the `wc` and `indexer` MapReduce applications produce the correct output when given the `pg-xxx.txt` files as input. The tests also check that your implementation runs the Map and Reduce tasks in parallel, and that your implementation recovers from workers that crash while running tasks.

If you run the test script now, it will hang because the coordinator never finishes:

```
$ cd mr-main
$ bash test-mr.sh
*** Starting wc test.
```

You can change `ret := false` to true in the `Done` function in `mr/coordinator.go` so that the coordinator exits immediately. Then:

```
$ bash ./test-mr.sh
*** Starting wc test.
sort: No such file or directory
cmp: EOF on mr-wc-all
--- wc output is not the same as mr-correct-wc.txt
--- wc test: FAIL
```

The test script expects to see output in files named `mr-out-X`, one for each reduce task. The empty implementations of `mr/coordinator.go` and `mr/worker.go` don't produce those files (or do much of anything else), so the test fails.

When you've finished, the test script output should look like this:

```
$ bash ./test-mr.sh
*** Starting wc test.
--- wc test: PASS
*** Starting indexer test.
--- indexer test: PASS
*** Starting map parallelism test.
--- map parallelism test: PASS
*** Starting reduce parallelism test.
--- reduce parallelism test: PASS
*** Starting crash test.
--- crash test: PASS
*** PASSED ALL TESTS
```

You'll also see some errors from the Go RPC package that look like

```
2019/12/16 13:27:09 rpc.Register: method "Done" has 1 input parameters; needs exactly three
```

Ignore these messages.

### A few rules:

- The map phase should divide the intermediate keys into buckets for `nReduce` reduce tasks, where `nReduce` is the argument that `mr-main/mrcoordinator.go` passes to `MakeCoordinator()`.
- The worker implementation should put the output of the X'th reduce task in the file `mr-out-X`.
- A `mr-out-X` file should contain one line per Reduce function output. The line should be generated with the Go `"%v %v"` format, called with the key and value. Have a look in `mr-main/mrsequential.go` for the line commented "this is the correct format". The test script will fail if your implementation deviates too much from this format.
- You can modify `mr/worker.go`, `mr/coordinator.go`, and `mr/rpc.go`. You can temporarily modify other files for testing, but make sure your code works with the original versions; we'll test with the original versions.
- The worker should put intermediate Map output in files in the current directory, where your worker can later read them as input to Reduce tasks.
- `mr-main/mrcoordinator.go` expects `mr/coordinator.go` to implement a `Done()` method that returns true when the MapReduce job is completely finished; at that point, `mrcoordinator.go` will exit.
- When the job is completely finished, the worker processes should exit. A simple way to implement this is to use the return value from `call()`: if the worker fails to contact the coordinator, it can assume that the coordinator has exited because the job is done, and so the worker can terminate too. Depending on your design, you might also find it helpful to have a "please exit" pseudo-task that the coordinator can give to workers.

## Task 2

You are required to implement a MapReduce application [credit.go](../mr-main/mrapps/credit.go), just like the others seen in the `mrapps` folder. For this application, you are given the input dataset of the form:

| User ID | Agency | Year | Credit Score |
| --- | --- | --- | --- |
| 64 | Equifax | 2023 | 660
| 128 | TransUnion | 2021 | 380

Your goal is to compute the total number of people per agency whose credit score in 2023 was larger than 400. 

This data is present as CSV files in `data/credit-score/`. The expected output for the given data is shown below, but your code may be tested against multiple **different data sets** upon submission.

```
Equifax: 648
Experian: 671
TransUnion: 659
Yellow Banana: 677
```

You can use a script in `mr-main/test-mr-app.sh` to test your MR application. Please refer to the [test script](../mr-main/test-mr-app.sh) for more info.

## Hints

- One way to get started is to modify `mr/worker.go`'s `Worker()` to send an RPC to the coordinator asking for a task. Then modify the coordinator to respond with the file name of an as-yet-unstarted map task. Then modify the worker to read that file and call the application Map function, as in `mrsequential.go`.
- The application Map and Reduce functions are loaded at run-time using the Go plugin package, from files whose names end in `.so`.
- If you change anything in the `mr/` directory, you will probably have to re-build any MapReduce plugins you use, with something like `go build -buildmode=plugin ../mrapps/wc.go`.
- This lab relies on the workers sharing a file system. That's straightforward when all workers run on the same machine, but would require a global filesystem like GFS if the workers ran on different machines.
- A reasonable naming convention for intermediate files is `mr-X-Y`, where `X` is the Map task number, and `Y` is the reduce task number.
- The worker's map task code will need a way to store intermediate key/value pairs in files in a way that can be correctly read back during reduce tasks. One possibility is to use Go's `encoding/json` package. To write key/value pairs to a JSON file:

```
  enc := json.NewEncoder(file)
  for _, kv := ... {
    err := enc.Encode(&kv)
```

and to read such a file back:

```
  dec := json.NewDecoder(file)
  for {
    var kv KeyValue
    if err := dec.Decode(&kv); err != nil {
      break
    }
    kva = append(kva, kv)
  }
```

- The map part of your worker can use the `ihash(key)` function (in `worker.go`) to pick the reduce task for a given key.
- You can steal some code from `mrsequential.go` for reading Map input files, for sorting intermedate key/value pairs between the Map and Reduce, and for storing Reduce output in files.
- The coordinator, as an RPC server, will be concurrent; don't forget to lock shared data.
- Use Go's race detector, with `go build -race` and `go run -race`. `test-mr.sh` has a comment that shows you how to enable the race detector for the tests.
- Workers will sometimes need to wait, e.g. reduces can't start until the last map has finished. One possibility is for workers to periodically ask the coordinator for work, sleeping with `time.Sleep()` between each request. Another possibility is for the relevant RPC handler in the coordinator to have a loop that waits, either with `time.Sleep()` or `sync.Cond`. Go runs the handler for each RPC in its own thread, so the fact that one handler is waiting won't prevent the coordinator from processing other RPCs.
- The coordinator can't reliably distinguish between crashed workers, workers that are alive but have stalled for some reason, and workers that are executing but too slowly to be useful. The best you can do is have the coordinator wait for some amount of time, and then give up and re-issue the task to a different worker. For this lab, have the coordinator wait for ten seconds; after that the coordinator should assume the worker has died (of course, it might not have).
- To test crash recovery, you can use the `mrapps/crash.go` application plugin. It randomly exits in the Map and Reduce functions.
- To ensure that nobody observes partially written files in the presence of crashes, the MapReduce paper mentions the trick of using a temporary file and atomically renaming it once it is completely written. You can use `ioutil.TempFile` to create a temporary file and `os.Rename` to atomically rename it.
- `test-mr.sh` runs all the processes in the sub-directory `mr-tmp`, so if something goes wrong and you want to look at intermediate or output files, look there.
- If you are a Windows user and use WSL, you might have to do `dos2unix test-mr.sh` before running the test script (do this in case you get weird errors when run `bash test-mr.sh`).

## Handin procedure

**Important: Before submitting, please run `test-mr.sh` one final time.**

Your code would be submitted on Gradescope.
As usual, make sure to frequently commit all of your code to Gitlab as you work on the assignment

**Good luck!**
