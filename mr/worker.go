package mr

import (
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"sort"
	"strings"
	"sync"
)

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

func WriteIntermediate(filename string, data []string, writers *sync.WaitGroup) {
	// prevents overwriting map that was already done
	if _, err := os.Stat(filename); err == nil {
		DPrintf("file %v already exists\n", filename)
		writers.Done()
		return
	}
	file, err := os.Create(filename)
	if err != nil {
		log.Fatalf("cannot create %v", filename)
	}
	for i := 0; i < len(data); i += 2 {
		fmt.Fprintf(file, "%v\t\t%v\n\n", data[i], data[i+1])
	}
	// fmt.Fprintf(file, "%v", data)
	file.Close()
	DPrintf("wrote to file %v\n", filename)
	writers.Done()
}

func DoMapTask(mapf func(string, string) []KeyValue, Key string, NReduce int, MapTask int) {
	DPrintf("Starting map task: %v\n", Key)
	// read file
	file, err := os.Open(Key)
	if err != nil {
		log.Fatalf("cannot open %v", Key)
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", Key)
	}
	file.Close()
	DPrintf("performing map...\n")
	kva := mapf(Key, string(content))

	outputFilenames := []string{}
	// create intermediate files
	DPrintf("processing %v key-value pairs...\n", len(kva))
	fileData := make(map[string][]string)
	for _, kv := range kva {
		reduceTask := ihash(kv.Key) % NReduce
		outputFilename := fmt.Sprintf("mr-%v-%v", MapTask, reduceTask)
		fileData[outputFilename] = append(fileData[outputFilename], kv.Key, kv.Value)
	}
	DPrintf("created %v intermediate files\n", len(fileData))
	writers := sync.WaitGroup{}
	for filename, data := range fileData {
		writers.Add(1)
		go WriteIntermediate(filename, data, &writers)
		outputFilenames = append(outputFilenames, filename)
	}
	writers.Wait()
	args := ArgsMap{Key: Key, Filenames: outputFilenames}
	DPrintf("sending map task done\n")
	call("Coordinator.MapTaskDone", &args, &Reply{})
}

func DoRedTask(reducef func(string, []string) string, Key string, Keys []string) {
	DPrintf("Starting reduce task: %v\n", Key)
	// read intermediate files and put into key-value pairs
	kva := []KeyValue{}
	for _, filename := range Keys {
		file, err := os.Open(filename)
		if err != nil {
			// continue because not all intermediate files may exist
			// it is possible for a key to not have any intermediate files
			continue
		}
		content, err := ioutil.ReadAll(file)
		if err != nil {
			log.Fatalf("cannot read %v", filename)
		}
		file.Close()
		lines := strings.Split(string(content), "\n\n")
		for _, line := range lines {
			if line == "" {
				continue
			}
			parts := strings.Split(line, "\t\t")
			kva = append(kva, KeyValue{Key: parts[0], Value: parts[1]})
		}
	}

	// sort key-value pairs
	sort.Sort(ByKey(kva))

	// generate output filename
	outputFilename := fmt.Sprintf("mr-out-%v", Key)
	// check if this file already exists and has content
	if prevFile, err := os.Stat(outputFilename); err == nil && prevFile.Size() > 0 {
		return
	}
	// initialize variables
	outputFile, err := os.Create(outputFilename)
	if err != nil {
		log.Fatalf("cannot create %v", outputFilename)
	}

	// get values for each key
	values := []string{}
	for i := 0; i < len(kva); i++ {
		if i == 0 || kva[i].Key == kva[i-1].Key {
			values = append(values, kva[i].Value)
		} else {
			// perform reduce task
			outputData := reducef(kva[i-1].Key, values)
			// initialize values for next(current) key
			values = []string{kva[i].Value}
			// send output to file
			fmt.Fprintf(outputFile, "%v %v\n", kva[i-1].Key, outputData)
		}
	}
	if len(values) > 0 {
		// perform reduce task on last key
		outputData := reducef(kva[len(kva)-1].Key, values)
		// send output to file
		fmt.Fprintf(outputFile, "%v %v\n", kva[len(kva)-1].Key, outputData)
	}
	outputFile.Close()

	// if file is empty, delete it
	fileInfo, err := os.Stat(outputFilename)
	if err != nil {
		log.Fatalf("cannot stat %v", outputFilename)
	}
	if fileInfo.Size() == 0 {
		os.Remove(outputFilename)
	}

	args := ArgsRed{Key: Key, Filename: outputFilename}
	call("Coordinator.RedTaskDone", &args, &Reply{})
}

// mr-main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {
	DPrintf("initialized worker\n")
	for {
		// repeatedly call coordinator to get reduce task
		DPrintf("calling coordinator\n")
		args := Args{}
		reply := Reply{}
		success := call("Coordinator.GetTask", &args, &reply)
		if !success {
			DPrintf("failed to get task\n")
			continue
		} else if reply.Wait {
			DPrintf("waiting...\n")
			continue
		} else if reply.Quit {
			DPrintf("quit\n")
			break
		} else if reply.IsMap {
			// perform map task
			DoMapTask(mapf, reply.Key, reply.NReduce, reply.MapTask)
		} else {
			// perform reduce task
			DoRedTask(reducef, reply.Key, reply.Keys)
		}
	}
}

// example function to show how to make an RPC call to the coordinator.
// the RPC argument and reply types are defined in rpc.go.
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	call("Coordinator.Example", &args, &reply)

	// reply.Y should be 100.
	fmt.Printf("reply.Y %v\n", reply.Y)
}

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
// DO NOT MODIFY
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println("Unable to Call", rpcname, "- Got error:", err)
	return false
}
