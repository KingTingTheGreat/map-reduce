package mr

import (
	"fmt"
	"io/ioutil"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

type Coordinator struct {
	// Your definitions here.
	Lock          *sync.Mutex
	NReduce       int
	Files         []string
	MapInput      []string
	MapInputFiles map[string]int
	MapOutput     map[string][]string
	MapDone       bool
	RedInput      []string
	RedOutput     map[string]string
	RedDone       bool
	MapStartTimes map[string]time.Time
	RedStartTimes map[string]time.Time
}

const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

// Your code here -- RPC handlers for the worker to call.

// an example RPC handler.
// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

func (c *Coordinator) HasFinishedMap() bool {
	// check if all map tasks are done
	for _, output := range c.MapOutput {
		if len(output) == 0 {
			return false
		}
	}
	c.MapDone = true
	return true
}

func (c *Coordinator) HasFinishedRed() bool {
	// check if all reduce tasks are done
	for _, output := range c.RedOutput {
		if len(output) == 0 {
			return false
		}
	}
	c.RedDone = true
	return true
}

func (c *Coordinator) MapTaskDone(args *ArgsMap, reply *Reply) error {
	DPrintf("Map task done: %v\n", args.Key)
	c.Lock.Lock()
	defer c.Lock.Unlock()
	// mark map task as done
	c.MapOutput[args.Key] = args.Filenames
	return nil
}

func (c *Coordinator) GetBucket(i int) []string {
	bucket := []string{}
	for j, _ := range c.Files {
		bucket = append(bucket, fmt.Sprintf("mr-%v-%v", j, i))
	}
	return bucket
}

func (c *Coordinator) RedTaskDone(args *ArgsRed, reply *Reply) error {
	DPrintf("Red task done: %v\n", args.Key)
	c.Lock.Lock()
	defer c.Lock.Unlock()
	// mark reduce task as done
	c.RedOutput[args.Key] = args.Filename
	return nil
}

func (c *Coordinator) GetTask(args *Args, reply *Reply) error {
	DPrintf("Called by worker, finding task to send\n")
	// only allow one call at the time
	c.Lock.Lock()
	defer c.Lock.Unlock()
	if !c.MapDone && !c.HasFinishedMap() {
		// stay in map phase until all map tasks are completed
		DPrintf("Finding map task\n")
		num := 0
		for filename, i := range c.MapInputFiles {
			// for _, filename := range c.MapInput {
			output := c.MapOutput[filename]
			// for filename, output := range c.MapOutput {
			// check that this task is not done and that it has not been started in the last 10 seconds
			if len(output) == 0 && time.Since(c.MapStartTimes[filename]) > 10*time.Second {
				reply.Key = filename
				reply.IsMap = true
				reply.NReduce = c.NReduce
				reply.MapTask = i
				c.MapStartTimes[filename] = time.Now()
				DPrintf("Sent map task %v\n", i)
				return nil
			}
			num++
		}
		// map tasks haven't finished yet but all are currently being worked on
		DPrintf("No map task to send\n")
		reply.Wait = true
		return nil
	} else if !c.RedDone && !c.HasFinishedRed() {
		// stay in this phase until all reduce tasks are completed
		DPrintf("Finding reduce task\n")
		for i, filename := range c.RedInput {
			output := c.RedOutput[filename]
			// check that this task is not done and that it has not been started in the last 10 seconds
			if len(output) == 0 && time.Since(c.RedStartTimes[filename]) > 10*time.Second {
				reply.Key = fmt.Sprintf("%v", filename)
				reply.IsMap = false
				reply.Keys = c.GetBucket(i)
				c.RedStartTimes[filename] = time.Now()
				DPrintf("Sent reduce task\n")
				return nil
			}
		}
		// reduce tasks haven't finished yet but all are currently being worked on
		DPrintf("No reduce task to send\n")
		// reply.Quit = true
		reply.Wait = true
		return nil
	} else {
		// all tasks are done
		DPrintf("All tasks are done\n")
		reply.Quit = true
		return nil
	}
}

// mr-main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	c.Lock.Lock()
	defer c.Lock.Unlock()
	return c.RedDone
}

// create a Coordinator.
// mr-main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	// set start times as 0
	InitTime := time.Time{}

	// initialize structures to keep track of map tasks
	MapInput := make([]string, len(files))
	MapInputFiles := make(map[string]int)
	MapOutput := make(map[string][]string)
	MapStartTimes := make(map[string]time.Time)
	for i, filename := range files {
		MapInput[i] = filename
		MapInputFiles[filename] = i
		MapOutput[filename] = []string{}
		MapStartTimes[filename] = InitTime
	}

	// initialize structures to keep track of reduce tasks
	RedInput := make([]string, nReduce)
	RedOutput := make(map[string]string)
	RedStartTimes := make(map[string]time.Time)
	for i := 0; i < nReduce; i++ {
		RedInput[i] = fmt.Sprintf("%v", i)
		RedOutput[fmt.Sprintf("%v", i)] = ""
		RedStartTimes[fmt.Sprintf("%v", i)] = InitTime
	}

	// initialize the coordinator
	c := Coordinator{
		&sync.Mutex{},
		nReduce,
		files,
		MapInput,
		MapInputFiles,
		MapOutput,
		false,
		RedInput,
		RedOutput,
		false,
		MapStartTimes,
		RedStartTimes,
	}

	// ensure files can be read and are not empty
	for _, filename := range files {
		log.Printf("file: %v\n", filename)
		file, err := os.Open(filename)
		if err != nil {
			log.Fatalf("cannot open %v", filename)
		}
		_, err = ioutil.ReadAll(file)
		if err != nil {
			log.Fatalf("cannot read %v", filename)
		}
		file.Close()
	}

	c.server()
	DPrintf("serving...\n")
	return &c
}

// start a thread that listens for RPCs from worker.go
// DO NOT MODIFY
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := coordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}
