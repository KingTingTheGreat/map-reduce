package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import (
	"os"
	"strconv"
)

// example to show how to declare the arguments
// and reply for an RPC.
type ExampleArgs struct {
	X int
}
type ExampleReply struct {
	Y int
}

// Add your RPC definitions here.
type Args struct {
	Key   string
	Value string
}

type ArgsMap struct {
	Key       string
	Filenames []string
}

type ArgsRed struct {
	Key      string
	Filename string
}

type Reply struct {
	Key     string
	Keys    []string
	MapTask int
	NReduce int
	IsMap   bool
	Quit    bool
	Wait    bool
}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
