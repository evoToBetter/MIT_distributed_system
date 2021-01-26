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

const strSpliter = "-"

//
// example to show how to declare the arguments
// and reply for an RPC.
//
type TaskType int

const (
	NoTask TaskType = iota
	ReduceTask
	WaitTask
	MapTask
	FinishTask
)

type TaskResult int

const (
	Failed TaskResult = iota
	Ongoing
	Done
)

type GetTaskRequest struct {
	WorkerName string
}
type GetTaskResponse struct {
	TaskType   TaskType
	Filename   string
	WorkerName string
	MapNum     int
	ReduceNum  int
}

type TaskFinishRequest struct {
	TaskType   TaskType
	Result     TaskResult
	WorkerName string
}

type TaskFinishResponse struct {
	TaskType TaskType
}

type ExampleArgs struct {
	X int
}

type ExampleReply struct {
	Y int
}

// Add your RPC definitions here.

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the master.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func masterSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
