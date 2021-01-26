package mr

import (
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"sort"
	"strconv"
	"strings"
	"time"
)

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

var workerName string

//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {
	getTaskResponse := getTask()
	for true {
		// retry if get wait notify from master
		retryCount := 0
		for retryCount < 100 && getTaskResponse.TaskType == WaitTask {
			time.Sleep(time.Second)
			log.Printf("Try get task again, count %v", retryCount)
			getTaskResponse = getTask()
			retryCount++
		}
		if retryCount == 100 {
			log.Fatal("Could not get task from master!")
			return
		}
		switch getTaskResponse.TaskType {
		case MapTask:
			// reuse workername which is already get from master
			if workerName == "" {
				workerName = getTaskResponse.WorkerName
			}
			strArr := strings.SplitN(getTaskResponse.Filename, strSpliter, 2)
			filename := strArr[1]
			index := strArr[0]
			file, err := os.Open(filename)
			if err != nil {
				log.Fatalf("Could not open %v", filename)
				finishTask(MapTask, getTaskResponse.WorkerName, Failed)
				return
			}
			content, err := ioutil.ReadAll(file)
			if err != nil {
				log.Fatalf("Could not read %v", filename)
				finishTask(MapTask, getTaskResponse.WorkerName, Failed)
				return
			}
			//ready content
			kva := mapf(filename, string(content))
			log.Printf("finish map %v", getTaskResponse.Filename)
			sort.Sort(ByKey(kva))
			reduceNum := getTaskResponse.ReduceNum
			intermediateFiles := make([]*os.File, reduceNum)
			// write file to different reduce files
			for i := 0; i < reduceNum; i++ {
				intermediateFileName := fmt.Sprintf("mr%v%v%v%v", strSpliter, index, strSpliter, i)
				intermediateFiles[i], _ = os.Create(intermediateFileName)
			}
			i := 0
			for i < len(kva) {

				j := i + 1
				for j < len(kva) && kva[j].Key == kva[i].Key {
					j++
				}
				keys := []string{}
				for k := i; k < j; k++ {
					keys = append(keys, kva[k].Key+" "+kva[k].Value)
				}
				outStr := strings.Join(keys[:], "\n")
				fmt.Fprintf(intermediateFiles[ihash(kva[i].Key)%reduceNum], "%v\n", outStr)
				i = j
			}
			for i := 0; i < reduceNum; i++ {
				intermediateFiles[i].Close()
			}
			log.Printf("finish map task %v", getTaskResponse.Filename)
			finishTask(MapTask, getTaskResponse.WorkerName, Done)
		case ReduceTask:
			if workerName == "" {
				workerName = getTaskResponse.WorkerName
			}
			log.Printf("get reduce task %v", getTaskResponse)
			mapNum := getTaskResponse.MapNum
			strArr := strings.Split(getTaskResponse.Filename, strSpliter)
			reduceIndex := strArr[2]
			intermediate := []KeyValue{}
			for i := 0; i < mapNum; i++ {
				mapResultFileName := "mr" + strSpliter + strconv.Itoa(i) + strSpliter + reduceIndex
				file, err := os.Open(mapResultFileName)
				if err != nil {
					log.Fatalf("Could not open %v", mapResultFileName)
					finishTask(ReduceTask, getTaskResponse.WorkerName, Failed)
					return
				}
				content, err := ioutil.ReadAll(file)
				if err != nil {
					log.Fatalf("Could not read %v", mapResultFileName)
					finishTask(ReduceTask, getTaskResponse.WorkerName, Failed)
					return
				}
				contentArr := strings.Split(string(content), "\n")
				for _, element := range contentArr {
					if element == "" {
						continue
					}
					keyValue := KeyValue{}
					elementArr := strings.Split(element, " ")
					keyValue.Key = elementArr[0]
					keyValue.Value = elementArr[1]
					intermediate = append(intermediate, keyValue)
				}
			}
			sort.Sort(ByKey(intermediate))
			ofile, _ := os.Create(getTaskResponse.Filename)
			i := 0
			for i < len(intermediate) {
				j := i + 1
				for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
					j++
				}
				values := []string{}
				for k := i; k < j; k++ {
					values = append(values, intermediate[k].Value)
				}
				output := reducef(intermediate[i].Key, values)

				// this is the correct format for each line of Reduce output.
				fmt.Fprintf(ofile, "%v %v\n", intermediate[i].Key, output)
				i = j
			}
			log.Printf("finish reduce task %v", getTaskResponse)
			finishTask(ReduceTask, getTaskResponse.WorkerName, Done)
		case NoTask:
			log.Println("there is no task, retry!")
			time.Sleep(time.Second)
		case FinishTask:
			log.Println("Get notify, all task finish!")
			return
		default:
			log.Fatal("Unsupport task type! Retry get task.")
			time.Sleep(time.Second)
		}
		getTaskResponse = getTask()
	}
}

func getTask() *GetTaskResponse {
	getTaskRequest := GetTaskRequest{}
	if workerName != "" {
		getTaskRequest.WorkerName = workerName
	}
	getTaskResponse := GetTaskResponse{}
	call("Master.GetTask", &getTaskRequest, &getTaskResponse)
	log.Printf("getTaskResponse: %v", getTaskResponse)
	return &getTaskResponse
}

func finishTask(taskType TaskType, workerName string, result TaskResult) *TaskFinishResponse {
	request := TaskFinishRequest{
		TaskType:   taskType,
		WorkerName: workerName,
		Result:     result,
	}
	response := TaskFinishResponse{}
	call("Master.FinishTask", request, response)
	return &response
}

//
// example function to show how to make an RPC call to the master.
//
// the RPC argument and reply types are defined in rpc.go.
//
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	call("Master.Example", &args, &reply)

	// reply.Y should be 100.
	fmt.Printf("reply.Y %v\n", reply.Y)
}

//
// send an RPC request to the master, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := masterSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}
