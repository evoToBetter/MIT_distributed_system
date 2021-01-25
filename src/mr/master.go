package mr

import (
	"container/list"
	"log"
	"math"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"strconv"
	"sync"
	"time"
)

type Master struct {
	// Your definitions here.
	mapTasks           *list.List
	reduceTasks        *list.List
	mapRunningTasks    map[string]string
	reduceRunningTasks map[string]string
	mapTaskLock        sync.Mutex
	reduceTaskLock     sync.Mutex
	mapNum             int
	reduceNum          int
	workerIndex        int
	workerIndexLock    sync.Mutex
}

// Your code here -- RPC handlers for the worker to call.

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (m *Master) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

func (m *Master) GetTask(request *GetTaskRequest, response *GetTaskResponse) error {
	log.Println("enter get task!")
	filename, workerName := m.getMapTask()
	log.Printf("get map task, filename is %v!", filename)
	if filename != "" {
		response.TaskType = MapTask
		response.Filename = filename
		response.WorkerName = workerName
		response.ReduceNum = m.reduceNum
		log.Printf("get one map task %v!", response)
		go m.waitTaskFinish(workerName, MapTask)
		return nil
	} else {
		if !m.mapTaskFinish() {
			response.TaskType = WaitTask
			return nil
		}
	}
	filename, workerName = m.getReduceTask()
	log.Printf("get reduce task, filename is %v!", filename)
	if filename != "" {
		response.TaskType = ReduceTask
		response.Filename = filename
		response.WorkerName = workerName
		response.MapNum = m.mapNum
		log.Printf("get one reduce task %v!", response)
		go m.waitTaskFinish(workerName, ReduceTask)
	} else if !m.Done() {
		response.TaskType = WaitTask
	} else {
		response.TaskType = FinishTask
	}
	return nil
}

func (m *Master) FinishTask(request *TaskFinishRequest, response *TaskFinishResponse) error {
	if request.TaskType == MapTask {
		m.mapTaskLock.Lock()
		delete(m.mapRunningTasks, request.WorkerName)
		m.mapTaskLock.Unlock()
		response.TaskType = MapTask
		return nil
	} else if request.TaskType == ReduceTask {
		m.reduceTaskLock.Lock()
		delete(m.reduceRunningTasks, request.WorkerName)
		m.reduceTaskLock.Unlock()
		response.TaskType = ReduceTask
		return nil
	} else {
		response.TaskType = NoTask
		return nil
	}
}

func (m *Master) waitTaskFinish(workerName string, taskType TaskType) {
	time.Sleep(10 * time.Second)
	if taskType == MapTask {
		m.mapTaskLock.Lock()
		if _, ok := m.mapRunningTasks[workerName]; ok {
			m.returnMapTask(workerName)
		} else {
			log.Println("map task finish by: " + workerName)
		}
		m.mapTaskLock.Unlock()
	} else if taskType == ReduceTask {
		m.reduceTaskLock.Lock()
		if _, ok := m.reduceRunningTasks[workerName]; ok {
			m.returnReduceTask(workerName)
		}
		m.reduceTaskLock.Unlock()
	} else {
		return
	}
}

func (m *Master) getMapTask() (string, string) {
	var filename string
	var workerName string
	m.mapTaskLock.Lock()
	if m.mapTasks.Len() > 0 {
		firstEle := m.mapTasks.Front()
		filename = firstEle.Value.(string)
		m.mapTasks.Remove(firstEle)
		workerName = m.getWorkerName()
		m.mapRunningTasks[workerName] = filename
		log.Printf("current mapRunningTask is %v", m.mapRunningTasks)
	}
	m.mapTaskLock.Unlock()
	log.Printf("get map task %v for %v", filename, workerName)
	return filename, workerName
}

func (m *Master) mapTaskFinish() bool {
	ret := false
	m.mapTaskLock.Lock()
	if m.mapTasks.Len() == 0 && len(m.mapRunningTasks) == 0 {
		ret = true
	}
	m.mapTaskLock.Unlock()
	return ret
}

func (m *Master) returnMapTask(workerName string) {

	filename := m.mapRunningTasks[workerName]
	delete(m.mapRunningTasks, workerName)
	m.mapTasks.PushBack(filename)

}

func (m *Master) getReduceTask() (string, string) {
	var filename string
	var workerName string
	m.reduceTaskLock.Lock()
	if m.reduceTasks.Len() > 0 {
		firstEle := m.reduceTasks.Front()
		filename = firstEle.Value.(string)
		m.reduceTasks.Remove(firstEle)
		workerName = m.getWorkerName()
		m.reduceRunningTasks[workerName] = filename
	}
	m.reduceTaskLock.Unlock()
	return filename, workerName
}

func (m *Master) returnReduceTask(workerName string) {

	filename := m.reduceRunningTasks[workerName]
	delete(m.reduceRunningTasks, workerName)
	m.reduceTasks.PushBack(filename)

}

func (m *Master) getWorkerName() string {
	m.workerIndexLock.Lock()
	defer m.workerIndexLock.Unlock()
	if m.workerIndex == math.MaxInt32 {
		m.workerIndex = 0
	} else {
		m.workerIndex++
	}
	return "worker" + strSpliter + strconv.Itoa(m.workerIndex)
}

//
// start a thread that listens for RPCs from worker.go
//
func (m *Master) server() {
	rpc.Register(m)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := masterSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

//
// main/mrmaster.go calls Done() periodically to find out
// if the entire job has finished.
//
func (m *Master) Done() bool {
	ret := false
	m.mapTaskLock.Lock()
	defer m.mapTaskLock.Unlock()
	if m.mapTasks.Len() > 0 {
		return ret
	}
	if len(m.mapRunningTasks) > 0 {
		return ret
	}

	m.reduceTaskLock.Lock()
	defer m.reduceTaskLock.Unlock()
	if m.reduceTasks.Len() > 0 {
		return ret
	}
	if len(m.reduceRunningTasks) > 0 {
		return ret
	}

	ret = true
	return ret
}

//
// create a Master.
// main/mrmaster.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeMaster(files []string, nReduce int) *Master {
	// init list and runing record map
	m := Master{
		mapTasks:           list.New(),
		reduceTasks:        list.New(),
		mapRunningTasks:    make(map[string]string),
		reduceRunningTasks: make(map[string]string),
	}
	m.reduceNum = nReduce
	m.workerIndex = 0
	m.mapNum = len(files)
	for i, file := range files {
		m.mapTasks.PushBack(strconv.Itoa(i) + strSpliter + file)
	}
	for i := 0; i < nReduce; i++ {
		reduceFileName := "mr" + strSpliter + "out" + strSpliter + strconv.Itoa(i)
		m.reduceTasks.PushBack(reduceFileName)
	}
	m.server()
	return &m
}
