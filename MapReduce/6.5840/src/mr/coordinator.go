package mr

import (
	"encoding/gob"
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

const (
	IDLE = 0
	IN_PROGRESS = 1
	COMPLETED = 2
)

type Task struct{
	Status int // this is kept as string in guide
	WorkerId string
	StartedAt time.Time
}

type MapTask struct{
	FileName string
	NReduce int
	Task
}

type ReduceTask struct{
	Region int
	Locations []string
	Task
}


type Coordinator struct {
	MapTasks []*MapTask
	MapTasksRemaining int
	ReduceTasks []*ReduceTask
	ReduceTasksRemaining int
	Mu sync.Mutex

}

type MapReduceTask interface{
	AssignTask(workerId string)
}

func(t*Task) AssignTask(workerId string){
	t.Status = IN_PROGRESS
	t.WorkerId = workerId
	t.StartedAt = time.Now()
}

// Finds the first idle map task then first idle reduce task.
// If task found, then update the task with the worker assignment
// If no tasks, then reply is set to nil
func (c * Coordinator) AssignTask(workerId string, task *MapReduceTask) error {
	c.Mu.Lock()
	defer c.Mu.Unlock()
	gob.Register(MapTask{})
	gob.Register(ReduceTask{})

	if c.MapTasksRemaining !=0 {
		for _, mt:= range c.MapTasks{
			if mt.Status == IDLE {
				fmt.Printf("[Coordingator] Assigning Map Task: %v\n", mt.FileName)
				mt.AssignTask(workerId)
				*task = mt
				break
			}
		}
		return nil
	}
	if c.ReduceTasksRemaining!=0{
		for _, rt:= range c.ReduceTasks{
			if rt.Status ==IDLE{
				fmt.Printf("[Coordinator] Assigning Reduce Task: %v \n", rt.Region)
				rt.AssignTask(workerId)
				*task = rt
				break
			}
		}
		return nil
	}
	fmt.Println("[Coordinator] No idle tasks found")
	task = nil
	return nil
}

//
// start a thread that listens for RPCs from worker.go
//
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

//
// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
//
func (c *Coordinator) Done() bool {
	ret := false

	if c.MapTasksRemaining == 0 && c.ReduceTasksRemaining == 0{
		ret = true
	}

	return ret
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}
	c.MapTasks = make([]*MapTask,len(files))
	c.MapTasksRemaining = len(files)
	c.ReduceTasks = make([]*ReduceTask,nReduce)
	c.ReduceTasksRemaining = nReduce
	c.Mu = sync.Mutex{}

	for i, file := range files{
		c.MapTasks[i] = &MapTask{
			FileName: file,
			NReduce: nReduce,
			Task: Task{Status: IDLE},
		}
	}

	c.server()
	return &c
}
