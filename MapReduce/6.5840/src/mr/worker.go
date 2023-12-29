package mr

import (
	"encoding/gob"
	"fmt"
	"hash/fnv"
	"io"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"time"

	"github.com/google/uuid"
)

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

type Worker struct{
	WorkerId string
	RegionToPairs map[int][] KeyValue
}

func (w *Worker) Sockname() string {
	s:="/var/tmp/5840-mr-"
	s+=w.WorkerId
	return s
}

func (w *Worker) server(){
	rpc.Register(w)
	rpc.HandleHTTP()
	sockname := w.Sockname()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	} 
	go http.Serve(l,nil)
}

func MakeWorker( mapf func(string,string)[]KeyValue, reducef func(string,[]string) string){
	workerId := uuid.New().String()[0:6]
	w:= Worker{workerId, make(map[int][]KeyValue)}
	w.server()

	gob.Register(MapTask{})
	gob.Register(ReduceTask{})
	taskReply:= new(MapReduceTaskReply)
	ok:= call(coordinatorSock(),"Coordinator.AssignTask",w.WorkerId,taskReply)
	task:=(*taskReply)
	for ok && taskReply != nil{
		switch task:=(*taskReply).(type) {
		case MapTask:
			fmt.Printf("Processing Map task: %v\n", task)
			ok = w.processMap(mapf,task.FileName,task.NReduce)

		case ReduceTask:
			fmt.Printf("Processing Reduce task: %v\n", task)
			ok = w.processReduce(reducef,task.Region,task.Locations)
		default:
			fmt.Printf("Unknown task type %T, terminating program.\n", task)
			ok = false		
		}
		tmpReply := 0
		if ok{
			ok = call(coordinatorSock(),"Coodinator.CompleteTask",task, &tmpReply)
		}
		time.Sleep(2*time.Second)
		ok = call(coordinatorSock(),"Coordinator.AssignTask",w.WorkerId,taskReply)
	}
}

func (w * Worker) processMap(mapf func(string,string)[]KeyValue,fileName string, nReduce int) bool{
	f,err := os.Open(fileName)
	defer f.Close()

	if err!=nil{
		log.Fatalf("Cannot open %v \n",fileName)
	}
	content,err := io.ReadAll(f)
	if err!=nil {
		log.Fatalf("Cannot read %v \n",fileName)
	}
	kva:=mapf(fileName,string(content))
	for _ , kv := range kva{
		region:= ihash(kv.Key)%nReduce + 1
		w.RegionToPairs[region] = append(w.RegionToPairs[region], kv)
	}
	return true
}

type MapReduceTaskReply interface{
}

// //
// // example function to show how to make an RPC call to the coordinator.
// //
// // the RPC argument and reply types are defined in rpc.go.
// //
// func CallExample() {

// 	// declare an argument structure.
// 	args := ExampleArgs{}

// 	// fill in the argument(s).
// 	args.X = 99

// 	// declare a reply structure.
// 	reply := ExampleReply{}

// 	// send the RPC request, wait for the reply.
// 	// the "Coordinator.Example" tells the
// 	// receiving server that we'd like to call
// 	// the Example() method of struct Coordinator.
// 	ok := call("Coordinator.Example", &args, &reply)
// 	if ok {
// 		// reply.Y should be 100.
// 		fmt.Printf("reply.Y %v\n", reply.Y)
// 	} else {
// 		fmt.Printf("call failed!\n")
// 	}
// }

//
// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(sockname string, rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	// sockname := coordinatorSock()
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
