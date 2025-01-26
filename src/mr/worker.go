package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io"
	"log"
	"net/rpc"
	"os"
)

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.
	for {
		task, filename, nReduce := CallFetchTask()

		if task == "map" {
			nTask := ihash(filename) % nReduce

			kv, err := doMap(mapf, filename)
			if err != nil {
				fmt.Printf("doMap failed!\n")
				return
			}
			err = saveMapResult(kv, nReduce, nTask)
			if err != nil {
				fmt.Printf("saveMapResult failed!\n")
				return
			}
		} else if task == "reduce" {
			return
		} else {
			log.Fatalf("No task available!\n")
		}
	}

	// uncomment to send the Example RPC to the coordinator.
	// CallExample()

}

// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	// the "Coordinator.Example" tells the
	// receiving server that we'd like to call
	// the Example() method of struct Coordinator.
	ok := call("Coordinator.Example", &args, &reply)
	if ok {
		// reply.Y should be 100.
		fmt.Printf("reply.Y %v\n", reply.Y)
	} else {
		fmt.Printf("call failed!\n")
	}
}

// Calls the Coordinator.FetchTask RPC and returns the task, filename, and number of reduce tasks.
func CallFetchTask() (string, string, int) {
	args := TaskReply{}
	reply := TaskReply{}

	ok := call("Coordinator.FetchTask", &args, &reply)
	if !ok {
		fmt.Printf("FetchTask failed!\n")
		return "", "", 0
	}

	return reply.Task, reply.Filename, reply.NReduce
}

// Reads the content of the given file, applies the map function, and returns the resulting key-value pairs.
func doMap(mapf func(string, string) []KeyValue, filename string) ([]KeyValue, error) {
	file, err := os.Open(filename)
	if err != nil {
		log.Fatalf("cannot open %v", filename)
		return nil, err
	}
	content, err := io.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", filename)
		return nil, err
	}
	file.Close()
	kva := mapf(filename, string(content))
	return kva, nil
}

func saveMapResult(kvp []KeyValue, nReduce int, nTask int) error {
	mapFiles := make(map[int]*os.File)

	for _, kv := range kvp {
		reduceTask := ihash(kv.Key) % nReduce
		file, exist := mapFiles[reduceTask]
		if !exist {
			filename := fmt.Sprintf("mr-%d-%d.json", nTask, reduceTask)
			var err error
			file, err = os.Create(filename) // Use `=` instead of `:=` to avoid shadowing
			if err != nil {
				log.Fatalf("Error while creating map file for task %d for bucket %d: %v", nTask, reduceTask, err)
				return err
			}
			mapFiles[reduceTask] = file
		}

		enc := json.NewEncoder(file)
		err := enc.Encode(&kv)
		if err != nil {
			log.Fatalf("Error while encoding key-value pair for task %d for bucket %d: %v", nTask, reduceTask, err)
			return err
		}
	}

	// Close all files
	for _, file := range mapFiles {
		err := file.Close()
		if err != nil {
			log.Printf("Error while closing file: %v", err)
		}
	}

	return nil
}

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
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

	fmt.Println(err)
	return false
}
