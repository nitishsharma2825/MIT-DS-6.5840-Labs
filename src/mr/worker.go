package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io"
	"log"
	"net/rpc"
	"os"
	"sort"
	"strconv"
	"strings"
	"time"
)

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

// for sorting by key.
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

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.
	exit := false
	for !exit {
		task, filename, nReduce, ok := CallFetchTask()

		if !ok {
			fmt.Println("Coordinator crashed, worker exiting")
			return
		}

		if task == "map" {
			nTask := ihash(filename)

			kv, err := doMap(mapf, filename)
			if err != nil {
				fmt.Printf("doMap failed!\n")
				return
			}

			err = saveMapResult(kv, nReduce, nTask)
			if err != nil {
				fmt.Printf("saveMapResultTemp failed!\n")
				return
			}

			ok, exit = CallDone("map", filename)
		} else if task == "reduce" {
			index, err := strconv.Atoi(filename)
			if err != nil {
				log.Fatalf("Error while converting filename to integer: %v", err)
				return
			}

			files := getFiles(index)
			kvData := []KeyValue{}

			if len(files) > 0 {
				for _, filepath := range files {
					file, err := os.Open(filepath)
					if err != nil {
						log.Fatalf("Error while opening file: %v", err)
						return
					}

					dec := json.NewDecoder(file)
					for {
						var kv KeyValue
						if err := dec.Decode(&kv); err != nil {
							break
						}
						kvData = append(kvData, kv)
					}
				}

				sort.Sort(ByKey(kvData))
				doReduce(reducef, kvData, filename[:1])
			}

			ok, exit = CallDone("reduce", filename)
		} else if task == "exit" {
			exit = true
		}

		if !ok {
			fmt.Println("Coordinator crashed, worker exiting")
			return
		}

		time.Sleep(time.Millisecond * 100)
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
func CallFetchTask() (string, string, int, bool) {
	args := ExampleArgs{}
	reply := TaskReply{}

	ok := call("Coordinator.FetchTask", &args, &reply)

	return reply.Task, reply.Filename, reply.NReduce, ok
}

func CallDone(taskType string, file string) (bool, bool) {
	args := WorkerTaskArgs{
		Task:     taskType,
		Filename: file,
	}
	reply := WorkerTaskReply{
		Exit: false,
	}

	ok := call("Coordinator.TaskDone", &args, &reply)

	return ok, reply.Exit
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
	var err error = nil

	for _, kv := range kvp {
		// find the reduce task for this key
		reduceTask := ihash(kv.Key) % nReduce
		file, exist := mapFiles[reduceTask]

		if !exist {
			filename := fmt.Sprintf("mr-%d-%d.json", nTask, reduceTask)
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

func getFiles(index int) []string {
	suffix := strconv.Itoa(index) + ".json"
	dir, err := os.Getwd()
	if err != nil {
		log.Fatalf("Error while getting current working directory: %v", err)
		return nil
	}

	files, err2 := os.ReadDir(dir)
	if err2 != nil {
		log.Fatalf("Error while reading directory: %v", err2)
		return nil
	}

	fileset := []string{}
	for _, file := range files {
		if !file.IsDir() && strings.HasSuffix(file.Name(), suffix) {
			fileset = append(fileset, file.Name())
		}
	}

	return fileset
}

func doReduce(reducef func(string, []string) string, kvps []KeyValue, index string) {
	oname := "mr-out-" + index + ".txt"
	ofile, err := os.Create(oname)
	if err != nil {
		log.Fatalf("Error while creating output file: %v", err)
		return
	}

	//
	// call Reduce on each distinct key in kvps[],
	// and print the result to mr-out-<index>.
	//
	i := 0
	for i < len(kvps) {
		j := i + 1
		for j < len(kvps) && kvps[j].Key == kvps[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, kvps[k].Value)
		}
		output := reducef(kvps[i].Key, values)

		// this is the correct format for each line of Reduce output.
		fmt.Fprintf(ofile, "%v %v\n", kvps[i].Key, output)

		i = j
	}

	ofile.Close()
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
