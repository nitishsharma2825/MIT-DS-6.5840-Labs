package mr

import (
	"errors"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
)

type Coordinator struct {
	// Your definitions here.
	files   map[string]bool
	nReduce int
	mu      sync.Mutex
}

// Your code here -- RPC handlers for the worker to call.

// FetchTask assigns an available map task to the worker if any are available.
func (c *Coordinator) FetchTask(reply *TaskReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	for file, done := range c.files {
		if !done {
			reply.task = "map"
			reply.filename = file
			reply.nReduce = c.nReduce
			c.files[file] = true
			return nil
		}
	}
	return errors.New("no tasks available")
}

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

// start a thread that listens for RPCs from worker.go
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

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	ret := true

	// Your code here.
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, done := range c.files {
		if !done {
			ret = false
			break
		}
	}

	return ret
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{
		files:   make(map[string]bool),
		nReduce: nReduce,
	}

	// Your code here.
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, file := range files {
		c.files[file] = false
	}

	c.server()
	return &c
}
