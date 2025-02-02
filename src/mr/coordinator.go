package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"strconv"
	"sync"
	"time"
)

type Task struct {
	taskId   string
	status   string
	lastTime time.Time
	mu       sync.Mutex
}

type Coordinator struct {
	// Your definitions here.
	mfiles     map[string]*Task
	rfiles     map[string]*Task
	nReduce    int
	mu         sync.Mutex
	mapTask    int
	reduceTask int
}

// Your code here -- RPC handlers for the worker to call.

// FetchTask assigns an available map task to the worker if any are available.
func (c *Coordinator) FetchTask(args *ExampleArgs, reply *TaskReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.mapTask > 0 {

		for file, task := range c.mfiles {
			task.mu.Lock()

			if task.status == "left" {
				task.status = "running map"
				task.lastTime = time.Now()
				reply.Task = "map"
				reply.Filename = file
				reply.NReduce = c.nReduce

				go waitForTask(task)

				task.mu.Unlock()
				return nil
			}

			task.mu.Unlock()
		}

		reply.Task = "wait"
		return nil
	}

	if c.reduceTask > 0 {

		for i := 0; i < c.nReduce; i++ {
			idx := strconv.Itoa(i)
			task := c.rfiles[idx]
			task.mu.Lock()

			if task.status == "left" {
				task.status = "running reduce"
				task.lastTime = time.Now()
				reply.Task = "reduce"
				reply.Filename = idx
				reply.NReduce = c.nReduce

				go waitForTask((c.rfiles[idx]))
				task.mu.Unlock()
				return nil
			}

			task.mu.Unlock()
		}

		reply.Task = "wait"
		return nil
	}

	reply.Task = "exit"
	return nil
}

func (c *Coordinator) TaskDone(args *WorkerTaskArgs, reply *WorkerTaskReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if args.Task == "map" {
		task := c.mfiles[args.Filename]
		task.mu.Lock()
		task.status = "completed"
		task.lastTime = time.Now()
		task.mu.Unlock()
		c.mapTask -= 1
	} else {
		task := c.rfiles[args.Filename]
		task.mu.Lock()
		task.status = "completed"
		task.lastTime = time.Now()
		task.mu.Unlock()
	}

	reply.Exit = (c.mapTask == 0 && c.reduceTask == 0)
	return nil
}

func waitForTask(task *Task) {
	time.Sleep(10 * time.Second)
	task.mu.Lock()
	defer task.mu.Unlock()
	if task.status != "completed" {
		task.status = "left"
		task.lastTime = time.Now()
	}
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
	// Your code here.
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.mapTask == 0 && c.reduceTask == 0
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{
		mfiles:     make(map[string]*Task),
		rfiles:     make(map[string]*Task),
		nReduce:    nReduce,
		mapTask:    len(files),
		reduceTask: nReduce,
	}

	// Your code here.
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, file := range files {
		c.mfiles[file] = &Task{taskId: file, status: "left", lastTime: time.Now(), mu: sync.Mutex{}}
	}

	for i := 0; i < nReduce; i++ {
		idx := strconv.Itoa(i)
		c.rfiles[idx] = &Task{taskId: idx, status: "left", lastTime: time.Now(), mu: sync.Mutex{}}
	}

	c.server()
	return &c
}
