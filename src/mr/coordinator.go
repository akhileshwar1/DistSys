package mr

import "fmt"
import "log"
import "net"
import "os"
import "net/rpc"
import "net/http"
import "errors"
import "io/ioutil"
import "sync"

type task struct {
  filename string
  status int  // 0: not started, 1: started, 2: done.
}

type Coordinator struct {
  mu sync.Mutex
  tasks []task
  current int  // stores the current index of the tasks list.
  nReduce int  // no of nReduce files to distribute the keys to.
}

// Your code here -- RPC handlers for the worker to call.

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

func (c *Coordinator) GetTask(args *struct{}, reply *TaskReply) error {
    c.mu.Lock()
    defer c.mu.Unlock()
    if c.current >= len(c.tasks) {
        return errors.New("No tasks available")
    }

    filename := c.tasks[c.current].filename
    file, err := os.Open(filename)
    if err != nil {
        return errors.New("cannot open " + filename)
    }
    defer file.Close()  // Ensure the file is closed even if there's an error

    content, err := ioutil.ReadAll(file)
    if err != nil {
        return errors.New("cannot read " + filename)
    }

    reply.Filename = filename
    reply.Content = string(content)
    reply.Nreduce = c.nReduce
    c.current++  // Move to the next task
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

	// Your code here.


	return ret
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}

  for _, name := range files {
    t := task{name, 0}
    c.tasks = append(c.tasks, t)
  }
  c.current = 0
  c.nReduce = nReduce

  fmt.Println(c)

	c.server()
	return &c
}
