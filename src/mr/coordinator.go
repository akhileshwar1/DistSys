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

type Stack []task

func (s *Stack) push(value task) {
  *s = append(*s, value)
}

func (s *Stack) pop() (task, bool) {
  if len(*s) == 0 {
    return task{}, false // stack is empty.
  }

  index := len(*s) - 1
  value := (*s)[index]
  *s = (*s)[:index] // resize the stack, effectively removes the last element.
  return value, true
}

// returns the removed task.
func (s *Stack) remove(filename string) (task, bool) {
  for i, task := range *s {
    if task.filename == filename {
      *s = append((*s)[:i], (*s)[i+1:]...)
      return task, true
    }
  }

  return task{}, false
}

func (s *Stack) peek() (task, bool) {
  if len(*s) == 0 {
    return task{}, false // stack is empty.
  }

  index := len(*s) - 1
  value := (*s)[index]
  return value, true
}

type task struct {
  filename string
  operation string // "map" or "reduce"
}

type Coordinator struct {
  mu sync.Mutex
  startedTasks Stack 
  notStartedTasks Stack 
  doneTasks Stack 
  nReduce int  // no of nReduce files to distribute the keys to.
}

func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
  reply.Y = args.X + 1
  return nil
}

func (c *Coordinator) GetTask(args *struct{}, reply *TaskReply) error {
  c.mu.Lock()
  defer c.mu.Unlock()
  task, bool := c.notStartedTasks.pop()
  if !bool {
    return errors.New("No tasks available")
  }

  filename := task.filename
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
  reply.Operation = task.operation

  c.startedTasks.push(task) // push to started status.
  return nil
}

func (c *Coordinator) DoneTask(filename string, reply *TaskReply) error {
  task, bool := c.startedTasks.remove(filename)
  if !bool {
    return errors.New("No such task")
  }

  // push it to the done stack.
  c.doneTasks.push(task)
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
  _, nbool := c.notStartedTasks.peek()
  _, sbool := c.startedTasks.peek()
  if (!nbool && !sbool) {
    return true
  }

  return false
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
  c := Coordinator{}

  // add reduce tasks
  i := 0
  for i < nReduce {
    t := task{fmt.Sprintf("mr-%d", i), "reduce"}
    c.notStartedTasks.push(t)
    i++
  }

  // add map tasks
  for _, name := range files {
    t := task{name, "map"}
    c.notStartedTasks.push(t)
  }

  c.nReduce = nReduce
  c.server()
  return &c
}
