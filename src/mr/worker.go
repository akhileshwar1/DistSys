package mr

import "os"
import "encoding/json"
import "fmt"
import "log"
import "net/rpc"
import "hash/fnv"


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


//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {
  filename, content, taskno, nReduce := CallGet()
  if content == ""{
    fmt.Println("content is nil")
  }
  kva := mapf(filename, content)
  for _, kv := range kva {
    i := ihash(kv.Key) % nReduce
    filename := fmt.Sprintf("mr-%d-%d", taskno, i)
    file, err := os.OpenFile(filename, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
    if (os.IsNotExist(err)) {
      file, err = os.Create(filename)
    }

    // we have the file now, just write the key value pair as json to the file.
    // at the end we will have all the words distributed among nReduce files.
    enc := json.NewEncoder(file)
    enc.Encode(&kv)
  }
}

func CallGet() (string, string, int, int){
	reply := TaskReply{} 
  ok := call("Coordinator.GetTask", new(struct {}), &reply)
  if ok {
    // fmt.Printf("contents are %s", reply)
    return reply.Filename, reply.Content, reply.TaskNo, reply.Nreduce
  } else {
    fmt.Println("error is ", ok)
    return "", "", 0, 0
  }
}

//
// example function to show how to make an RPC call to the coordinator.
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

//
// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
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
