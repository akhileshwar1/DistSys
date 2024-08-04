package mr

import "os"
import "encoding/json"
import "fmt"
import "time"
import "log"
import "net/rpc"
import "hash/fnv"
import "strings"
import "strconv"

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

// Map functions return a slice of KeyValue.
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
  x := 5
  ticker := time.NewTicker(time.Duration(x) * time.Second)
  defer ticker.Stop()
  for range ticker.C {
    filename, content, nReduce, operation := CallGet()
    if (operation == "map") {
      MapTask(mapf, filename, content, nReduce)
    } else {
      ReduceTask(reducef, filename, content, nReduce)
    }
  }
}

func MapTask(mapf func(string, string) []KeyValue, filename string, content string, 
             nReduce int) {
  if content == ""{
    fmt.Println("content is nil")
    return
  }
  fmt.Println("task received")
  kva := mapf(filename, content)
  for _, kv := range kva {
    i := ihash(kv.Key) % nReduce
    filename := fmt.Sprintf("mr-%d", i)
    file, err := os.OpenFile(filename, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
    defer file.Close()
    if (os.IsNotExist(err)) {
      file, err = os.Create(filename)
    }

    // we have the file now, just write the key value pair as json to the file.
    // at the end we will have all the words distributed among nReduce files.
    enc := json.NewEncoder(file)
    enc.Encode(&kv)
  }
}

func ReduceTask(reducef func(string, []string) string, filename string, content string, 
             nReduce int) {
  if content == ""{
    fmt.Println("content is nil")
    return
  }

  // extract the no from filename
  parts := strings.Split(filename, "-")
  var num int
  if len(parts) > 1 {
    n, err := strconv.Atoi(parts[1])
    num = n
    if err != nil {
      fmt.Println("Error converting to int:", err)
    }
  }

  // read into kv slice from the input file.
  oname := fmt.Sprintf("mr-out-%d", num)
  fmt.Println(oname)
	ofile, _ := os.Create(oname)
  ifile, err := os.Open(filename)
  if (err != nil) {
    fmt.Println("cannot read input file")
    return
  }
	kva := []KeyValue{}
  dec := json.NewDecoder(ifile)
  for {
    var kv KeyValue
    if err := dec.Decode(&kv); err != nil {
      break
    }
    kva = append(kva, kv)
  }

	// call Reduce on each distinct key in kva[],
	// and print the result to ofile.
	i := 0
	for i < len(kva) {
		j := i + 1
		for j < len(kva) && kva[j].Key == kva[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, kva[k].Value)
		}
		output := reducef(kva[i].Key, values)

		// this is the correct format for each line of Reduce output.
		fmt.Fprintf(ofile, "%v %v\n", kva[i].Key, output)

		i = j
	}

	ofile.Close()
}


func CallGet() (string, string, int, string){
	reply := TaskReply{} 
  ok := call("Coordinator.GetTask", new(struct {}), &reply)
  if ok {
    // fmt.Printf("contents are %s", reply)
    return reply.Filename, reply.Content, reply.Nreduce, reply.Operation
  } else {
    fmt.Println("error is ", ok)
    return "", "", 0, ""
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
