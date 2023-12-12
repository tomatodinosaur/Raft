package mr

import "fmt"
import "log"
import "net/rpc"
import "hash/fnv"
import "os"
import "io/ioutil"
import "strconv" 
import "strings"
import "sort"
import "time"

var Nreduce int=10
//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

type working_man struct{
	status string
	mapfile string
	mapid int
	reduceid int
}

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }


//一共NRudece台reduce机器？
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

	// Your worker implementation here.
	w := working_man{}
	pid := os.Getpid()
	w.status,Nreduce=callstatus(pid)
	//获取访问分布式数据库的权限：
	
for w.status!="over" {
w.status,_=callstatus(pid)
//map__________________________________________________________________________________________________________
//___________________________________________________________________________________________________________________	
if w.status=="map" {
	w.mapfile,w.mapid=callformap(pid)
//	w.mapfile="../main/"+w.mapfile
	if w.mapid==-1{
		fmt.Printf("%d pid get mapjob fail,map is empty\n",pid)
		time.Sleep(time.Second)
		continue	
	}
	str, _ := os.Getwd()
	fmt.Println(str)
	file, err := os.Open(w.mapfile)
	if err != nil {
			log.Fatalf("cannot open %v", w.mapfile)
		}
	//将文件一次性读出，返回err,[]byte；需要转化成string
	content, err := ioutil.ReadAll(file)
	if err != nil {
			log.Fatalf("cannot read %v", w.mapfile)
		}
	file.Close()
	//调用map函数，生成kva【word，1】
	kva:= mapf(w.mapfile, string(content))
	intermediate:=make(map[int] []KeyValue)
	for _,kv :=range kva{
		idx:=ihash(kv.Key)%Nreduce
		intermediate[idx]=append(intermediate[idx],kv)
	}
	//创建输出文件
	for k:=range intermediate{
		x:=strconv.Itoa(w.mapid)
		y:=strconv.Itoa(k)
	  oname := "mr-"+x+"-"+y
    oname="../intermediate/reduce"+y+"/"+oname
// str, _ := os.Getwd()
// fmt.Println(str)
		ofile, _ := os.Create(oname)
		for _,val:=range intermediate[k]{
			fmt.Fprintf(ofile, "%v %v\n", val.Key,val.Value)
		}
		ofile.Close()
	}
	Callend(w.mapid,w.status)

//等待被唤醒:
var temp_str string
for temp_str=="map"{
	temp_str,_=callstatus(pid)
}
}  else if w.status=="reduce"{
	//reduce______________________________________________________________________________________
	//________________________________________________________________________________________________
w.reduceid=callforreduce(pid)
reduceid:=w.reduceid
x:=strconv.Itoa(reduceid)

if reduceid==-1{
	fmt.Printf("%d pid get reducejob fail,map is empty\n",pid)
	time.Sleep(time.Second)
	continue	
}


files, err := ioutil.ReadDir("../intermediate/"+"reduce"+x)
if err != nil {
	log.Fatal(err)
}
reduce_intermediate := []KeyValue{}//存储当前reducekey的所有文件的键值对

//循环处理reduce_key下的所有文件
for _,curfile := range files {
  curfilename:="../intermediate/reduce"+x+"/"+curfile.Name()

	file, err := os.Open(curfilename)
	if err != nil {
			fmt.Printf("cannot open %v\n", curfilename)
		}
	//将文件一次性读出，返回err,[]byte；需要转化成string
	content, err := ioutil.ReadAll(file)
	if err != nil {
		fmt.Printf("cannot read %v", curfilename)
	}
	words := strings.Fields(string(content))
	temp:=KeyValue{}
	for i,word:= range words{
		if i%2==0{
			temp.Key=word
		}
		if i%2==1 {
			temp.Value=word
			reduce_intermediate=append(reduce_intermediate,temp)
		}
	}
}
//根据K值排序
sort.Sort(ByKey(reduce_intermediate))

//oname := "../mr-out-"+x
oname := "mr-out-"+x
ofile, _ := os.Create(oname)

//
// call Reduce on each distinct key in intermediate[],
// and print the result to mr-out-0.
//
i := 0
for i < len(reduce_intermediate) {
	j := i + 1
	for j < len(reduce_intermediate) && reduce_intermediate[j].Key == reduce_intermediate[i].Key {
		j++
	}
	values := []string{}
	for k := i; k < j; k++ {
		values = append(values, reduce_intermediate[k].Value)
	}
	output := reducef(reduce_intermediate[i].Key, values)

	// this is the correct format for each line of Reduce output.
	fmt.Fprintf(ofile, "%v %v\n", reduce_intermediate[i].Key, output)

	i = j
}

ofile.Close()
// fmt.Println("文件已生成")
Callend(w.reduceid,w.status)
//fmt.Println("结果已告知")

	// uncomment to send the Example RPC to the coordinator.
	// CallExample()
	}
time.Sleep(time.Second)

}

fmt.Printf("%d worker exit\n",pid)
}
//
func callformap(p int) (s string,x int){
		args:=MapArgs{}
		reply:=MapReply{}
		ok := call("Coordinator.Distribute", &args, &reply)
		if ok {
			s=reply.Filename
			x=reply.Mapid
			if x!=-1{
			fmt.Printf("%d pid get mapjob_id: %v    filename %v\n", p,x,s)
			}
		} else {
			fmt.Printf("call failed!\n")
		}
		//return filename
		return s,x
}

func Callend(id int,status string) {
if status=="map" {
	args := End_Args{}
	args.Mapid = id
	reply := ExampleReply{}
	ok := call("Coordinator.Endhandle", &args, &reply)
	if ok {
		// reply.Y should be 100.
		fmt.Printf("%v mapjob finished\n",id)
	} else {
		fmt.Printf("call failed!\n")
	}
	return
}

if status=="reduce" {
	args := End_Args{}
	args.Reduceid = id
	reply := ExampleReply{}
	ok := call("Coordinator.Endhandle", &args, &reply)
	if ok {
		// reply.Y should be 100.
		fmt.Printf("%v reducejob finished\n",id)
	} else {
		fmt.Printf("call failed!\n")
	}

	return
}

}

func callstatus(p int)(s string,r int){
	args:=ExampleArgs{}
	reply:=StatusReply{}
	ok := call("Coordinator.Statushandle", &args, &reply)
	if ok {
		s=reply.Status
		r=reply.Nreduce
		fmt.Printf("%d pid  current status: %v\n",p,s)
	} else {
		fmt.Printf("call failed!\n")
	}
	//return filename
	return s,r
}

func callforreduce(p int) (x int){
	args:=ExampleArgs{}
	reply:=ReduceReply{}
	ok := call("Coordinator.Reduce_Distribute", &args, &reply)
	if ok {
		x=reply.Key
		if x!=-1{
		fmt.Printf("%d pid get reducejob_id: %v \n",p,x)
		}
	} else {
		fmt.Printf("call failed!\n")
	}
	//return filename
	return x
}

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
