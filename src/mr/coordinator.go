package mr

import "log"
import "net"
import "os"
import "net/rpc"
import "net/http"
import "fmt"
import "sync"
import "time"
import "strconv"
//map任务
type mapjob struct{
	Key string
	mapid int
}

type reducejob struct{
	Key int
}

//协调者
type Coordinator struct {
	// Your definitions here.
	mapchan chan mapjob
	reducechan chan reducejob

	Remain_maps []int
	Remain_reduces[]int

	current_maps []int
	current_reduces []int
	start_time[]int64
	reduce_start_time[]int64

	mapfiles []string
	
	TASK string
	mux sync.Mutex
	nReduce int
}

// Your code here -- RPC handlers for the worker to call.
func (c*Coordinator) Distribute(args *MapArgs, reply *MapReply) error{
	defer c.mux.Unlock()
	c.mux.Lock()

	if len(c.mapchan)>0 {		
		temp:=<-c.mapchan
		fmt.Println("分配map任务",temp.Key," ",temp.mapid)
		reply.Filename=temp.Key
		reply.Mapid=temp.mapid

		c.current_maps[temp.mapid]=1
		c.start_time[temp.mapid]=time.Now().Unix()

		return nil
	}else{
		reply.Filename=""
		reply.Mapid=-1
		return nil
	}
}

func (c*Coordinator) Endhandle(args *End_Args, reply *ExampleReply) error{
	defer c.mux.Unlock()
	c.mux.Lock()

if c.TASK=="map" {
	endid:=args.Mapid
	c.Remain_maps[endid]=1

	c.current_maps[endid]=0
	c.start_time[endid]=0

	fmt.Println("完成map任务",endid)
	k:=true
	for i:=0;i<len(c.Remain_maps);i++{
		if c.Remain_maps[i]==0{
			k=false
		}
	}
	if k==true {
		c.TASK="reduce"
		c.reducechan=make(chan reducejob,c.nReduce)
			//处理状态转化到reduce态：
		for i:=0;i<c.nReduce;i++{
			rj:=reducejob{i}
			c.reducechan<-rj;
			c.Remain_reduces=append(c.Remain_reduces,0)
			fmt.Println("创建reduce任务",i)
		}
		
	}
	return nil
}


if c.TASK=="reduce" {
	endid:=args.Reduceid
	c.Remain_reduces[endid]=1
	fmt.Println("完成reduce任务",endid)

	c.current_reduces[endid]=0
	c.reduce_start_time[endid]=0

	k:=true
	for i:=0;i<len(c.Remain_reduces);i++{
		if c.Remain_reduces[i]==0{
			k=false
		}
	}
	if k==true {
		c.TASK="over"
		fmt.Println("完成mapreduce任务")
	}
	return nil
}
return nil
}


func (c *Coordinator) Statushandle(args *ExampleArgs, reply *StatusReply) error {
	defer c.mux.Unlock()
	c.mux.Lock()
	reply.Status=c.TASK
	reply.Nreduce=c.nReduce
	return nil
}


func (c*Coordinator) Reduce_Distribute(args *ExampleArgs, reply *ReduceReply) error{
	defer c.mux.Unlock()
	c.mux.Lock()
	if len(c.reducechan)>0 {		
		temp:=<-c.reducechan
		fmt.Println("分配reduce任务成功",temp.Key)
		reply.Key=temp.Key

		c.current_reduces[temp.Key]=1
		c.reduce_start_time[temp.Key]=time.Now().Unix()
		
		return nil
	}else{
	reply.Key=-1;
	return nil
	}
}
//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
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
	defer c.mux.Unlock()
	c.mux.Lock()
	
	ret := false

	// Your code here.
	if c.TASK=="over" {
		ret=true;
	}

	return ret
}



func (c *Coordinator) Check() {
for c.Done()!=true{ 
	c.mux.Lock()
	if c.TASK=="map"{
		fmt.Println("start check map")
		for i:=0;i<len(c.current_maps);i++ {
			//未完成
			if c.current_maps[i]==1 {
				cost:=time.Now().Unix()-c.start_time[i];
				//超时
				if cost>=10 {
					//重新维护
					c.current_maps[i]=0
					c.start_time[i]=0
					//丢回map池
					mj:=mapjob{c.mapfiles[i],i}
					c.mapchan<-mj
					fmt.Println(c.mapfiles[i],mj.mapid,"重新放回map池")
					//清空中间文件mr-i-k
					ii:=strconv.Itoa(i)
					for k:=0;k<10;k++{
						str:=strconv.Itoa(k)
						err := os.Remove("../intermediate/reduce"+str+"/mr-"+ii+"-"+str)
						if err==nil {
							log.Println("清除中间文件success")
						}					
					}
					//收回权限(暂时不需要，因为直接crash了)；
				}
			}
		}

	}else if c.TASK=="reduce"{
		fmt.Println("start check reduce")
		for i:=0;i<c.nReduce;i++{
			if c.current_reduces[i]==1{
				cost:=time.Now().Unix()-c.reduce_start_time[i];
				if cost>=10 {
					//重新维护
					c.current_reduces[i]=0
					c.reduce_start_time[i]=0
					//丢回map池
					rj:=reducejob{i}
					c.reducechan<-rj
					fmt.Println(rj.Key,"重新放回reduce池")
			}
		}
	}
	
}
c.mux.Unlock()
time.Sleep(time.Second*10)
}
fmt.Println("check exit")

}


//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}
	// Your code here.
//	defer close(c.mapchan)
	c.TASK="map"
	c.mapchan=make(chan mapjob,nReduce)
	c.mapfiles=files
	c.nReduce=nReduce
	for i,filename:=range files{
		mj:=mapjob{filename,i}
		fmt.Println(filename,mj.mapid)
		c.mapchan<-mj
		c.Remain_maps=append(c.Remain_maps,0)
		c.current_maps=append(c.current_maps,0)
		c.start_time=append(c.start_time,0)
	}
	for i:=0;i<nReduce;i++ {
		c.current_reduces=append(c.current_reduces,0)
		c.reduce_start_time=append(c.reduce_start_time,0)
	}
	go c.Check()
	c.server()
	return &c
}
