package main

//
// a MapReduce pseudo-application to test that workers
// execute reduce tasks in parallel.
//
// go build -buildmode=plugin rtiming.go
//

import "6.824/mr"
import "fmt"
import "os"
import "syscall"
import "time"
import "io/ioutil"

func nparallel(phase string) int {
	// create a file so that other workers will see that
	// we're running at the same time as them.
	pid := os.Getpid()
	myfilename := fmt.Sprintf("mr-worker-%s-%d", phase, pid)
	err := ioutil.WriteFile(myfilename, []byte("x"), 0666)
	if err != nil {
		panic(err)
	}

	// are any other workers running?
	// find their PIDs by scanning directory for mr-worker-XXX files.
	dd, err := os.Open(".")
	if err != nil {
		panic(err)
	}
	names, err := dd.Readdirnames(1000000)
	if err != nil {
		panic(err)
	}
	ret := 0
	for _, name := range names {
		var xpid int
		pat := fmt.Sprintf("mr-worker-%s-%%d", phase)
		n, err := fmt.Sscanf(name, pat, &xpid)
		if n == 1 && err == nil {
			err := syscall.Kill(xpid, 0)
			if err == nil {
				// if err == nil, xpid is alive.
				ret += 1
			}
		}
	}
	dd.Close()

	time.Sleep(1 * time.Second)

	err = os.Remove(myfilename)
	if err != nil {
		panic(err)
	}

	return ret
}

func Map(filename string, contents string) []mr.KeyValue {

	kva := []mr.KeyValue{}
	kva = append(kva, mr.KeyValue{"a", "1"})
	kva = append(kva, mr.KeyValue{"b", "1"})
	kva = append(kva, mr.KeyValue{"c", "1"})
	kva = append(kva, mr.KeyValue{"d", "1"})
	kva = append(kva, mr.KeyValue{"e", "1"})
	kva = append(kva, mr.KeyValue{"f", "1"})
	kva = append(kva, mr.KeyValue{"g", "1"})
	kva = append(kva, mr.KeyValue{"h", "1"})
	kva = append(kva, mr.KeyValue{"i", "1"})
	kva = append(kva, mr.KeyValue{"j", "1"})
	return kva
}

func Reduce(key string, values []string) string {
	n := nparallel("reduce")

	val := fmt.Sprintf("%d", n)

	return val
}
/*
这段代码似乎是一个简化版本的的大规模并行处理（MapReduce）任务的实现。我会逐步为您解释每个部分。

nparallel 函数：

这个函数的目标是并行地运行其他进程，同时限制并行进程的数量。
它首先创建一个名为 mr-worker-phase-pid 的文件，其中 "phase" 是传入的参数，而 "pid" 是当前进程的进程ID。这样其他worker进程可以在运行时检查这个文件是否存在，以了解是否有其他worker正在运行。
然后，它扫描当前目录，寻找其他名为 "mr-worker-phase-XXX" 的文件。通过这种方式，它可以找到所有正在运行的其他worker进程的PID。
对于每个找到的文件，它尝试向对应的进程发送一个信号（通过 syscall.Kill(xpid, 0)）。如果该进程存在并响应信号（即它还在运行），函数会增加 "ret" 变量的值。
最后，函数等待1秒钟（可能为了模拟一些实际工作负载），然后删除创建的文件。
函数返回的是并行运行的worker数量。
Map 函数：

这个函数接受一个文件名和一个内容字符串作为输入，但实际上它并没有使用这些输入。
函数生成了一个固定的键值对列表（key-value pairs）并返回它。这个列表包含12个键值对，每个键都是一个小写字母，每个值都是 "1"。
Reduce 函数：

这个函数接受一个键和一系列值作为输入，但实际上它并没有对这些值进行任何操作。
它首先调用 nparallel("reduce") 函数。这个调用的目的是限制同时运行的 "reduce" 阶段的worker进程数量。
然后，它使用返回的数字 "n" 来格式化一个字符串，该字符串的值为 n。
最后，这个字符串被返回作为结果。
总的来说，这段代码是一个简化版本的MapReduce的实现，但并没有真正执行任何分布式处理或数据归约的工作。可能这段代码是为了演示或测试某些概念而写的。
*/