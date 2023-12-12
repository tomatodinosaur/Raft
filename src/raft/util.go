package raft

import "log"
//import "os"

// Debugging
const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	// file, err := os.OpenFile("raft.log", os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0666)  
	// if err != nil {  
	// log.Fatal("无法打开日志文件：", err)  
	// }  
	// defer file.Close()  
	 
	// // 设置日志输出到文件  
	// log.SetOutput(file)  
	if Debug {
		log.Printf(format, a...)
	}
	return
}
//leader 退出领导前发送的心跳，因为TERM的更新 而被 其他节点接收