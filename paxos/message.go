package paxos

import (
	"net/rpc"
)

type MsgArgs struct {
	Number int
	Value  interface{}
	From   int
	To     int
}

type MsgReply struct {
	Ok     bool
	Number int
	Value  interface{}
}

/*
call 用于发送RPC消息，接受对方的响应
srv：表示服务器的地址。
name：表示要调用的远程函数的名称。
args：表示传递给远程函数的参数。
reply：表示远程函数调用的返回值。
*/
func call(srv string, name string, args interface{}, reply interface{}) bool {
	// 建立到服务器的 TCP 连接。如果连接失败，函数返回 false。如果连接成功，它在函数退出时关闭连接（defer c.Close()）
	c, err := rpc.Dial("tcp", srv)
	if err != nil {
		return false
	}
	defer c.Close()

	// c.Call 函数实际调用远程函数，将参数 args 传递给远程函数，并将返回值存储在 reply 中。
	err = c.Call(name, args, reply)
	if err == nil {
		return true
	}
	return false
}
