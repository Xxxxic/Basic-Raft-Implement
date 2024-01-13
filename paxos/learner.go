package paxos

import (
	"fmt"
	"log"
	"net"
	"net/rpc"
)

type Learner struct {
	lis net.Listener
	// 学习者id
	id int
	// 记录接受者已接受的提案：[接受者id]请求信息
	acceptedMsg map[int]MsgArgs
}

// 接受来自 acceptor 的提案，将提案编号更大的提案存储到成员变量MsgArgs中
func (l *Learner) Learn(args *MsgArgs, reply *MsgReply) error {
	a := l.acceptedMsg[args.From]
	if a.Number < args.Number {
		//a.Number = args.Number
		l.acceptedMsg[args.From] = *args // ?
		reply.Ok = true
	} else {
		reply.Ok = false
	}

	return nil
}

// 根据accpetedMsg中存储的提案信息判断一个提案是否被超过半数的接受者接受，如果是就返回该批准的提案值
func (l *Learner) chosen() interface{} {
	acceptCounts := make(map[int]int)
	acceptMsg := make(map[int]MsgArgs)

	for _, accepted := range l.acceptedMsg {
		if accepted.Number != 0 {
			acceptCounts[accepted.Number]++
			acceptMsg[accepted.Number] = accepted
		}
	}

	for n, count := range acceptCounts {
		if count >= l.majority() {
			return acceptMsg[n].Value
		}
	}
	return nil
}

func (l *Learner) majority() int {
	return len(l.acceptedMsg)/2 + 1
}

// Learner也要监听某个端口 启动RPC
func newLearner(id int, acceptorIds []int) *Learner {
	learner := &Learner{
		id:          id,
		acceptedMsg: make(map[int]MsgArgs),
	}

	for _, aid := range acceptorIds {
		learner.acceptedMsg[aid] = MsgArgs{
			Number: 0,
			Value:  nil,
		}
	}
	learner.server(id)
	return learner
}

func (l *Learner) server(id int) {
	rpcs := rpc.NewServer()
	rpcs.Register(l)
	addr := fmt.Sprintf(":%d", id)
	lis, e := net.Listen("tcp", addr)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	l.lis = lis
	go func() {
		for {
			conn, err := l.lis.Accept()
			if err != nil {
				continue
			}
			go rpcs.ServeConn(conn)
		}
	}()
}

func (l *Learner) close() {
	l.lis.Close()
}
