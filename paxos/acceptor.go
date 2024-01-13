package paxos

import (
	"fmt"
	"log"
	"net"
	"net/rpc"
)

type Acceptor struct {
	// 用来启动RPC服务并监听端口
	lis net.Listener
	// 服务器id
	id int
	// 接受者承诺的提案编号 为0表示接受者还没有收到过任何Prepare信息
	minProposal int
	// 接受者已接受的提案编号 为0 表示还没有接受任何提案
	acceptedNumber int
	// 接受者已接受提案值 为nil 表示还没有接受任何提案
	acceptedValue interface{}

	// 学习者id列表
	learners []int
}

func newAcceptor(id int, learners []int) *Acceptor {
	acceptor := &Acceptor{
		id:       id,
		learners: learners,
	}
	acceptor.server()
	return acceptor
}

func (a *Acceptor) server() {
	rpcs := rpc.NewServer()
	rpcs.Register(a)
	addr := fmt.Sprintf(":%d", a.id)
	l, e := net.Listen("tcp", addr)
	if e != nil {
		log.Fatal("listen error: ", e)
	}
	a.lis = l
	go func() {
		for {
			conn, err := a.lis.Accept()
			if err != nil {
				continue
			}
			go rpcs.ServeConn(conn)
		}
	}()
}

func (a *Acceptor) close() {
	err := a.lis.Close()
	if err != nil {
		return
	}
}

func (a *Acceptor) Prepare(args *MsgArgs, reply *MsgReply) error {
	// 重复的也不要
	if args.Number > a.minProposal {
		a.minProposal = args.Number
		reply.Ok = true
		reply.Value = a.acceptedValue
		reply.Number = a.acceptedNumber
	} else {
		reply.Ok = false
	}
	return nil
}

func (a *Acceptor) Accept(args *MsgArgs, reply *MsgReply) error {
	if args.Number >= a.minProposal {
		// n小才不接受提案
		a.minProposal = args.Number
		a.acceptedValue = args.Value
		a.acceptedNumber = args.Number
		reply.Ok = true
		// 提议被接受后就广播给学习者，后台转发接受的提案给学习者
		for _, lid := range a.learners {
			go func(learner int) {
				addr := fmt.Sprintf("127.0.0.1:%d", learner)
				args.From = a.id
				args.To = learner
				resp := new(MsgReply)
				ok := call(addr, "Learner.Learn", args, resp)
				if !ok {
					return
				}
			}(lid)
		}
	} else {
		reply.Ok = false
	}
	return nil
}
