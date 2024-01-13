package main

import (
	"fmt"
	"log"
	"net"
	"net/rpc"
	"testing"
)

func main() {

}

func start(acceptorIds []int, learnerIds []int) ([]*Acceptor, []*Learner) {
	acceptors := make([]*Acceptor, 0)
	for _, aid := range acceptorIds {
		a := newAcceptor(aid, learnerIds)
		acceptors = append(acceptors, a)
	}
	learners := make([]*Learner, 0)
	for _, lid := range learnerIds {
		l := newLearner(lid, acceptorIds)
		learners = append(learners, l)
	}

	return acceptors, learners
}

func cleanup(acceptors []*Acceptor, learners []*Learner) {
	for _, a := range acceptors {
		a.close()
	}
	for _, l := range learners {
		l.close()
	}
}

func TestSingleProposer(t *testing.T) {
	acceptorIds := []int{1001, 1002, 1003}
	learnerIds := []int{2001}
	acceptors, leanrners := start(acceptorIds, learnerIds)

	defer cleanup(acceptors, leanrners)

	p := &Proposer{
		id:        1,
		acceptors: acceptorIds,
	}
	value := p.propose("hello world")
	if value != "hello world" {
		t.Errorf("value = %s, except %s", value, "hello world")
	}

	learnValue := leanrners[0].chosen()
	if learnValue != value {
		t.Errorf("learnValue = %s, except %s", learnValue, "hello world")
	}
}

func TestTwoProposer(t *testing.T) {
	acceptorIds := []int{1001, 1002, 1003}
	learnerIds := []int{2001}
	acceptors, leanrners := start(acceptorIds, learnerIds)

	defer cleanup(acceptors, leanrners)

	p1 := &Proposer{
		id:        1,
		acceptors: acceptorIds,
	}
	v1 := p1.propose("hello world")
	p2 := &Proposer{
		id:        2,
		acceptors: acceptorIds,
	}
	v2 := p2.propose("no hello world")
	if v1 != v2 {
		t.Errorf("v1 = %s, v2 =  %s", v1, v2)
	}

	learnValue := leanrners[0].chosen()
	if learnValue != v1 {
		t.Errorf("learnValue = %s, except %s", learnValue, v1)
	}
}

type Proposer struct {
	id        int
	round     int
	number    int
	acceptors []int
}

func (p *Proposer) propose(v interface{}) interface{} {
	p.round++
	p.number = p.proposalNumber()

	// 一阶段
	prepareCount := 0
	maxNumber := 0
	for _, aid := range p.acceptors {
		args := MsgArgs{
			Number: p.number,
			From:   p.id,
			To:     aid,
		}
		reply := new(MsgReply)
		err := call(fmt.Sprintf("127.0.0.1:%d", aid), "Acceptor.Prepare", args, reply)
		if !err {
			continue
		}

		if reply.Ok {
			prepareCount++
			if reply.Number > maxNumber {
				maxNumber = reply.Number
				v = reply.Value
			}
		}

		if prepareCount == p.majority() {
			break
		}
	}

	// 二阶段
	acceptCount := 0
	if prepareCount >= p.majority() {
		for _, aid := range p.acceptors {
			args := MsgArgs{
				Number: p.number,
				Value:  v,
				From:   p.id,
				To:     aid,
			}
			reply := new(MsgReply)
			ok := call(fmt.Sprintf("127.0.0.1:%d", aid), "Acceptor.Accept", args, reply)
			if !ok {
				continue
			}

			// 接受提案
			if reply.Ok {
				acceptCount++
			}
		}
	}

	// 批准提案
	if acceptCount > p.majority() {
		return v
	}
	return nil
}

func (p *Proposer) majority() int {
	return len(p.acceptors)/2 + 1
}

// 提案编号 =（轮次，服务器id）
func (p *Proposer) proposalNumber() int {
	return p.round<<16 | p.id
}

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
