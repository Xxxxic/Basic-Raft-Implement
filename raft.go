package main

import (
	"fmt"
	"log"
	"math/rand"
	"net/http"
	"net/rpc"
	"time"
)

type node struct {
	connect bool
	address string
}

// 新建节点
func newNode(address string) *node {
	node := &node{}
	node.address = address
	node.connect = true // 默认连接上
	return node
}

// State def
type State int

// status of node
const (
	Follower State = iota + 1
	Candidate
	Leader
)

// 日志条目
type LogEntry struct {
	LogIndex int         // 日志条目索引
	LogTerm  int         // 日志条目任期
	LogCMD   interface{} // 操作命令
}

type RequestVoteArgs struct {
	Term        int
	CandidateID int
}

type RequestVoteReply struct {
	Term        int  // 处理请求节点的任期号，用于候选者更新自己的任期
	VoteGranted bool // 候选者获得选票时为 true; 否则为 false VoteGranted bool
}

type AppendEntriesArgs struct {
	Term     int
	LeaderID int

	PrevLogIndex int        // 新日志之前的索引
	PrevLogTerm  int        // PrevLogIndex 的任期号
	Entries      []LogEntry // 需要复制的日志条目，用于发送心跳消息时 Entries 为空
	LeaderCommit int        // Leader 已经commit的索引值
}

type AppendEntriesReply struct {
	Success bool
	Term    int

	NextIndex int // 如果 Follower Index小于 Leader Index， 会告诉 Leader 下次开始发送的索引位置
}

// Raft Node
type Raft struct {
	me int

	nodes map[int]*node

	state       State // 服务器当前状态
	currentTerm int   // 服务器当前已知的最新任期
	votedFor    int   // 当前任期内收到选票的候选人ID
	voteCount   int

	log []LogEntry // 状态机日志条目集合

	commitIndex int   // 被提交的最大索引
	lastApplied int   // 被应用到状态机的最大索引
	nextIndex   []int // 保存需要发送给每个节点的下一个条目索引
	matchIndex  []int // 保存已经复制给每个节点日志的最高索引

	// channels: 在 Go 中被用作协程（goroutine）之间的通信机制
	heartbeatC chan bool
	toLeaderC  chan bool
}

func (rf *Raft) getLastIndex() int {
	rlen := len(rf.log)
	if rlen == 0 {
		return 0
	}
	return rf.log[rlen-1].LogIndex
}

func (rf *Raft) getLastTerm() int {
	rlen := len(rf.log)
	if rlen == 0 {
		return 0
	}
	return rf.log[rlen-1].LogTerm
}

// 获得当前存活节点数
func (rf *Raft) getAliveNodeCount() int {
	count := 1
	for _, node := range rf.nodes {
		if node.connect {
			count++
		}
	}
	return count
}

// 广播 RequestVoteArgs RPC
func (rf *Raft) broadcastRequestVote() {
	var args = RequestVoteArgs{
		Term:        rf.currentTerm,
		CandidateID: rf.me,
	}

	for i := range rf.nodes {
		if rf.nodes[i].connect == false {
			continue
		}
		go func(i int) {
			var reply RequestVoteReply
			rf.sendRequestVote(i, args, &reply)
		}(i)
	}
}

// 发送 RequestVoteArgs RPC
func (rf *Raft) sendRequestVote(serverID int, args RequestVoteArgs, reply *RequestVoteReply) {
	client, err := rpc.DialHTTP("tcp", rf.nodes[serverID].address)
	if err != nil {
		//log.Fatal("dialing: ", err)
		log.Print("dialing: ", err) // Use log.Print instead of log.Fatal

		// 将连接不上的节点移出集群
		rf.nodes[serverID].connect = false
	}

	// defer 规定某个函数或者方法在执行结束后必须要执行的代码。
	defer func(client *rpc.Client) {
		if client != nil { // Add check for nil client
			err := client.Close()
			if err != nil {
				log.Print("close client error: ", err) // Use log.Print instead of log.Fatal
			}
		}
	}(client)

	// 连接上节点就发请求
	if err == nil {
		err = client.Call("Raft.RequestVote", args, reply)
		if err != nil {
			return
		}

		log.Print("sendRequestVote: ", reply)
		log.Print("My currentTerm: ", rf.currentTerm)
		// 收到比自己大的任期号 将自己的状态重置为 Follower
		if reply.Term > rf.currentTerm {
			rf.currentTerm = reply.Term
			rf.state = Follower
			rf.votedFor = -1
			return
		}

		// 收到选票：得票数加1
		if reply.VoteGranted {
			rf.voteCount++
		}
	}

	// 如果收到大多数节点的选票，就将自己的状态改为 Leader
	//if rf.voteCount >= len(rf.nodes)/2+1 {
	log.Print("My voteCount: ", rf.voteCount)
	log.Print("Alive node count: ", rf.getAliveNodeCount())
	if rf.voteCount >= (rf.getAliveNodeCount()+1)/2 {
		rf.toLeaderC <- true
	}
}

// RequestVote rpc method
func (rf *Raft) RequestVote(args RequestVoteArgs, reply *RequestVoteReply) error {
	// 如果收到的任期号小于自己的任期号，就拒绝投票
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		return nil
	}

	// 没有投票过 就把票投给它
	if rf.votedFor == -1 {
		rf.currentTerm = args.Term
		rf.votedFor = args.CandidateID
		reply.Term = rf.currentTerm
		reply.VoteGranted = true
	}

	// 如果已经投过票了，就拒绝投票
	return nil
}

// 广播 AppendEntries RPC
func (rf *Raft) broadcastAppendEntries() {
	for i := range rf.nodes {
		var args AppendEntriesArgs
		args.Term = rf.currentTerm
		args.LeaderID = rf.me
		args.LeaderCommit = rf.commitIndex

		// 计算 preLogIndex 、preLogTerm
		// 提取 preLogIndex - baseIndex 之后的entry，发送给 follower
		prevLogIndex := rf.nextIndex[i] - 1
		if rf.getLastIndex() > prevLogIndex {
			args.PrevLogIndex = prevLogIndex
			args.PrevLogTerm = rf.log[prevLogIndex].LogTerm
			args.Entries = rf.log[prevLogIndex:]
			log.Printf("send entries: %v\n", args.Entries)
		}

		go func(i int, args AppendEntriesArgs) {
			var reply AppendEntriesReply
			rf.sendAppendEntries(i, args, &reply)
		}(i, args)
	}
}

// 发送 AppendEntries RPC
func (rf *Raft) sendAppendEntries(serverID int, args AppendEntriesArgs, reply *AppendEntriesReply) {
	client, err := rpc.DialHTTP("tcp", rf.nodes[serverID].address)
	if err != nil {
		//log.Fatal("dialing:", err)
		log.Print("dialing:", err) // Use log.Print instead of log.Fatal
		// 连接不上就返回
		return
	}

	defer func(client *rpc.Client) {
		err := client.Close()
		if err != nil {
			log.Print("close client error: ", err) // Use log.Print instead of log.Fatal
		}
	}(client)

	err = client.Call("Raft.AppendEntries", args, reply)
	if err != nil {
		log.Print("call error: ", err) // Log the error and return
		return
	}

	// 如果 leader 节点落后于 follower 节点
	if reply.Success {
		if reply.NextIndex > 0 {
			rf.nextIndex[serverID] = reply.NextIndex
			rf.matchIndex[serverID] = rf.nextIndex[serverID] - 1
		}
	} else {
		// 如果 leader 的 term 小于 follower 的 term， 需要将 leader 转变为 follower 重新选举
		if reply.Term > rf.currentTerm {
			rf.currentTerm = reply.Term
			rf.state = Follower
			rf.votedFor = -1
			return
		}
	}
}

// AppendEntries rpc method
func (rf *Raft) AppendEntries(args AppendEntriesArgs, reply *AppendEntriesReply) error {
	// 如果 leader 节点小于当前节点 term
	if args.Term < rf.currentTerm {
		reply.Success = false
		reply.Term = rf.currentTerm
		return nil
	}

	// 如果只是 AppendEntries RPC 心跳
	rf.heartbeatC <- true
	if len(args.Entries) == 0 {
		reply.Success = true
		reply.Term = rf.currentTerm
		return nil
	}

	// 如果有 entries
	// leader 维护的 LogIndex 大于当前 Follower 的 LogIndex
	// 代表当前 Follower 失联过，所以 Follower 要告知 Leader 它当前
	// 的最大索引，以便下次心跳 Leader 返回
	if args.PrevLogIndex > rf.getLastIndex() {
		reply.Success = false
		reply.Term = rf.currentTerm
		reply.NextIndex = rf.getLastIndex() + 1
		return nil
	}

	rf.log = append(rf.log, args.Entries...)
	rf.commitIndex = rf.getLastIndex()
	reply.Success = true
	reply.Term = rf.currentTerm
	reply.NextIndex = rf.getLastIndex() + 1

	return nil
}

// 把 Raft 对象 rf 注册到 RPC 服务，其他程序就可以通过 RPC 调用 Raft 的方法
func (rf *Raft) rpc(port string) {
	err := rpc.Register(rf)
	if err != nil {
		return
	}
	rpc.HandleHTTP() // 通过 HTTP 协议传输 RPC 数据
	go func() {
		// 创建一个 goroutine轻量级线程 来监听端口。
		err := http.ListenAndServe(port, nil)
		if err != nil {
			log.Fatal("listen error: ", err)
		}
	}()
}

// 启动 raft
func (rf *Raft) start() {
	rf.state = Follower // 初始状态为 Follower
	rf.currentTerm = 0
	rf.votedFor = -1
	rf.heartbeatC = make(chan bool) // 应答心跳 创建通道
	rf.toLeaderC = make(chan bool)  // 转变leader状态 创建通道

	// 创建 goroutine 来处理 Raft 节点的状态转换和任务执行
	go func() {
		rand.Seed(time.Now().UnixNano()) // 生成随机数种子
		for {
			switch rf.state {
			// Follower: 监听来自 Leader 的心跳，如果一段时间内未收到心跳，就将自己的状态改为 Candidate。
			case Follower:
				select {
				case <-rf.heartbeatC:
					// log.Printf("follower-%d recived heartbeat\n", rf.me)
				case <-time.After(time.Duration(rand.Intn(500-300)+300) * time.Millisecond):
					log.Printf("follower-%d timeout\n", rf.me)
					rf.state = Candidate
				}
			// Candidate: 向其他节点发送 RequestVoteArgs RPC，如果收到大多数节点的选票，就将自己的状态改为 Leader。
			case Candidate:
				fmt.Printf("Node: %d, I'm candidate\n", rf.me)
				rf.currentTerm++
				rf.votedFor = rf.me
				rf.voteCount = 1
				go rf.broadcastRequestVote() // 广播发起投票请求，开始选举。

				select {
				// 如果在一个随机的时间间隔（300ms-500ms）后没有达到 Leader 状态，那么就回退到 Follower 状态。
				case <-time.After(time.Duration(rand.Intn(500-300)+300) * time.Millisecond):
					rf.state = Follower
				// 如果收到大多数节点的选票，就将自己的状态改为 Leader。
				case <-rf.toLeaderC:
					fmt.Printf("Node: %d, I'm leader\n", rf.me)
					rf.state = Leader

					// 初始化 peers 的 nextIndex 和 matchIndex
					rf.nextIndex = make([]int, len(rf.nodes))
					rf.matchIndex = make([]int, len(rf.nodes))
					for i := range rf.nodes {
						rf.nextIndex[i] = 1
						rf.matchIndex[i] = 0
					}

					go func() {
						i := 0
						for {
							i++
							rf.log = append(rf.log, LogEntry{rf.currentTerm, i, fmt.Sprintf("user send : %d", i)})
							time.Sleep(5 * time.Second)
						}
					}()
				}
			// Leader: 向其他节点广播心跳，维持 Leader 地位。
			case Leader:
				rf.broadcastAppendEntries()
				time.Sleep(200 * time.Millisecond)
			}
		}
	}()
}
