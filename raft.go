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

// LogEntry struct
type LogEntry struct {
	LogTerm  int
	LogIndex int
	LogCMD   interface{}
}

type RequestVote struct {
	Term        int
	CandidateID int
}

type RequestVoteReply struct {
	//当前任期号， 以便候选人去更新自己的任期号
	Term int
	//候选人赢得此张选票时为真
	VoteGranted bool
}

type AppendEntriesArgs struct {
	Term     int
	LeaderID int

	// 新日志之前的索引
	PrevLogIndex int
	// PrevLogIndex 的任期号
	PrevLogTerm int
	// 准备存储的日志条目（表示心跳时为空）
	Entries []LogEntry
	// Leader 已经commit的索引值
	LeaderCommit int
}

type AppendEntriesReply struct {
	Success bool
	Term    int

	// 如果 Follower Index小于 Leader Index， 会告诉 Leader 下次开始发送的索引位置
	NextIndex int
}

// Raft Node
type Raft struct {
	me int

	nodes map[int]*node

	state       State
	currentTerm int
	votedFor    int
	voteCount   int

	// 日志条目集合
	log []LogEntry

	// 被提交的最大索引
	commitIndex int
	// 被应用到状态机的最大索引
	lastApplied int

	// 保存需要发送给每个节点的下一个条目索引
	nextIndex []int
	// 保存已经复制给每个节点日志的最高索引
	matchIndex []int

	// channels
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

func (rf *Raft) broadcastRequestVote() {
	var args = RequestVote{
		Term:        rf.currentTerm,
		CandidateID: rf.me,
	}

	for i := range rf.nodes {
		go func(i int) {
			var reply RequestVoteReply
			rf.sendRequestVote(i, args, &reply)
		}(i)
	}
}

func (rf *Raft) sendRequestVote(serverID int, args RequestVote, reply *RequestVoteReply) {
	client, err := rpc.DialHTTP("tcp", rf.nodes[serverID].address)
	if err != nil {
		log.Fatal("dialing: ", err)
	}

	defer func(client *rpc.Client) {
		err := client.Close()
		if err != nil {
			log.Fatal("close client error: ", err)
		}
	}(client)
	err = client.Call("Raft.RequestVote", args, reply)
	if err != nil {
		return
	}

	// 当前candidate节点无效
	if reply.Term > rf.currentTerm {
		rf.currentTerm = reply.Term
		rf.state = Follower
		rf.votedFor = -1
		return
	}

	if reply.VoteGranted {
		rf.voteCount++
	}

	if rf.voteCount >= len(rf.nodes)/2+1 {
		rf.toLeaderC <- true
	}
}

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

func (rf *Raft) sendAppendEntries(serverID int, args AppendEntriesArgs, reply *AppendEntriesReply) {
	client, err := rpc.DialHTTP("tcp", rf.nodes[serverID].address)
	if err != nil {
		log.Fatal("dialing:", err)
	}

	defer func(client *rpc.Client) {
		err := client.Close()
		if err != nil {
			log.Fatal("close client error: ", err)
		}
	}(client)
	err = client.Call("Raft.Heartbeat", args, reply)
	if err != nil {
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
					log.Printf("follower-%d recived heartbeat\n", rf.me)
				case <-time.After(time.Duration(rand.Intn(500-300)+300) * time.Millisecond):
					log.Printf("follower-%d timeout\n", rf.me)
					rf.state = Candidate
				}
			// Candidate: 向其他节点发送 RequestVote RPC，如果收到大多数节点的选票，就将自己的状态改为 Leader。
			case Candidate:
				fmt.Printf("Node: %d, I'm candidate\n", rf.me)
				rf.currentTerm++
				rf.votedFor = rf.me
				rf.voteCount = 1
				go rf.broadcastRequestVote()

				select {
				case <-time.After(time.Duration(rand.Intn(500-300)+300) * time.Millisecond):
					rf.state = Follower
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
							time.Sleep(3 * time.Second)
						}
					}()
				}
			// Leader: 向其他节点广播心跳，维持 Leader 地位。
			case Leader:
				rf.broadcastAppendEntries()
				time.Sleep(100 * time.Millisecond)
			}
		}
	}()
}
