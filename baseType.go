package main

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
