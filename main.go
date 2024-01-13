package main

import (
	"flag"
	"log"
	"strings"
)

func main() {
	// 命令行参数
	port := flag.String("port", ":9091", "rpc listen port")
	cluster := flag.String("cluster", "127.0.0.1:9091", "comma sep")
	id := flag.Int("id", 1, "node ID")

	flag.Parse()
	clusters := strings.Split(*cluster, ",")
	log.Printf("port:%s, cluster:%v, ID:%d", *port, clusters, *id)

	// 初始化一个 ns 的 map，key 是整数，value 是指针类型的 node。
	ns := make(map[int]*node)
	for k, v := range clusters {
		ns[k] = newNode(v)
	}

	// 初始化 raft
	raft := &Raft{}
	raft.me = *id
	raft.nodes = ns
	raft.rpc(*port)
	raft.start()

	// 一直阻塞
	select {}
}
