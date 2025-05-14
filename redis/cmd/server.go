package main

import (
	bitcask "SingleKVDataSet"
	bitcask_redis "SingleKVDataSet/redis"
	"github.com/tidwall/redcon"
	"log"
	"sync"
)

const addr = "127.0.0.1:6380"

type BitcaskServer struct {
	dbs    map[int]*bitcask_redis.RedisDataStructure
	server *redcon.Server
	mu     *sync.RWMutex
}

func main() {
	// 打开 Redis 数据结构服务
	redisDataStructure, err := bitcask_redis.NewRedisDataStructure(bitcask.DefaultOptions)
	if err != nil {
		panic(err)
	}

	// 初始化 BitcaskServer
	bitcaskServer := &BitcaskServer{
		dbs: make(map[int]*bitcask_redis.RedisDataStructure),
	}
	bitcaskServer.dbs[0] = redisDataStructure

	// 初始化一个 Redis 服务器
	bitcaskServer.server = redcon.NewServer(addr, execClientCommand, bitcaskServer.accept, bitcaskServer.close)
	bitcaskServer.listen()
}

func (svr *BitcaskServer) listen() {
	log.Println("Bitcask server running, ready to accept connections")
	_ = svr.server.ListenAndServe()
}

func (svr *BitcaskServer) accept(conn redcon.Conn) bool {
	cli := new(BitcaskClient)
	svr.mu.Lock()
	defer svr.mu.Unlock()
	cli.server = svr
	cli.db = svr.dbs[0]
	conn.SetContext(cli)
	return true
}

func (svr *BitcaskServer) close(conn redcon.Conn, err error) {
	for _, db := range svr.dbs {
		_ = db.Close()
	}
	_ = svr.server.Close()
}

// redis 协议解析示例
//func main() {
//	conn, err := net.Dial("tcp", "127.0.0.1:6379")
//	if err != nil {
//		panic(err)
//	}
//
//	// 向 redis 发送命令
//	cmd := "set k-name bitcask-kv\r\n"
//	conn.Write([]byte(cmd))
//
//	// 解析 redis 响应
//	reader := bufio.NewReader(conn)
//	res, err := reader.ReadString('\n')
//	if err != nil {
//		panic(err)
//	}
//	fmt.Println(res)
//}
