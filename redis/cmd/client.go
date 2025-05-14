package main

import (
	"SingleKVDataSet"
	bitcask_redis "SingleKVDataSet/redis"
	"SingleKVDataSet/utils"
	"fmt"
	"github.com/tidwall/redcon"
	"strings"
)

func newWrongNumberOfArgsError(cmd string) error {
	return fmt.Errorf("Err wrong number of arguments for '%s' command", cmd)
}

type cmdHandler func(cli *BitcaskClient, args [][]byte) (interface{}, error)

var supportedCommands = map[string]cmdHandler{
	"set":       set,
	"get":       get,
	"hset":      hset,
	"hget":      hget,
	"sadd":      sadd,
	"sismember": sismember,
	"srem":      srem,
	"lpush":     lpush,
	"rpush":     rpush,
	"lpop":      lpop,
	"rpop":      rpop,
	"zadd":      zadd,
	"zscore":    zscore,
}

type BitcaskClient struct {
	server *BitcaskServer
	db     *bitcask_redis.RedisDataStructure
}

func execClientCommand(conn redcon.Conn, cmd redcon.Command) {
	command := strings.ToLower(string(cmd.Args[0]))
	cmdFunc, ok := supportedCommands[command]
	if !ok {
		conn.WriteError("ERR unsupported command '" + string(cmd.Args[0]) + "'")
		return
	}

	cilent, _ := conn.Context().(*BitcaskClient)
	switch command {
	case "quit":
		_ = conn.Close()
	case "ping":
		conn.WriteString("PONG")
	default:
		res, err := cmdFunc(cilent, cmd.Args[1:])
		if err == SingleKVDataSet.ErrKeyNotFound {
			conn.WriteNull()
		} else {
			conn.WriteError(err.Error())
		}
		conn.WriteAny(res)
	}
}

func set(cli *BitcaskClient, args [][]byte) (interface{}, error) {
	if len(args) != 2 {
		return nil, newWrongNumberOfArgsError("SET")
	}
	key, value := args[0], args[1]
	if err := cli.db.Set(key, 0, value); err != nil {
		return nil, err
	}
	return redcon.SimpleString("OK"), nil
}

func get(cli *BitcaskClient, args [][]byte) (interface{}, error) {
	if len(args) != 1 {
		return nil, newWrongNumberOfArgsError("GET")
	}
	value, err := cli.db.Get(args[0])
	if err != nil {
		return nil, err
	}
	return value, nil
}

func hset(cli *BitcaskClient, args [][]byte) (interface{}, error) {
	if len(args) != 3 {
		return nil, newWrongNumberOfArgsError("HSET")
	}
	var ok = 0
	key, field, value := args[0], args[1], args[2]
	res, err := cli.db.HSet(key, field, value)
	if err != nil {
		return nil, err
	}
	if res {
		ok = 1
	}
	return redcon.SimpleInt(ok), nil
}

func hget(cli *BitcaskClient, args [][]byte) (interface{}, error) {
	if len(args) != 2 {
		return nil, newWrongNumberOfArgsError("HGET")
	}
	key, field := args[0], args[1]

	res, err := cli.db.HGet(key, field)
	if err != nil {
		return nil, err
	}
	return redcon.SimpleString(res), nil
}

func sadd(cli *BitcaskClient, args [][]byte) (interface{}, error) {
	if len(args) != 2 {
		return nil, newWrongNumberOfArgsError("SADD")
	}
	var ok = 0
	key, member := args[0], args[1]
	res, err := cli.db.SAdd(key, member)
	if err != nil {
		return nil, err
	}
	if res {
		ok = 1
	}
	return redcon.SimpleInt(ok), nil
}

func sismember(cli *BitcaskClient, args [][]byte) (interface{}, error) {
	if len(args) != 2 {
		return nil, newWrongNumberOfArgsError("SISMEMBER")
	}
	var ok = 0
	key, member := args[0], args[1]
	res, err := cli.db.SIsMember(key, member)
	if err != nil {
		return nil, err
	}
	if res {
		ok = 1
	}
	return redcon.SimpleInt(ok), nil
}

func srem(cli *BitcaskClient, args [][]byte) (interface{}, error) {
	if len(args) != 2 {
		return nil, newWrongNumberOfArgsError("SREM")
	}
	var ok = 0
	key, member := args[0], args[1]
	res, err := cli.db.SRem(key, member)
	if err != nil {
		return nil, err
	}
	if res {
		ok = 1
	}
	return redcon.SimpleInt(ok), nil
}

func lpush(cli *BitcaskClient, args [][]byte) (interface{}, error) {
	if len(args) != 2 {
		return nil, newWrongNumberOfArgsError("LPUSH")
	}

	key, value := args[0], args[1]
	res, err := cli.db.LPush(key, value)
	if err != nil {
		return nil, err
	}
	return redcon.SimpleInt(res), nil
}

func rpush(cli *BitcaskClient, args [][]byte) (interface{}, error) {
	if len(args) != 2 {
		return nil, newWrongNumberOfArgsError("RPUSH")
	}

	key, value := args[0], args[1]
	res, err := cli.db.RPush(key, value)
	if err != nil {
		return nil, err
	}
	return redcon.SimpleInt(res), nil
}

func lpop(cli *BitcaskClient, args [][]byte) (interface{}, error) {
	if len(args) != 1 {
		return nil, newWrongNumberOfArgsError("LPOP")
	}
	key := args[0]
	res, err := cli.db.LPop(key)
	if err != nil {
		return nil, err
	}
	return redcon.SimpleString(res), nil
}

func rpop(cli *BitcaskClient, args [][]byte) (interface{}, error) {
	if len(args) != 1 {
		return nil, newWrongNumberOfArgsError("RPOP")
	}
	key := args[0]
	res, err := cli.db.RPop(key)
	if err != nil {
		return nil, err
	}
	return redcon.SimpleString(res), nil
}

func zadd(cli *BitcaskClient, args [][]byte) (interface{}, error) {
	if len(args) != 3 {
		return nil, newWrongNumberOfArgsError("ZADD")
	}
	var ok = 0
	key, score, value := args[0], args[1], args[2]
	res, err := cli.db.ZAdd(key, utils.FloatFromBytes(score), value)
	if err != nil {
		return nil, err
	}
	if res {
		ok = 1
	}
	return redcon.SimpleInt(ok), nil
}

func zscore(cli *BitcaskClient, args [][]byte) (interface{}, error) {
	if len(args) != 2 {
		return nil, newWrongNumberOfArgsError("ZSCORE")
	}
	key, member := args[0], args[1]
	res, err := cli.db.ZScore(key, member)
	if err != nil {
		return nil, err
	}
	// todo 返回是一个float
	return redcon.SimpleInt(res), nil
}
