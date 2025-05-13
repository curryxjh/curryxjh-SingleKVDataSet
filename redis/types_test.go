package redis

import (
	bitcask "SingleKVDataSet"
	"SingleKVDataSet/utils"
	"github.com/stretchr/testify/assert"
	"os"
	"testing"
	"time"
)

func TestRedisDataStructure_Get(t *testing.T) {
	opts := bitcask.DefaultOptions
	dir, _ := os.MkdirTemp("../TestingFile", "bitcask-go-string-get")
	opts.DirPath = dir
	rds, err := NewRedisDataStructure(opts)
	assert.Nil(t, err)

	err = rds.Set(utils.GetTestKey(1), 0, utils.RandomValue(256))
	assert.Nil(t, err)
	err = rds.Set(utils.GetTestKey(2), time.Second*5, utils.RandomValue(256))
	assert.Nil(t, err)

	val1, err := rds.Get(utils.GetTestKey(1))
	assert.Nil(t, err)
	//t.Log(string(val1))
	assert.NotNil(t, val1)

	val2, err := rds.Get(utils.GetTestKey(2))
	assert.Nil(t, err)
	//t.Log(string(val2))
	assert.NotNil(t, val2)

	_, err = rds.Get(utils.GetTestKey(3))
	assert.Equal(t, err, bitcask.ErrKeyNotFound)
}

func TestRedisDataStructure_Del_Type(t *testing.T) {
	opts := bitcask.DefaultOptions
	dir, _ := os.MkdirTemp("../TestingFile", "bitcask-go-string-del-type")
	opts.DirPath = dir
	rds, err := NewRedisDataStructure(opts)
	assert.Nil(t, err)

	// del
	err = rds.Del(utils.GetTestKey(11))
	assert.Nil(t, err)

	err = rds.Set(utils.GetTestKey(1), 0, utils.RandomValue(256))
	assert.Nil(t, err)

	err = rds.Del(utils.GetTestKey(1))
	assert.Nil(t, err)

	_, err = rds.Get(utils.GetTestKey(1))
	//t.Log(err)
	assert.Equal(t, err, bitcask.ErrKeyNotFound)

	// type
	err = rds.Set(utils.GetTestKey(2), 0, utils.RandomValue(256))
	assert.Nil(t, err)

	typ, err := rds.Type(utils.GetTestKey(2))
	assert.Nil(t, err)
	assert.Equal(t, typ, String)
}

func TestRedisDataStructure_HSet_HGet(t *testing.T) {
	opts := bitcask.DefaultOptions
	dir, _ := os.MkdirTemp("../TestingFile", "bitcask-go-hash-get")
	opts.DirPath = dir
	rds, err := NewRedisDataStructure(opts)
	assert.Nil(t, err)

	ok1, err := rds.HSet(utils.GetTestKey(1), []byte("field1"), utils.RandomValue(256))
	assert.Nil(t, err)
	assert.True(t, ok1)
	//t.Log(ok1)

	v1 := utils.RandomValue(256)
	ok2, err := rds.HSet(utils.GetTestKey(1), []byte("field1"), v1)
	assert.Nil(t, err)
	assert.False(t, ok2)
	//t.Log(ok2)

	v2 := utils.RandomValue(256)
	ok3, err := rds.HSet(utils.GetTestKey(1), []byte("field2"), v2)
	assert.Nil(t, err)
	assert.True(t, ok3)
	//t.Log(ok3)

	val1, err := rds.HGet(utils.GetTestKey(1), []byte("field1"))
	assert.Nil(t, err)
	//t.Log(string(val1))
	assert.Equal(t, val1, v1)

	val2, err := rds.HGet(utils.GetTestKey(1), []byte("field2"))
	assert.Nil(t, err)
	//t.Log(string(val2))
	assert.Equal(t, val2, v2)

	val3, err := rds.HGet(utils.GetTestKey(1), []byte("field3"))
	assert.Nil(t, val3)
	assert.Equal(t, err, bitcask.ErrKeyNotFound)
	//t.Log(string(val3))
	//t.Log(err)
}

func TestRedisDataStructure_HDel(t *testing.T) {
	opts := bitcask.DefaultOptions
	dir, _ := os.MkdirTemp("../TestingFile", "bitcask-go-hash-del")
	opts.DirPath = dir
	rds, err := NewRedisDataStructure(opts)
	assert.Nil(t, err)

	del1, err := rds.HDel(utils.GetTestKey(100), []byte("field1"))
	//t.Log(del1)
	//t.Log(err)
	assert.False(t, del1)
	assert.Nil(t, err)

	ok1, err := rds.HSet(utils.GetTestKey(1), []byte("field1"), utils.RandomValue(256))
	assert.Nil(t, err)
	assert.True(t, ok1)

	v1 := utils.RandomValue(256)
	ok2, err := rds.HSet(utils.GetTestKey(1), []byte("field1"), v1)
	assert.Nil(t, err)
	assert.False(t, ok2)

	v2 := utils.RandomValue(256)
	ok3, err := rds.HSet(utils.GetTestKey(1), []byte("field2"), v2)
	assert.Nil(t, err)
	assert.True(t, ok3)

	del2, err := rds.HDel(utils.GetTestKey(1), []byte("field1"))
	//t.Log(del2)
	//t.Log(err)
	assert.True(t, del2)
	assert.Nil(t, err)
}
