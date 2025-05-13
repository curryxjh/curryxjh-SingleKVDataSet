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
