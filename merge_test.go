package SingleKVDataSet

import (
	"SingleKVDataSet/utils"
	"github.com/stretchr/testify/assert"
	"os"
	"testing"
)

// 没有任何数据的时候进行merge操作
func TestDB_Merge(t *testing.T) {
	opts := DefaultOptions
	dir, _ := os.MkdirTemp("./TestingFile", "bitcask-go-merge-1")
	opts.DirPath = dir
	db, err := Open(opts)
	defer destroyDB(db)
	assert.Nil(t, err)
	assert.NotNil(t, db)

	err = db.Merge()
	//t.Log(err)
	assert.Nil(t, err)
}

// 全部是有效数据的时候进行merger操作
func TestDB_Merge_Valid(t *testing.T) {
	opts := DefaultOptions
	dir, _ := os.MkdirTemp("./TestingFile", "bitcask-go-merge-2")
	opts.DirPath = dir
	opts.DataFileSize = 32 * 1024 * 1024
	opts.DataFileMergeRatio = 0
	db, err := Open(opts)
	//defer destroyDB(db)
	assert.Nil(t, err)
	assert.NotNil(t, db)

	for i := 0; i < 50000; i++ {
		err := db.Put(utils.GetTestKey(i), utils.RandomValue(256))
		assert.Nil(t, err)
	}

	err = db.Merge()
	//t.Log(err)
	assert.Nil(t, err)

	err = db.Close()
	assert.Nil(t, err)
	db2, err := Open(opts)
	defer destroyDB(db2)
	assert.Nil(t, err)

	keys := db2.ListKeys()
	assert.Equal(t, len(keys), 50000)

	for i := 0; i < 50000; i++ {
		val, err := db2.Get(utils.GetTestKey(i))
		assert.Nil(t, err)
		assert.NotNil(t, val)
	}
}

// 有部分失效数据的情况下进行Merge, 并且还有重复put的数据
func TestDB_Merge_Valid_Invalid(t *testing.T) {
	opts := DefaultOptions
	dir, _ := os.MkdirTemp("./TestingFile", "bitcask-go-merge-3")
	opts.DirPath = dir
	opts.DataFileSize = 32 * 1024 * 1024
	opts.DataFileMergeRatio = 0
	db, err := Open(opts)
	//defer destroyDB(db)
	assert.Nil(t, err)
	assert.NotNil(t, db)

	for i := 0; i < 50000; i++ {
		err := db.Put(utils.GetTestKey(i), utils.RandomValue(256))
		assert.Nil(t, err)
	}

	for i := 0; i < 10000; i++ {
		err := db.Delete(utils.GetTestKey(i))
		assert.Nil(t, err)
	}
	for i := 40000; i < 50000; i++ {
		err := db.Put(utils.GetTestKey(i), []byte("new value"))
		assert.Nil(t, err)
	}

	err = db.Merge()
	assert.Nil(t, err)

	// 重启后校验
	err = db.Close()
	assert.Nil(t, err)

	db2, err := Open(opts)
	defer destroyDB(db2)
	assert.Nil(t, err)

	keys := db2.ListKeys()
	assert.Equal(t, len(keys), 40000)

	for i := 0; i < 10000; i++ {
		_, err := db2.Get(utils.GetTestKey(i))
		assert.Equal(t, err, ErrKeyNotFound)
	}

	for i := 40000; i < 50000; i++ {
		val, err := db2.Get(utils.GetTestKey(i))
		assert.Nil(t, err)
		assert.Equal(t, val, []byte("new value"))
	}
}

// 全部是无效数据
func TestDB_Merge_Invalid(t *testing.T) {
	opts := DefaultOptions
	dir, _ := os.MkdirTemp("./TestingFile", "bitcask-go-merge-3")
	opts.DirPath = dir
	opts.DataFileSize = 32 * 1024 * 1024
	opts.DataFileMergeRatio = 0
	db, err := Open(opts)
	//defer destroyDB(db)
	assert.Nil(t, err)
	assert.NotNil(t, db)

	for i := 0; i < 50000; i++ {
		err := db.Put(utils.GetTestKey(i), utils.RandomValue(256))
		assert.Nil(t, err)
	}

	for i := 0; i < 50000; i++ {
		err := db.Delete(utils.GetTestKey(i))
		assert.Nil(t, err)
	}

	err = db.Merge()
	assert.Nil(t, err)

	// 重启后校验
	err = db.Close()
	assert.Nil(t, err)

	db2, err := Open(opts)
	defer destroyDB(db2)
	assert.Nil(t, err)

	keys := db2.ListKeys()
	assert.Equal(t, len(keys), 0)
}
