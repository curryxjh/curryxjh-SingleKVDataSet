package SingleKVDataSet

import (
	"SingleKVDataSet/utils"
	"fmt"
	"github.com/stretchr/testify/assert"
	"os"
	"testing"
)

func TestDB_NewWriteBatch1(t *testing.T) {
	opts := DefaultOptions
	dir, _ := os.MkdirTemp("./TestingFile", "bitcask-go-batch-1")
	opts.DirPath = dir
	db, err := Open(opts)
	defer destroyDB(db)
	assert.Nil(t, err)
	assert.NotNil(t, db)

	// 写数据之后不提交
	wb := db.NewWriteBatch(DefaultWriteBatchOptions)
	err = wb.Put(utils.GetTestKey(1), utils.RandomValue(10))
	assert.Nil(t, err)
	err = wb.Delete(utils.GetTestKey(2))

	_, err = db.Get(utils.GetTestKey(1))
	assert.Equal(t, ErrKeyNotFound, err)

	// 正常提交数据
	err = wb.Commit()
	assert.Nil(t, err)

	val1, err := db.Get(utils.GetTestKey(1))
	//t.Log(val1)
	//t.Log(err)
	assert.Nil(t, err)
	assert.NotNil(t, val1)

	// 删除有效数据
	wb2 := db.NewWriteBatch(DefaultWriteBatchOptions)
	err = wb2.Delete(utils.GetTestKey(1))
	assert.Nil(t, err)
	err = wb2.Commit()
	assert.Nil(t, err)

	_, err = db.Get(utils.GetTestKey(1))
	//t.Log(val2)

	//t.Log(err)
	assert.Equal(t, ErrKeyNotFound, err)
}

func TestDB_NewReadBatch2(t *testing.T) {
	opts := DefaultOptions
	dir, _ := os.MkdirTemp("./TestingFile", "bitcask-go-batch-2")
	opts.DirPath = dir
	fmt.Println(opts.DirPath)
	db, err := Open(opts)
	assert.Nil(t, err)
	assert.NotNil(t, db)

	err = db.Put(utils.GetTestKey(1), utils.RandomValue(10))
	assert.Nil(t, err)

	wb := db.NewWriteBatch(DefaultWriteBatchOptions)
	err = wb.Put(utils.GetTestKey(2), utils.RandomValue(10))
	assert.Nil(t, err)
	err = wb.Delete(utils.GetTestKey(1))
	assert.Nil(t, err)

	err = wb.Commit()
	assert.Nil(t, err)

	err = wb.Put(utils.GetTestKey(22), utils.RandomValue(10))
	assert.Nil(t, err)
	err = wb.Commit()
	assert.Nil(t, err)

	// 重启
	err = db.Close()
	assert.Nil(t, err)

	db2, err := Open(opts)
	defer destroyDB(db2)
	assert.Nil(t, err)

	_, err = db2.Get(utils.GetTestKey(1))
	//t.Log(val)
	//t.Log(err)
	assert.Equal(t, ErrKeyNotFound, err)
	//t.Log(err)

	// 校验序列号
	//t.Log(db.seqNo)
	assert.Equal(t, uint64(2), db.seqNo)
}

//
//func TestDB_NewWriteBatch3(t *testing.T) {
//	opts := DefaultOptions
//	dir, _ := os.MkdirTemp("./TestingFile", "bitcask-go-batch-3")
//	opts.DirPath = dir
//	fmt.Println(opts.DirPath)
//	db, err := Open(opts)
//	defer destroyDB(db)
//	assert.Nil(t, err)
//	assert.NotNil(t, db)
//
//	keys := db.ListKeys()
//	t.Log(len(keys))
//
//	wbOpts := DefaultWriteBatchOptions
//	wbOpts.MaxBatchNum = 10000000
//	wb := db.NewWriteBatch(wbOpts)
//	for i := 0; i < 500000; i++ {
//		err = wb.Put(utils.GetTestKey(i), utils.RandomValue(1024))
//		assert.Nil(t, err)
//	}
//	err = wb.Commit()
//	assert.Nil(t, err)
//}
