package SingleKVDataSet

import (
	"SingleKVDataSet/utils"
	"github.com/stretchr/testify/assert"
	"os"
	"testing"
	"time"
)

// 测试完成后销毁DB数据目录
func destroyDB(db *DB) {
	if db != nil {
		if db.activeFile != nil {
			err := db.Close()
			if err != nil {
				panic(err)
			}
		} else {
			_ = db.Close()
		}
		err := os.RemoveAll(db.options.DirPath)
		if err != nil {
			panic(err)
		}
	}
}

func TestOpen(t *testing.T) {
	opts := DefaultOptions
	dir, _ := os.MkdirTemp("./TestingFile", "bitcask-go")
	opts.DirPath = dir
	db, err := Open(opts)
	defer destroyDB(db)
	assert.Nil(t, err)
	assert.NotNil(t, db)
}
func TestDB_Put(t *testing.T) {
	opts := DefaultOptions
	dir, _ := os.MkdirTemp("./TestingFile", "bitcask-go-put")
	opts.DirPath = dir
	db, err := Open(opts)
	assert.Nil(t, err)
	assert.NotNil(t, db)

	// 1.正常 Put 一条数据
	err = db.Put(utils.GetTestKey(1), utils.RandomValue(24))
	assert.Nil(t, err)
	val1, err := db.Get(utils.GetTestKey(1))
	assert.Nil(t, err)
	assert.NotNil(t, val1)

	// 2.重复 Put key 相同的数据
	err = db.Put(utils.GetTestKey(1), utils.RandomValue(24))
	assert.Nil(t, err)
	val2, err := db.Get(utils.GetTestKey(1))
	assert.Nil(t, err)
	assert.NotNil(t, val2)

	// 3.key 为空
	err = db.Put(nil, utils.RandomValue(24))
	assert.Equal(t, ErrKeyIsEmpty, err)

	// 4.value 为空
	err = db.Put(utils.GetTestKey(2), nil)
	assert.Nil(t, err)
	val3, err := db.Get(utils.GetTestKey(2))
	assert.Equal(t, 0, len(val3))
	assert.Nil(t, err)

	// 5.写到数据文件进行了转换
	for i := 0; i < 2000000; i++ {
		err := db.Put(utils.GetTestKey(i), utils.RandomValue(128))
		assert.Nil(t, err)
	}
	//t.Log(len(db.oldFiles))
	assert.Equal(t, 1, len(db.oldFiles))

	err = db.Close()
	db2, err := Open(opts)
	defer destroyDB(db2)
	assert.Nil(t, err)
	assert.NotNil(t, db)

	val4 := utils.RandomValue(128)
	err = db2.Put(utils.GetTestKey(3), val4)
	assert.Nil(t, err)
	val5, err := db2.Get(utils.GetTestKey(3))
	assert.Nil(t, err)
	assert.Equal(t, val4, val5)
}

func TestDB_Get(t *testing.T) {
	opts := DefaultOptions
	dir, _ := os.MkdirTemp("./TestingFile", "bitcask-go-get")
	opts.DirPath = dir
	opts.DataFileSize = 64 * 1024 * 1024
	db, err := Open(opts)
	assert.Nil(t, err)
	assert.NotNil(t, db)

	// 正常读取一条数据
	err = db.Put(utils.GetTestKey(1), utils.RandomValue(24))
	assert.Nil(t, err)
	val1, err := db.Get(utils.GetTestKey(1))
	assert.Nil(t, err)
	assert.NotNil(t, val1)

	// 读取一个不存在的key
	val2, err := db.Get(utils.GetTestKey(2))
	assert.Nil(t, val2)
	assert.Equal(t, ErrKeyNotFound, err)

	// 值被重复put后读取
	err = db.Put(utils.GetTestKey(2), utils.RandomValue(24))
	assert.Nil(t, err)
	err = db.Put(utils.GetTestKey(2), utils.RandomValue(24))
	assert.Nil(t, err)
	val3, err := db.Get(utils.GetTestKey(2))
	assert.Nil(t, err)
	assert.NotNil(t, val3)

	// 值被删除后再Get
	err = db.Put(utils.GetTestKey(3), utils.RandomValue(24))
	assert.Nil(t, err)
	err = db.Delete(utils.GetTestKey(3))
	assert.Nil(t, err)
	val4, err := db.Get(utils.GetTestKey(3))
	assert.Equal(t, ErrKeyNotFound, err)
	assert.Equal(t, 0, len(val4))

	// 转换为了旧数据文件，从旧的数据文件中获取value
	for i := 100; i < 1000000; i++ {
		err := db.Put(utils.GetTestKey(i), utils.RandomValue(128))
		assert.Nil(t, err)
	}
	assert.Equal(t, 2, len(db.oldFiles))
	val5, err := db.Get(utils.GetTestKey(101))
	assert.Nil(t, err)
	assert.NotNil(t, val5)

	// 重启后，前面写入的数据都能拿到
	err = db.Close()
	assert.Nil(t, err)

	// 重启数据库
	db2, err := Open(opts)
	defer destroyDB(db2)
	val6, err := db2.Get(utils.GetTestKey(1))
	assert.Nil(t, err)
	assert.NotNil(t, val6)
	assert.Equal(t, val1, val6)

	val7, err := db2.Get(utils.GetTestKey(2))
	assert.Nil(t, err)
	assert.NotNil(t, val7)
	assert.Equal(t, val3, val7)

	val8, err := db2.Get(utils.GetTestKey(3))
	assert.Equal(t, 0, len(val8))
	assert.Equal(t, ErrKeyNotFound, err)
}

func TestDB_Delete(t *testing.T) {
	opts := DefaultOptions
	dir, _ := os.MkdirTemp("./TestingFile", "bitcask-go-delete")
	opts.DirPath = dir
	opts.DataFileSize = 64 * 1024 * 1024
	db, err := Open(opts)
	assert.Nil(t, err)
	assert.NotNil(t, db)

	// 正常删除一个存在的key
	err = db.Put(utils.GetTestKey(1), utils.RandomValue(24))
	assert.Nil(t, err)
	err = db.Delete(utils.GetTestKey(1))
	assert.Nil(t, err)
	_, err = db.Get(utils.GetTestKey(1))
	assert.Equal(t, ErrKeyNotFound, err)

	// 删除一个不存在的key
	err = db.Delete([]byte("unknown key"))
	assert.Nil(t, err)

	// 删除一个空的key
	err = db.Delete(nil)
	assert.Equal(t, ErrKeyIsEmpty, err)

	// 值被删除之后重新put
	err = db.Put(utils.GetTestKey(2), utils.RandomValue(24))
	assert.Nil(t, err)
	err = db.Delete(utils.GetTestKey(2))
	assert.Nil(t, err)

	err = db.Put(utils.GetTestKey(2), utils.RandomValue(24))
	assert.Nil(t, err)
	val1, err := db.Get(utils.GetTestKey(2))
	assert.Nil(t, err)
	assert.NotNil(t, val1)

	// 重启后，再进行校验
	err = db.Close()
	assert.Nil(t, err)

	// 重启数据库
	db2, err := Open(opts)
	defer destroyDB(db2)
	_, err = db2.Get(utils.GetTestKey(1))
	assert.Equal(t, ErrKeyNotFound, err)
	val2, err := db2.Get(utils.GetTestKey(2))
	assert.Nil(t, err)
	assert.Equal(t, val1, val2)
}

func TestDB_ListKeys(t *testing.T) {
	opts := DefaultOptions
	dir, _ := os.MkdirTemp("./TestingFile", "bitcask-go-list-keys")
	opts.DirPath = dir
	opts.DataFileSize = 64 * 1024 * 1024
	db, err := Open(opts)
	defer destroyDB(db)
	assert.Nil(t, err)
	assert.NotNil(t, db)

	// 数据库为空
	keys1 := db.ListKeys()
	assert.Equal(t, 0, len(keys1))

	// 只有一条数据
	err = db.Put(utils.GetTestKey(1), utils.RandomValue(24))
	assert.Nil(t, err)
	keys2 := db.ListKeys()
	assert.Equal(t, 1, len(keys2))

	// 有多条数据
	err = db.Put(utils.GetTestKey(2), utils.RandomValue(24))
	assert.Nil(t, err)
	err = db.Put(utils.GetTestKey(3), utils.RandomValue(24))
	assert.Nil(t, err)
	err = db.Put(utils.GetTestKey(4), utils.RandomValue(24))
	assert.Nil(t, err)

	keys3 := db.ListKeys()
	assert.Equal(t, 4, len(keys3))
	for _, k := range keys3 {
		assert.NotNil(t, k)
	}
}

func TestDB_Fold(t *testing.T) {
	opts := DefaultOptions
	dir, _ := os.MkdirTemp("./TestingFile", "bitcask-go-fold")
	opts.DirPath = dir
	opts.DataFileSize = 64 * 1024 * 1024
	db, err := Open(opts)
	defer destroyDB(db)
	assert.Nil(t, err)
	assert.NotNil(t, db)

	err = db.Put(utils.GetTestKey(1), utils.RandomValue(24))
	assert.Nil(t, err)
	err = db.Put(utils.GetTestKey(2), utils.RandomValue(24))
	assert.Nil(t, err)
	err = db.Put(utils.GetTestKey(3), utils.RandomValue(24))
	assert.Nil(t, err)
	err = db.Put(utils.GetTestKey(4), utils.RandomValue(24))
	assert.Nil(t, err)

	err = db.Fold(func(key []byte, value []byte) bool {
		assert.NotNil(t, key)
		assert.NotNil(t, value)
		return true
	})
	assert.Nil(t, err)
}

func TestDB_Close(t *testing.T) {
	opts := DefaultOptions
	dir, _ := os.MkdirTemp("./TestingFile", "bitcask-go-fold")
	opts.DirPath = dir
	opts.DataFileSize = 64 * 1024 * 1024
	db, err := Open(opts)
	defer destroyDB(db)
	assert.Nil(t, err)
	assert.NotNil(t, db)

	err = db.Put(utils.GetTestKey(1), utils.RandomValue(24))
	assert.Nil(t, err)
}

func TestDB_Sync(t *testing.T) {
	opts := DefaultOptions
	dir, _ := os.MkdirTemp("./TestingFile", "bitcask-go-sync")
	opts.DirPath = dir
	opts.DataFileSize = 64 * 1024 * 1024
	db, err := Open(opts)
	defer destroyDB(db)
	assert.Nil(t, err)
	assert.NotNil(t, db)

	err = db.Put(utils.GetTestKey(1), utils.RandomValue(24))
	assert.Nil(t, err)

	err = db.Sync()
	assert.Nil(t, err)
}

func TestDB_FileLock(t *testing.T) {
	opts := DefaultOptions
	dir, _ := os.MkdirTemp("./TestingFile", "bitcask-go-filelock")
	opts.DirPath = dir
	opts.DataFileSize = 64 * 1024 * 1024
	db, err := Open(opts)
	defer destroyDB(db)
	//t.Log(db)
	//t.Log(err)
	assert.Nil(t, err)
	assert.NotNil(t, db)

	_, err = Open(opts)
	//t.Log(err)
	assert.Equal(t, ErrDatabaseIsUsing, err)
}

func TestDB_FileLock2(t *testing.T) {
	opts := DefaultOptions
	dir, _ := os.MkdirTemp("./TestingFile", "bitcask-go-filelock")
	opts.DirPath = dir
	opts.DataFileSize = 64 * 1024 * 1024
	db, err := Open(opts)
	assert.Nil(t, err)
	assert.NotNil(t, db)

	err = db.Close()
	assert.Nil(t, err)

	db2, err := Open(opts)
	defer destroyDB(db2)
	//t.Log(db2)
	//t.Log(err)
	assert.NotNil(t, db2)
	assert.Nil(t, err)
}

func TestDB_OpenMMap(t *testing.T) {
	opts := DefaultOptions
	dir, _ := os.MkdirTemp("./TestingFile/MMap", "bitcask-go-mmap")
	opts.DirPath = dir
	opts.DataFileSize = 64 * 1024 * 1024
	db, err := Open(opts)
	assert.Nil(t, err)
	assert.NotNil(t, db)

	for i := 0; i < 10000; i++ {
		err = db.Put(utils.GetTestKey(i), utils.RandomValue(i+2))
		assert.Nil(t, err)
	}
	db.Close()
	now := time.Now()
	opts.MMapAtStartup = false
	db2, err := Open(opts)
	defer destroyDB(db2)
	t.Log("open time: ", time.Since(now))
	assert.Nil(t, err)
}

func TestDB_Stat(t *testing.T) {
	opts := DefaultOptions
	dir, _ := os.MkdirTemp("./TestingFile", "bitcask-go-stat")
	opts.DirPath = dir
	opts.DataFileSize = 64 * 1024 * 1024
	db, err := Open(opts)
	defer destroyDB(db)
	assert.Nil(t, err)
	assert.NotNil(t, db)

	for i := 100; i < 1000000; i++ {
		err := db.Put(utils.GetTestKey(i), utils.RandomValue(128))
		assert.Nil(t, err)
	}
	for i := 100; i < 1000000; i++ {
		err := db.Delete(utils.GetTestKey(i))
		assert.Nil(t, err)
	}
	for i := 2000; i < 5000; i++ {
		err := db.Put(utils.GetTestKey(i), utils.RandomValue(128))
		assert.Nil(t, err)
	}
	stat := db.Stat()
	//t.Log(stat)
	assert.NotNil(t, stat)
}

func TestDB_Backup(t *testing.T) {
	opts := DefaultOptions
	dir, _ := os.MkdirTemp("./TestingFile", "bitcask-go-origin")
	opts.DirPath = dir
	db, err := Open(opts)
	defer destroyDB(db)
	assert.Nil(t, err)
	assert.NotNil(t, db)

	for i := 0; i < 10000; i++ {
		err = db.Put(utils.GetTestKey(i), utils.RandomValue(128))
		assert.Nil(t, err)
	}

	backupDir, _ := os.MkdirTemp("./TestingFile", "bitcask-go-backup")
	err = db.Backup(backupDir)
	assert.Nil(t, err)
	opts1 := DefaultOptions
	opts1.DirPath = backupDir
	db2, err := Open(opts1)
	defer destroyDB(db2)
	assert.Nil(t, err)
	assert.NotNil(t, db2)
}
