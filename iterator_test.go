package SingleKVDataSet

import (
	"SingleKVDataSet/utils"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestDB_NewIterator(t *testing.T) {
	opts := DefaultOptions
	opts.DirPath = "./TestingFile"
	db, err := Open(opts)
	defer destroyDB(db)
	assert.Nil(t, err)
	assert.NotNil(t, db)

	iterator := db.NewIterator(DefaultIteratorOptions)
	defer iterator.Close()
	assert.NotNil(t, iterator)
	//t.Log(iterator.Valid())
	assert.Equal(t, false, iterator.Valid())
}

func TestDB_Iterator_One_Value(t *testing.T) {
	opts := DefaultOptions
	opts.DirPath = "./TestingFile"
	db, err := Open(opts)
	defer destroyDB(db)
	assert.Nil(t, err)
	assert.NotNil(t, db)

	err = db.Put(utils.GetTestKey(10), utils.GetTestKey(10))
	assert.Nil(t, err)

	iterator := db.NewIterator(DefaultIteratorOptions)
	defer iterator.Close()
	assert.NotNil(t, iterator)
	//t.Log(iterator.Valid())
	//t.Log(string(iterator.Key()))
	assert.Equal(t, true, iterator.Valid())
	assert.Equal(t, utils.GetTestKey(10), iterator.Key())
	val, err := iterator.Value()
	assert.Nil(t, err)
	assert.Equal(t, utils.GetTestKey(10), val)
}

func TestDB_Iterator_Multi_Values(t *testing.T) {
	opts := DefaultOptions
	opts.DirPath = "./TestingFile"
	db, err := Open(opts)
	defer destroyDB(db)
	assert.Nil(t, err)
	assert.NotNil(t, db)

	err = db.Put([]byte("abcde"), utils.RandomValue(10))
	assert.Nil(t, err)
	err = db.Put([]byte("fghij"), utils.RandomValue(10))
	assert.Nil(t, err)
	err = db.Put([]byte("lmnop"), utils.RandomValue(10))
	assert.Nil(t, err)
	err = db.Put([]byte("qrstu"), utils.RandomValue(10))
	assert.Nil(t, err)

	// 正向迭代
	iter1 := db.NewIterator(DefaultIteratorOptions)
	for iter1.Rewind(); iter1.Valid(); iter1.Next() {
		//t.Log(string(iter1.Key()))
		assert.NotNil(t, iter1.Key())
	}
	iter1.Rewind()
	for iter1.Seek([]byte("c")); iter1.Valid(); iter1.Next() {
		//t.Log(string(iter1.Key()))
		assert.NotNil(t, iter1.Key())
	}
	iter1.Close()

	// 反向迭代
	iterOpts1 := DefaultIteratorOptions
	iterOpts1.Reverse = true
	iter2 := db.NewIterator(iterOpts1)
	for iter2.Rewind(); iter2.Valid(); iter2.Next() {
		//t.Log(string(iter2.Key()))
		assert.NotNil(t, iter2.Key())
	}
	iter2.Rewind()
	for iter2.Seek([]byte("c")); iter2.Valid(); iter2.Next() {
		//t.Log(string(iter2.Key()))
		assert.NotNil(t, iter2.Key())
	}
	iter2.Close()

	// 指定prefix
	iterOpts2 := DefaultIteratorOptions
	iterOpts2.Prefix = []byte("ab")
	iter3 := db.NewIterator(iterOpts2)
	for iter3.Rewind(); iter3.Valid(); iter3.Next() {
		//t.Log(string(iter3.Key()))
		assert.NotNil(t, iter3.Key())
	}
	iter3.Close()
}
