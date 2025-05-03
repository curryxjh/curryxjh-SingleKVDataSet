package index

import (
	"SingleKVDataSet/data"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestBTree_Put(t *testing.T) {
	bt := NewBTree()

	res1 := bt.Put(nil, &data.LogRecordPos{Fid: 1, Offset: 100})
	assert.True(t, res1)

	res2 := bt.Put([]byte("a"), &data.LogRecordPos{Fid: 1, Offset: 2})
	assert.True(t, res2)
}

func TestBTree_Get(t *testing.T) {
	bt := NewBTree()

	res1 := bt.Put(nil, &data.LogRecordPos{Fid: 1, Offset: 100})
	assert.True(t, res1)

	pos1 := bt.Get(nil)
	assert.Equal(t, uint32(1), pos1.Fid)
	assert.Equal(t, int64(100), pos1.Offset)

	res2 := bt.Put([]byte("a"), &data.LogRecordPos{Fid: 1, Offset: 2})
	assert.True(t, res2)

	res3 := bt.Put([]byte("a"), &data.LogRecordPos{Fid: 1, Offset: 3})
	assert.True(t, res3)

	pos2 := bt.Get([]byte("a"))
	assert.Equal(t, uint32(1), pos2.Fid)
	assert.Equal(t, int64(3), pos2.Offset)
}

func TestBTree_Delete(t *testing.T) {
	bt := NewBTree()
	res1 := bt.Put(nil, &data.LogRecordPos{Fid: 1, Offset: 100})
	assert.True(t, res1)

	_, res2 := bt.Delete(nil)
	assert.True(t, res2)

	res3 := bt.Put([]byte("aaa"), &data.LogRecordPos{Fid: 21, Offset: 33})
	assert.True(t, res3)

	_, res4 := bt.Delete([]byte("aaa"))
	assert.True(t, res4)
}

func TestBTree_Iterator(t *testing.T) {
	bt1 := NewBTree()

	// BTree 为空的情况
	iter1 := bt1.Iterator(false)
	//t.Log(iter1.Valid())
	assert.Equal(t, false, iter1.Valid())

	// BTree 有数据的情况
	bt1.Put([]byte("abcede"), &data.LogRecordPos{Fid: 1, Offset: 10})
	iter2 := bt1.Iterator(false)
	assert.Equal(t, true, iter2.Valid())
	//t.Log(iter2.Key())
	//t.Log(iter2.Value())
	assert.NotNil(t, iter2.Key())
	assert.NotNil(t, iter2.Value())
	iter2.Next()
	//t.Log(iter2.Valid())
	assert.Equal(t, false, iter2.Valid())

	// BTree 有多条数据
	bt1.Put([]byte("RRRR"), &data.LogRecordPos{Fid: 1, Offset: 10})
	bt1.Put([]byte("SSSS"), &data.LogRecordPos{Fid: 1, Offset: 10})
	bt1.Put([]byte("tttt"), &data.LogRecordPos{Fid: 1, Offset: 10})
	iter3 := bt1.Iterator(false)
	for iter3.Rewind(); iter3.Valid(); iter3.Next() {
		//t.Log("key = ", string(iter3.Key()))
		assert.NotNil(t, iter3.Key())
	}

	iter4 := bt1.Iterator(true)
	for iter4.Rewind(); iter4.Valid(); iter4.Next() {
		//t.Log("key = ", string(iter4.Key()))
		assert.NotNil(t, iter4.Key())
	}

	// Seek 测试
	iter5 := bt1.Iterator(false)
	for iter5.Seek([]byte("S")); iter5.Valid(); iter5.Next() {
		//t.Log(string(iter5.Key()))
		assert.NotNil(t, iter5.Key())
	}

	// 反向Seek测试
	iter6 := bt1.Iterator(true)
	for iter6.Seek([]byte("zz")); iter6.Valid(); iter6.Next() {
		//t.Log(string(iter6.Key()))
		assert.NotNil(t, iter6.Key())
	}
}
