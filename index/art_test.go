package index

import (
	"SingleKVDataSet/data"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestAdaptiveRadixTree_Put(t *testing.T) {
	art := NewART()
	res1 := art.Put([]byte("key-1"), &data.LogRecordPos{Fid: 1, Offset: 12})
	assert.Nil(t, res1)
	res2 := art.Put([]byte("key-2"), &data.LogRecordPos{Fid: 1, Offset: 12})
	assert.Nil(t, res2)
	res3 := art.Put([]byte("key-3"), &data.LogRecordPos{Fid: 1, Offset: 12})
	assert.Nil(t, res3)
	res4 := art.Put([]byte("key-3"), &data.LogRecordPos{Fid: 1, Offset: 15})
	//t.Log(res4)
	assert.Equal(t, res4.Fid, uint32(1))
	assert.Equal(t, res4.Offset, int64(12))
}

func TestAdaptiveRadixTree_Get(t *testing.T) {
	art := NewART()
	art.Put([]byte("key-1"), &data.LogRecordPos{Fid: 1, Offset: 12})
	pos := art.Get([]byte("key-1"))
	//t.Log(pos)
	assert.NotNil(t, pos)

	pos1 := art.Get([]byte("not exist"))
	//t.Log(pos1)
	assert.Nil(t, pos1)

	art.Put([]byte("key-1"), &data.LogRecordPos{Fid: 123, Offset: 456})
	pos2 := art.Get([]byte("key-1"))
	//t.Log(pos2)
	assert.NotNil(t, pos2)
}

func TestAdaptiveRadixTree_Delete(t *testing.T) {
	art := NewART()
	res1, ok1 := art.Delete([]byte("not exist"))
	//t.Log(res1)
	//t.Log(ok1)
	assert.Nil(t, res1)
	assert.False(t, ok1)

	art.Put([]byte("key-1"), &data.LogRecordPos{Fid: 1, Offset: 12})
	res2, ok2 := art.Delete([]byte("key-1"))
	//t.Log(res2)
	//t.Log(ok2)
	assert.NotNil(t, res2)
	assert.True(t, ok2)
	pos := art.Get([]byte("key-1"))
	//t.Log(pos)
	assert.Nil(t, pos)
}

func TestAdaptiveRadixTree_Size(t *testing.T) {
	art := NewART()
	//t.Log(art.Size())
	assert.Equal(t, art.Size(), 0)
	art.Put([]byte("key-1"), &data.LogRecordPos{Fid: 1, Offset: 12})
	art.Put([]byte("key-2"), &data.LogRecordPos{Fid: 1, Offset: 12})
	//t.Log(art.Size())
	assert.Equal(t, art.Size(), 2)
	art.Put([]byte("key-1"), &data.LogRecordPos{Fid: 2, Offset: 12})
	//t.Log(art.Size())
	assert.Equal(t, art.Size(), 2)
}

func TestAdaptiveRadixTree_Iterator(t *testing.T) {
	art := NewART()

	art.Put([]byte("a"), &data.LogRecordPos{Fid: 1, Offset: 12})
	art.Put([]byte("fgd"), &data.LogRecordPos{Fid: 1, Offset: 12})
	art.Put([]byte("caca"), &data.LogRecordPos{Fid: 1, Offset: 12})
	art.Put([]byte("e"), &data.LogRecordPos{Fid: 1, Offset: 12})

	//t.Log("正向")
	iter := art.Iterator(false)

	for iter.Rewind(); iter.Valid(); iter.Next() {
		//t.Log(string(iter.Key()))
		assert.NotNil(t, iter.Key())
	}

	//t.Log("反向")
	iter1 := art.Iterator(true)

	for iter1.Rewind(); iter1.Valid(); iter1.Next() {
		//t.Log(string(iter1.Key()))
		assert.NotNil(t, iter1.Key())
	}
}
