package index

import (
	"SingleKVDataSet/data"
	"github.com/stretchr/testify/assert"
	"os"
	"path/filepath"
	"testing"
)

func TestBPlusTree_Put(t *testing.T) {
	path := filepath.Join("../TestingFile", "bptree-put")
	_ = os.Mkdir(path, os.ModePerm)
	defer func() {
		_ = os.RemoveAll(path)
	}()
	tree := NewBPlusTree(path, false)
	defer tree.Close()
	res1 := tree.Put([]byte("abc"), &data.LogRecordPos{Fid: 123, Offset: 987})
	//t.Log(res1)
	assert.True(t, res1)
	res2 := tree.Put([]byte("drf"), &data.LogRecordPos{Fid: 123, Offset: 999})
	//t.Log(res2)
	assert.True(t, res2)
	res3 := tree.Put([]byte("hij"), &data.LogRecordPos{Fid: 123, Offset: 567})
	//t.Log(res3)
	assert.True(t, res3)
}

func TestBPlusTree_Get(t *testing.T) {
	path := filepath.Join("../TestingFile", "bptree-get")
	_ = os.Mkdir(path, os.ModePerm)
	defer func() {
		_ = os.RemoveAll(path)
	}()

	tree := NewBPlusTree(path, false)
	defer tree.Close()

	pos := tree.Get([]byte("not exists"))
	//t.Log(pos)
	assert.Nil(t, pos)

	tree.Put([]byte("abc"), &data.LogRecordPos{Fid: 123, Offset: 987})
	pos1 := tree.Get([]byte("abc"))
	//t.Log(pos1)
	assert.NotNil(t, pos1)

	tree.Put([]byte("drf"), &data.LogRecordPos{Fid: 12, Offset: 999})
	pos2 := tree.Get([]byte("drf"))
	//t.Log(pos2)
	assert.NotNil(t, pos2)
}

func TestBPlusTree_Delete(t *testing.T) {
	path := filepath.Join("../TestingFile", "bptree-delete")
	_ = os.Mkdir(path, os.ModePerm)
	defer func() {
		_ = os.RemoveAll(path)
	}()

	tree := NewBPlusTree(path, false)
	defer tree.Close()

	pos, ok := tree.Delete([]byte("not exists"))
	//t.Log(pos)
	//t.Log(ok)
	assert.Nil(t, pos)
	assert.False(t, ok)

	tree.Put([]byte("abc"), &data.LogRecordPos{Fid: 123, Offset: 987})
	pos1, ok1 := tree.Delete([]byte("abc"))
	//t.Log(pos1)
	//t.Log(ok1)
	assert.True(t, ok1)
	assert.NotNil(t, pos1)

	pos2 := tree.Get([]byte("abc"))
	//t.Log(pos2)
	assert.Nil(t, pos2)
}

func TestBPlusTree_Size(t *testing.T) {
	path := filepath.Join("../TestingFile", "bptree-size")
	_ = os.Mkdir(path, os.ModePerm)
	defer func() {
		_ = os.RemoveAll(path)
	}()

	tree := NewBPlusTree(path, false)
	defer tree.Close()

	//t.Log(tree.Size())
	assert.Equal(t, 0, tree.Size())
	tree.Put([]byte("abc"), &data.LogRecordPos{Fid: 123, Offset: 789})
	tree.Put([]byte("adc"), &data.LogRecordPos{Fid: 123, Offset: 789})
	tree.Put([]byte("akc"), &data.LogRecordPos{Fid: 123, Offset: 789})
	tree.Put([]byte("kfc"), &data.LogRecordPos{Fid: 123, Offset: 789})
	//t.Log(tree.Size())
	assert.Equal(t, 4, tree.Size())
}

func TestBPlusTree_Iterator(t *testing.T) {
	path := filepath.Join("../TestingFile", "bptree-iter")
	_ = os.Mkdir(path, os.ModePerm)
	defer func() {
		_ = os.RemoveAll(path)
	}()

	tree := NewBPlusTree(path, false)
	defer tree.Close()

	tree.Put([]byte("abc"), &data.LogRecordPos{Fid: 123, Offset: 789})
	tree.Put([]byte("aac"), &data.LogRecordPos{Fid: 123, Offset: 789})
	tree.Put([]byte("dfc"), &data.LogRecordPos{Fid: 123, Offset: 789})
	tree.Put([]byte("kbc"), &data.LogRecordPos{Fid: 123, Offset: 789})
	tree.Put([]byte("aaa"), &data.LogRecordPos{Fid: 123, Offset: 789})
	tree.Put([]byte("zah"), &data.LogRecordPos{Fid: 123, Offset: 789})

	iter := tree.Iterator(true)
	defer iter.Close()
	for iter.Rewind(); iter.Valid(); iter.Next() {
		//t.Log(string(iter.Key()), iter.Value())
		assert.NotNil(t, iter.Key())
		assert.NotNil(t, iter.Value())
	}
}
