package index

import (
	"SingleKVDataSet/data"
	"bytes"
	"github.com/google/btree"
)

// 抽象索引接口，后续如果需要接入其他数据结构，仅需要实现该接口即可
type Indexer interface {
	// Put 向索引中存储key对应的数据位置信息
	Put(key []byte, pos *data.LogRecordPos) bool

	// Get 根据key取出对应索引的位置信息
	Get(key []byte) *data.LogRecordPos

	// Delete 根据 key 删除对应的索引位置信息
	Delete(key []byte) (*data.LogRecordPos, bool)

	// Size 索引中的数据量
	Size() int

	// Iterator 索引迭代器
	Iterator(reverse bool) Iterator
}

type IndexType = int8

const (
	// BTree 索引
	Btree IndexType = iota + 1

	// 自适应基数树
	ART
)

// NewIndexer 根据类型，初始化索引
func NewIndexer(typ IndexType) Indexer {
	switch typ {
	case Btree:
		return NewBTree()
	case ART:
		// todo
		return NewART()
	default:
		panic("unsupported index type")
	}
}

type Item struct {
	key []byte
	pos *data.LogRecordPos
}

func (ai *Item) Less(bi btree.Item) bool {
	return bytes.Compare(ai.key, bi.(*Item).key) == -1
}

// Iterator 通用索引迭代器
type Iterator interface {
	// Rewind 重新回到迭代器起点，即第一个数据
	Rewind()
	// Seek 根据传入的key查找第一个大于/小于等于的目标key，根据这个key开始遍历
	Seek(key []byte)
	// Next 跳转到下一个key
	Next()
	// Valid 是否有效，即是否已经遍历完了所有key，用于退出遍历
	Valid() bool
	// Key 当前遍历位置的key数据
	Key() []byte
	// Value 当前遍历位置的value数据
	Value() *data.LogRecordPos
	// Close 关闭迭代器，释放相应资源
	Close()
}
