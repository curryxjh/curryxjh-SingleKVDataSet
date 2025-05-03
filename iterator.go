package SingleKVDataSet

import (
	"SingleKVDataSet/index"
	"bytes"
)

type Iterator struct {
	IndexIter index.Iterator // 索引迭代器
	db        *DB
	options   IteratorOptions
}

// NewIterator 初始化迭代器
func (db *DB) NewIterator(opts IteratorOptions) *Iterator {
	indexIter := db.index.Iterator(opts.Reverse)
	return &Iterator{
		db:        db,
		IndexIter: indexIter,
		options:   opts,
	}
}

// Rewind 重新回到迭代器起点，即第一个数据
func (it *Iterator) Rewind() {
	it.IndexIter.Rewind()
	it.skipToNext()
}

// Seek 根据传入的key查找第一个大于/小于等于的目标key，根据这个key开始遍历
func (it *Iterator) Seek(key []byte) {
	it.IndexIter.Seek(key)
	it.skipToNext()
}

// Next 跳转到下一个key
func (it *Iterator) Next() {
	it.IndexIter.Next()
	it.skipToNext()
}

// Valid 是否有效，即是否已经遍历完了所有key，用于退出遍历
func (it *Iterator) Valid() bool {
	return it.IndexIter.Valid()
}

// Key 当前遍历位置的key数据
func (it *Iterator) Key() []byte {
	return it.IndexIter.Key()
}

// Value 当前遍历位置的value数据
func (it *Iterator) Value() ([]byte, error) {
	logRecordPos := it.IndexIter.Value()
	it.db.mu.RLock()
	defer it.db.mu.RUnlock()
	return it.db.getValueByPosition(logRecordPos)
}

// Close 关闭迭代器，释放相应资源
func (it *Iterator) Close() {
	it.IndexIter.Close()
}

func (it *Iterator) skipToNext() {
	prefixLen := len(it.options.Prefix)
	if prefixLen == 0 {
		return
	}
	for ; it.IndexIter.Valid(); it.IndexIter.Next() {
		key := it.IndexIter.Key()
		if prefixLen <= len(key) && bytes.Compare(it.options.Prefix, key[:prefixLen]) == 0 {
			break
		}
	}
}
