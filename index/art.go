package index

import (
	"SingleKVDataSet/data"
	"bytes"
	goart "github.com/plar/go-adaptive-radix-tree/v2"
	"sort"
	"sync"
)

// AdaptiveRadixTree 自适应基数树索引
// 主要封装了 https://github.com/plar/go-adaptive-radix-tree/v2
type AdaptiveRadixTree struct {
	tree goart.Tree
	lock *sync.RWMutex
}

func NewART() *AdaptiveRadixTree {
	return &AdaptiveRadixTree{
		tree: goart.New(),
		lock: new(sync.RWMutex),
	}
}

// Put 向索引中存储key对应的数据位置信息
func (art *AdaptiveRadixTree) Put(key []byte, pos *data.LogRecordPos) *data.LogRecordPos {
	art.lock.Lock()
	oldItem, _ := art.tree.Insert(key, pos)
	art.lock.Unlock()
	if oldItem == nil {
		return nil
	}
	return oldItem.(*data.LogRecordPos)
}

// Get 根据key取出对应索引的位置信息
func (art *AdaptiveRadixTree) Get(key []byte) *data.LogRecordPos {
	art.lock.RLock()
	defer art.lock.RUnlock()
	value, found := art.tree.Search(key)
	if !found {
		return nil
	}
	return value.(*data.LogRecordPos)
}

// Delete 根据 key 删除对应的索引位置信息
func (art *AdaptiveRadixTree) Delete(key []byte) (*data.LogRecordPos, bool) {
	art.lock.Lock()
	oldValue, deleted := art.tree.Delete(key)
	art.lock.Unlock()
	if oldValue == nil {
		return nil, false
	}
	return oldValue.(*data.LogRecordPos), deleted
}

// Size 索引中的数据量
func (art *AdaptiveRadixTree) Size() int {
	art.lock.RLock()
	size := art.tree.Size()
	art.lock.RUnlock()
	return size
}

// Iterator 索引迭代器
func (art *AdaptiveRadixTree) Iterator(reverse bool) Iterator {
	art.lock.RLock()
	defer art.lock.RUnlock()
	return newARTIterator(art.tree, reverse)
}

func (art *AdaptiveRadixTree) Close() error {
	return nil
}

// ART 索引迭代器
type artIterator struct {
	currIndex int     // 当前遍历到的位置下标
	reverse   bool    // 是否反向遍历
	values    []*Item // 存放key+位置索引信息
}

// 新建ART索引迭代器的方法
func newARTIterator(tree goart.Tree, reverse bool) *artIterator {
	var idx int

	if reverse {
		idx = tree.Size() - 1
	}
	values := make([]*Item, tree.Size())
	saveValues := func(node goart.Node) bool {
		item := &Item{
			key: node.Key(),
			pos: node.Value().(*data.LogRecordPos),
		}
		values[idx] = item
		if reverse {
			idx--
		} else {
			idx++
		}
		return true
	}

	tree.ForEach(saveValues)

	return &artIterator{
		currIndex: 0,
		reverse:   reverse,
		values:    values,
	}
}

// Rewind 重新回到迭代器起点，即第一个数据
func (ai *artIterator) Rewind() {
	ai.currIndex = 0
}

// Seek 根据传入的key查找第一个大于/小于等于的目标key，根据这个key开始遍历
func (ai *artIterator) Seek(key []byte) {
	if ai.reverse {
		ai.currIndex = sort.Search(len(ai.values), func(i int) bool {
			return bytes.Compare(ai.values[i].key, key) <= 0
		})
	} else {
		ai.currIndex = sort.Search(len(ai.values), func(i int) bool {
			return bytes.Compare(ai.values[i].key, key) >= 0
		})
	}
}

// Next 跳转到下一个key
func (ai *artIterator) Next() {
	ai.currIndex++
}

// Valid 是否有效，即是否已经遍历完了所有key，用于退出遍历
func (ai *artIterator) Valid() bool {
	return ai.currIndex < len(ai.values)
}

// Key 当前遍历位置的key数据
func (ai *artIterator) Key() []byte {
	return ai.values[ai.currIndex].key
}

// Value 当前遍历位置的value数据
func (ai *artIterator) Value() *data.LogRecordPos {
	return ai.values[ai.currIndex].pos
}

// Close 关闭迭代器，释放相应资源
func (ai *artIterator) Close() {
	ai.values = nil
}
