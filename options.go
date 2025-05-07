package SingleKVDataSet

// 用户数据库的一些配置项
type Options struct {
	// 数据库数据目录
	DirPath string

	// 数据文件大小
	DataFileSize int64

	// 每次写入之后是否需要执行一次安全的持久化
	SyncWrites bool

	// 累计写到多少字节后进行持久化
	BytesPerSync uint

	// 索引类型
	IndexType IndexerType

	// 启动时是否使用MMap加载数据
	MMapAtStartup bool
}

// 索引迭代器配置项
type IteratorOptions struct {
	// 遍历前缀为指定值的 key，默认为空
	Prefix []byte
	// 是否反向遍历，默认false是正向
	Reverse bool
}

type WriteBatchOptions struct {
	// 一个批次中最大的数据量
	MaxBatchNum uint
	// 提交事务时，是否进行sync持久化
	SyncWrites bool
}

type IndexerType = int8

const (
	// BTree 索引
	BTree IndexerType = iota + 1

	// ART 自适应基数树
	ART

	// BPlusTree B+ 树索引，将索引存储到磁盘上
	BPlusTree
)

var DefaultOptions = Options{
	DirPath:       "./TestingFile",
	DataFileSize:  256 * 1024 * 1024, // 256MB
	SyncWrites:    false,
	BytesPerSync:  0,
	IndexType:     BTree,
	MMapAtStartup: true,
}

var DefaultIteratorOptions = IteratorOptions{
	Prefix:  nil,
	Reverse: false,
}

var DefaultWriteBatchOptions = WriteBatchOptions{
	MaxBatchNum: 10000,
	SyncWrites:  true,
}
