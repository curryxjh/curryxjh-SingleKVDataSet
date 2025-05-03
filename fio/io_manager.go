package fio

const (
	DataFilePerm = 0644
)

// 抽象IO管理器接口，可以接入不同的IO类型，目前支持标准文件IO
type IOManager interface {
	// Read 从文件的给定位置读取对应的数据
	Read([]byte, int64) (int, error)

	// Write 写入字节数组到文件中
	Write([]byte) (int, error)

	// Sync 持久化数据
	Sync() error

	// Close 关闭文件
	Close() error

	// Size 获取到文件大小
	Size() (int64, error)
}

// 初始化 IOManager，目前仅支持标准IO
func NewIOManager(filename string) (IOManager, error) {
	return NewFileIOManager(filename)
}
