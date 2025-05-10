package utils

import (
	"github.com/shirou/gopsutil/v4/disk"
	"os"
	"path/filepath"
	"strings"
	"syscall"
)

func DirSize(dirPath string) (int64, error) {
	var size int64
	err := filepath.Walk(dirPath, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if !info.IsDir() {
			size += info.Size()
		}
		return nil
	})
	return size, err
}

func AvailableDiskSize() (uint64, error) {
	wd, err := syscall.Getwd()
	if err != nil {
		return 0, err
	}
	msg, err := disk.Usage(wd)
	if err != nil {
		return 0, err
	}
	// uint64(msg.Free) / 1024 / 1024 / 1024 GB
	return uint64(msg.Free), err
}

// CopyDir 拷贝数据目录
func CopyDir(src, dest string, exclude []string) error {
	if _, err := os.Stat(dest); os.IsNotExist(err) {
		if err := os.MkdirAll(dest, os.ModePerm); err != nil {
			return err
		}
	}
	return filepath.Walk(src, func(path string, info os.FileInfo, err error) error {

		// 假设文件路径为 ./TestingFile/bitcask/0001.data
		// 取出最后的部分0001.data
		src = filepath.Clean(src)
		fileName := strings.Replace(path, src, "", 1)

		if fileName == "" {
			return nil
		}
		for _, e := range exclude {
			matched, err := filepath.Match(e, info.Name())
			if err != nil {
				return err
			}
			if matched {
				return nil
			}
		}
		if info.IsDir() {
			return os.MkdirAll(filepath.Join(dest, fileName), info.Mode())
		}
		data, err := os.ReadFile(filepath.Join(src, fileName))
		if err != nil {
			return err
		}
		return os.WriteFile(filepath.Join(dest, fileName), data, info.Mode())
	})
}
