package utils

import (
	"github.com/shirou/gopsutil/v4/disk"
	"os"
	"path/filepath"
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
