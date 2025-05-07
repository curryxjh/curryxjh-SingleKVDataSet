package fio

import (
	"github.com/stretchr/testify/assert"
	"io"
	"path/filepath"
	"testing"
)

func TestMMap_Read(t *testing.T) {
	path := filepath.Join("../TestingFile", "mmap-a.data")
	defer destoryFile(path)

	mmapIO, err := NewMMapIOManager(path)
	assert.Nil(t, err)

	// 文件夹为空
	b1 := make([]byte, 10)
	n1, err := mmapIO.Read(b1, 0)
	//t.Log(err)
	//t.Log(n1)
	assert.Equal(t, 0, n1)
	assert.Equal(t, io.EOF, err)

	// 有文件的情况
	fio, err := NewFileIOManager(path)
	defer fio.Close()
	assert.Nil(t, err)
	_, err = fio.Write([]byte("hello"))
	assert.Nil(t, err)
	_, err = fio.Write([]byte("kv"))
	assert.Nil(t, err)
	_, err = fio.Write([]byte("goodbye"))
	assert.Nil(t, err)

	mmapIO2, err := NewMMapIOManager(path)
	assert.Nil(t, err)
	size, err := mmapIO2.Size()
	assert.Nil(t, err)
	//t.Log(size)
	assert.Equal(t, int64(14), size)

	b2 := make([]byte, 2)
	n2, err := mmapIO2.Read(b2, 0)
	assert.Nil(t, err)
	assert.Equal(t, 2, n2)
}
