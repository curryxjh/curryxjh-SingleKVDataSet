package fio

import (
	"github.com/stretchr/testify/assert"
	"os"
	"path/filepath"
	"testing"
)

func destoryFile(path string) {
	err := os.RemoveAll(path)
	if err != nil {
		panic(err)
	}
}

func TestNewFileIOManager(t *testing.T) {
	path := filepath.Join("../TestingFile", "a.data")
	fio, err := NewFileIOManager(path)

	defer destoryFile(path)
	defer fio.Close()

	assert.Nil(t, err)
	assert.NotNil(t, fio)
}

func TestFileIO_Write(t *testing.T) {
	path := filepath.Join("../TestingFile", "a.data")
	fio, err := NewFileIOManager(path)

	defer destoryFile(path)
	defer fio.Close()

	assert.Nil(t, err)
	assert.NotNil(t, fio)

	n, err := fio.Write([]byte(""))
	assert.Equal(t, 0, n)
	assert.Nil(t, err)

	n, err = fio.Write([]byte("bitcask kv"))
	//t.Log(n, err)
	assert.Equal(t, 10, n)
	assert.Nil(t, err)

	n, err = fio.Write([]byte("storage"))
	//t.Log(n, err)
	assert.Equal(t, 7, n)
	assert.Nil(t, err)
}

func TestFileIO_Read(t *testing.T) {
	path := filepath.Join("../TestingFile", "a.data")
	fio, err := NewFileIOManager(path)

	defer destoryFile(path)
	defer fio.Close()

	assert.Nil(t, err)
	assert.NotNil(t, fio)

	_, err = fio.Write([]byte("key-a"))
	assert.Nil(t, err)

	_, err = fio.Write([]byte("key-b"))
	assert.Nil(t, err)

	b1 := make([]byte, 5)
	n, err := fio.Read(b1, 0)
	assert.Equal(t, 5, n)
	//t.Log(string(b), n)
	assert.Equal(t, 5, n)
	assert.Equal(t, []byte("key-a"), b1)
	assert.Nil(t, err)

	b2 := make([]byte, 5)
	n, err = fio.Read(b2, 5)
	//t.Log(string(b2), n)
	assert.Equal(t, 5, n)
	assert.Equal(t, []byte("key-b"), b2)
	assert.Nil(t, err)
}

func TestFileIO_Sync(t *testing.T) {
	path := filepath.Join("../TestingFile", "a.data")
	fio, err := NewFileIOManager(path)

	defer destoryFile(path)
	defer fio.Close()

	assert.Nil(t, err)
	assert.NotNil(t, fio)

	err = fio.Sync()
	assert.Nil(t, err)
}

func TestFileIO_Close(t *testing.T) {
	path := filepath.Join("../TestingFile", "a.data")
	fio, err := NewFileIOManager(path)

	defer destoryFile(path)
	defer fio.Close()

	assert.Nil(t, err)
	assert.NotNil(t, fio)

	err = fio.Close()
	assert.Nil(t, err)
}
