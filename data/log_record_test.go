package data

import (
	"github.com/stretchr/testify/assert"
	"hash/crc32"
	"testing"
)

func TestEncodeLogRecord(t *testing.T) {
	// 正常编码一条数据
	rec1 := &LogRecord{
		Key:   []byte("name"),
		Value: []byte("bitcask_go"),
		Type:  LogRecordNormal,
	}
	res1, n1 := EncodeLogRecord(rec1)
	t.Log(res1)
	assert.NotNil(t, res1)
	assert.Greater(t, n1, int64(5))

	// LogRecord中value为空
	rec2 := &LogRecord{
		Key:  []byte("name"),
		Type: LogRecordNormal,
	}
	res2, n2 := EncodeLogRecord(rec2)
	t.Log(res2)
	assert.NotNil(t, res2)
	assert.Greater(t, n2, int64(5))
	// LogRecord的type为deleted

	rec3 := &LogRecord{
		Key:   []byte("name"),
		Value: []byte("bitcask_go"),
		Type:  LogRecordDeleted,
	}
	res3, n3 := EncodeLogRecord(rec3)
	t.Log(res3)
	assert.NotNil(t, res3)
	assert.Greater(t, n3, int64(5))
}

func TestDecodeLogRecordHeader(t *testing.T) {
	headerBuf1 := []byte{86, 238, 133, 193, 0, 8, 20}
	h1, size1 := DecodeLogRecordHeader(headerBuf1)
	//t.Log(h1)
	//t.Log(size1)
	assert.NotNil(t, h1)
	assert.Equal(t, int64(7), size1)
	assert.Equal(t, uint32(3246779990), h1.crc)
	assert.Equal(t, LogRecordNormal, h1.recordType)
	assert.Equal(t, uint32(4), h1.keySize)
	assert.Equal(t, uint32(10), h1.valueSize)

	headerBuf2 := []byte{9, 252, 88, 14, 0, 8, 0}
	h2, size2 := DecodeLogRecordHeader(headerBuf2)
	//t.Log(h2)
	//t.Log(size2)
	assert.NotNil(t, h2)
	assert.Equal(t, int64(7), size2)
	assert.Equal(t, uint32(240712713), h2.crc)
	assert.Equal(t, LogRecordNormal, h2.recordType)
	assert.Equal(t, uint32(4), h2.keySize)
	assert.Equal(t, uint32(0), h2.valueSize)

	headerBuf3 := []byte{21, 37, 35, 70, 1, 8, 20}
	h3, size3 := DecodeLogRecordHeader(headerBuf3)
	//t.Log(h3)
	//t.Log(size3)
	assert.NotNil(t, h3)
	assert.Equal(t, int64(7), size3)
	assert.Equal(t, uint32(1176708373), h3.crc)
	assert.Equal(t, LogRecordDeleted, h3.recordType)
	assert.Equal(t, uint32(4), h3.keySize)
	assert.Equal(t, uint32(10), h3.valueSize)
}

func TestGetLogRecordCRC(t *testing.T) {
	rec1 := &LogRecord{
		Key:   []byte("name"),
		Value: []byte("bitcask_go"),
		Type:  LogRecordNormal,
	}

	headerBuf1 := []byte{86, 238, 133, 193, 0, 8, 20}
	crc1 := getLogRecordCRC(rec1, headerBuf1[crc32.Size:])
	//t.Log(crc1)
	assert.Equal(t, uint32(3246779990), crc1)

	rec2 := &LogRecord{
		Key:  []byte("name"),
		Type: LogRecordNormal,
	}

	headerBuf2 := []byte{9, 252, 88, 14, 0, 8, 0}
	crc2 := getLogRecordCRC(rec2, headerBuf2[crc32.Size:])
	//t.Log(crc2)
	assert.Equal(t, uint32(240712713), crc2)

	rec3 := &LogRecord{
		Key:   []byte("name"),
		Value: []byte("bitcask_go"),
		Type:  LogRecordDeleted,
	}

	headerBuf3 := []byte{21, 37, 35, 70, 1, 8, 20}
	crc3 := getLogRecordCRC(rec3, headerBuf3[crc32.Size:])
	//t.Log(crc3)
	assert.Equal(t, uint32(1176708373), crc3)
}
