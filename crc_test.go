package corn

import (
	"testing"
	"time"
)

func TestCRC32(t *testing.T) {
	key := "wang"
	value := "zhen"
	timeStamp := uint32(time.Now().Unix())
	e := &entry{
		crc32:  0,
		tstamp: timeStamp,
		ttl:    0,
		ksz:    uint32(len(key)),
		vsz:    uint32(len(value)),
		key:    []byte(key),
		value:  []byte(value),
	}
	data := Marshal(e)
	crc, err := CRC32(data[4:])
	if err != nil {
		t.Fatal(err)
	}
	t.Log(crc)
}
