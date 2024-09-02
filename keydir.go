package corn

import "os"

type keyType = string

type keydir struct {
	file     *os.File
	vsz      int
	valuePos int64
	tstamp   uint32
}

func (kd *keydir) GetValue() (value []byte, err error) {
	value = make([]byte, kd.vsz)
	_, err = kd.file.ReadAt(value, int64(kd.valuePos))
	return
}
