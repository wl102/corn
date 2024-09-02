package corn

import "encoding/binary"

// entry header size
const hlen uint32 = 20

type entry struct {
	crc32  uint32
	tstamp uint32
	ttl    uint32
	ksz    uint32
	vsz    uint32
	key    []byte
	value  []byte
}

func Marshal(e *entry) []byte {
	esz := hlen + e.ksz + e.vsz
	b := make([]byte, esz)
	binary.BigEndian.PutUint32(b[:4], e.crc32)
	binary.BigEndian.PutUint32(b[4:8], e.tstamp)
	binary.BigEndian.PutUint32(b[8:12], e.ttl)
	binary.BigEndian.PutUint32(b[12:16], e.ksz)
	binary.BigEndian.PutUint32(b[16:20], e.vsz)
	copy(b[hlen:hlen+e.ksz], e.key)
	copy(b[hlen+e.ksz:esz], e.value)
	return b
}

func PutCRC(data []byte, crc uint32) {
	binary.BigEndian.PutUint32(data[:4], crc)
}

func UnMarshal(b []byte, e *entry) error {

	return nil
}
