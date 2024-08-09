package grain

import "encoding/binary"

type Hint struct {
	Offset    int64  //8
	TimeStamp int64  //8
	KSize     uint32 //4
	Key       []byte
}

func HintEncode(h *Hint) ([]byte, error) {
	length := 20 + h.KSize
	b := make([]byte, length)
	binary.BigEndian.PutUint64(b[:8], uint64(h.Offset))
	binary.BigEndian.PutUint64(b[8:16], uint64(h.TimeStamp))
	binary.BigEndian.PutUint32(b[16:20], h.KSize)
	copy(b[20:20+h.KSize], h.Key)
	return b, nil
}
func HintDecode(h *Hint, b []byte) error {
	h.Offset = int64(binary.BigEndian.Uint64(b[:8]))
	h.TimeStamp = int64(binary.BigEndian.Uint64(b[8:16]))
	h.KSize = binary.BigEndian.Uint32(b[16:20])
	h.Key = make([]byte, h.KSize)
	copy(h.Key, b[20:20+h.KSize])
	return nil
}
