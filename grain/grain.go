package grain

import (
	"encoding/binary"
	"errors"
)

type Grain struct {
	Offset    int64  //8
	TimeStamp int64  //8
	KSize     uint32 //4
	VSize     uint32 //4
	Key       []byte
	Val       []byte
}

func Encode(g *Grain) ([]byte, error) {
	length := 24 + g.KSize + g.VSize
	b := make([]byte, length)
	binary.BigEndian.PutUint64(b[:8], uint64(g.Offset))
	binary.BigEndian.PutUint64(b[8:16], uint64(g.TimeStamp))
	binary.BigEndian.PutUint32(b[16:20], g.KSize)
	binary.BigEndian.PutUint32(b[20:24], g.VSize)
	copy(b[24:24+g.KSize], g.Key)
	copy(b[24+g.KSize:], g.Val)
	return b, nil
}
func Decode(g *Grain, b []byte) error {
	g.Offset = int64(binary.BigEndian.Uint64(b[:8]))
	g.TimeStamp = int64(binary.BigEndian.Uint64(b[8:16]))
	g.KSize = binary.BigEndian.Uint32(b[16:20])
	g.VSize = binary.BigEndian.Uint32(b[20:24])
	g.Key = make([]byte, g.KSize)
	g.Val = make([]byte, g.VSize)
	copy(g.Key, b[24:24+g.KSize])
	copy(g.Val, b[24+g.KSize:])
	return nil
}

func DecodeHeader(b []byte) (Grain, error) {
	if len(b) < 24 {
		return Grain{}, errors.New("invalid header")
	}
	var g Grain
	g.Offset = int64(binary.BigEndian.Uint64(b[:8]))
	g.TimeStamp = int64(binary.BigEndian.Uint64(b[8:16]))
	g.KSize = binary.BigEndian.Uint32(b[16:20])
	g.VSize = binary.BigEndian.Uint32(b[20:24])
	return g, nil
}

func (g *Grain) Decode(b []byte) (HeaderInfo, error) {
	var hi HeaderInfo
	if len(b) < 24 {
		return hi, errors.New("invalid header")
	}
	g.Offset = int64(binary.BigEndian.Uint64(b[:8]))
	g.TimeStamp = int64(binary.BigEndian.Uint64(b[8:16]))
	g.KSize = binary.BigEndian.Uint32(b[16:20])
	g.VSize = binary.BigEndian.Uint32(b[20:24])

	hi.Offset = g.Offset
	hi.Total = 24 + g.KSize + g.VSize
	return hi, nil
}
