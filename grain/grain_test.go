package grain

import (
	"fmt"
	"testing"
	"time"
)

func TestEncodeAndDecode(t *testing.T) {
	var g Grain = Grain{
		Offset:    0,
		TimeStamp: time.Now().Unix(),
		KSize:     4,
		VSize:     4,
		Key:       []byte("wang"),
		Val:       []byte("zhen"),
	}
	b, err := Encode(&g)
	if err != nil {
		t.Fatal(err)
	}
	fmt.Println(b)
	var g1 Grain
	err = Decode(&g1, b)
	if err != nil {
		t.Fatal(err)
	}
	fmt.Printf("Offset:%d\nTimeStamp:%s\nKSize:%d\nVSize:%d\nKey:%s\nVal:%s\n",
		g1.Offset, time.Unix(g1.TimeStamp, 0), g1.KSize, g1.VSize, g1.Key, g1.Val)
}
