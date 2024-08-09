package grain

import (
	"reflect"
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
	var g1 Grain
	err = Decode(&g1, b)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(g, g1) {
		t.Error("decode not encode data\n")
	}
}
