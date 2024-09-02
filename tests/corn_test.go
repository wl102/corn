package tests

import (
	"corn"
	"testing"
)

func TestOpen(t *testing.T) {
	corn, err := corn.Open("./bitcask")
	if err != nil || corn == nil {
		t.Fatal(err)
	}
}
