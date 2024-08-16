package tests

import (
	"corn/grain"
	"os"
	"testing"
)

func TestOpen(t *testing.T) {
	corn, err := grain.Open("db")
	if err != nil || corn == nil {
		t.Fatal(err)
	}
	if _, err := os.Stat("db"); os.IsNotExist(err) {
		t.Error("mkdir is incorret")
	}
}
