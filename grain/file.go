package grain

import (
	"os"
)

type DBFile struct {
	File   *os.File
	Offset int64
}

func OpenFile(filename string) (*DBFile, error) {
	f, err := os.OpenFile(filename, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0666)
	if err != nil {
		return nil, err
	}
	return &DBFile{
		File: f,
	}, nil
}

func (f *DBFile) Read(offset int64, buf []byte) (int, error) {
	return f.File.ReadAt(buf, offset)
}

func (f *DBFile) Write(offset int64, buf []byte) (int, error) {
	n, err := f.File.Write(buf)
	if err != nil {
		return 0, err
	}
	return n, nil
}

func (f *DBFile) Sync() error {
	return f.File.Sync()
}

func (f *DBFile) Close() error {
	return f.File.Close()
}
