package grain

import (
	"errors"
	"log"
	"os"
	"path/filepath"
	"time"
)

type KeyPath struct {
	FileName string
	Offset   int64
}

type Corn struct {
	Files   map[string]*DBFile
	Offsets map[string]*KeyPath
}

func OpenDB(names ...string) *Corn {
	var corn Corn = Corn{
		Files:   make(map[string]*DBFile),
		Offsets: make(map[string]*KeyPath),
	}
	for i := range names {
		f, err := OpenFile(names[i])
		if err != nil {
			log.Fatalf("open db err:%s", err)
		}
		fi, err := os.Stat(names[i])
		if err != nil {
			log.Fatalln(err)
		}
		f.Offset = fi.Size()
		name := filepath.Base(names[i])
		corn.Files[name] = f
	}

	// init offests when open db
	// ...
	return &corn
}

func (c *Corn) Get(key string) (string, error) {
	var (
		kp *KeyPath
		ok bool
		g  Grain
	)
	kp, ok = c.Offsets[key]
	if !ok {
		return "", nil
	}
	f := c.Files[kp.FileName]
	offset := kp.Offset
	buf := make([]byte, 24)
	n, err := f.Read(offset, buf)
	if err != nil {
		log.Printf("%d,%s\n", n, err)
		return "", err
	}
	gh, err := DecodeHeader(buf)
	if err != nil {
		return "", err
	}
	b := make([]byte, 24+gh.KSize+gh.VSize)
	_, err = f.Read(offset, b)
	if err != nil {
		return "", err
	}
	err = Decode(&g, b)
	if err != nil {
		return "", err
	}
	return string(g.Val), nil
}

func (c *Corn) Put(filename, key, val string) error {
	var f *DBFile = c.Files[filename]
	if f == nil {
		return errors.New("can't find this filename")
	}
	var offset int64 = f.Offset
	var g Grain = Grain{
		Offset:    offset,
		TimeStamp: time.Now().Unix(),
		KSize:     uint32(len(key)),
		VSize:     uint32(len(val)),
		Key:       []byte(key),
		Val:       []byte(val),
	}
	b, err := Encode(&g)
	if err != nil {
		return err
	}
	n, err := f.Write(offset, b)
	if err != nil {
		return err
	}
	c.Offsets[key] = &KeyPath{
		FileName: filename,
		Offset:   f.Offset,
	}
	f.Offset += int64(n)
	return nil
}
