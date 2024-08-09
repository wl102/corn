package grain

import (
	"errors"
	"io"
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
	Files   map[string]*DBFile  //filename-fd
	Offsets map[string]*KeyPath //key-offset
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
	err := corn.Fold()
	if err != nil {
		log.Fatalf("when init mem index failed :%d\n", err)
	}
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

func (c *Corn) Delete(key string) error {
	kp, ok := c.Offsets[key]
	if !ok {
		return errors.New("key is not exsist")
	}
	err := c.Put(kp.FileName, key, "")
	delete(c.Offsets, key)
	return err
}

func (c *Corn) Fold() error {
	var (
		off int64
	)
	g := Grain{}
	b := make([]byte, 512)
	for k, v := range c.Files {
		n, err := readRecord(v, off, b)
		if err != nil {
			return nil
		}
		Decode(&g, b)
		if g.VSize > 0 {
			c.Offsets[string(g.Key)] = &KeyPath{
				FileName: k,
				Offset:   off,
			}
		}
		off += int64(24 + g.KSize + g.VSize)

		for err != io.EOF && n > 0 {
			n, err = readRecord(v, off, b)
			if err != nil {
				return nil
			}
			Decode(&g, b)
			if g.VSize > 0 {
				c.Offsets[string(g.Key)] = &KeyPath{
					FileName: k,
					Offset:   off,
				}
			}
			off += int64(24 + g.KSize + g.VSize)
		}
	}
	return nil
}

func readRecord(f *DBFile, off int64, b []byte) (n int, err error) {
	n, err = f.Read(off, b)
	if err != nil {
		log.Printf("%d,%s\n", n, err)
		return
	}
	gh, err := DecodeHeader(b)
	if err != nil {
		return
	}
	if int(24+gh.KSize+gh.VSize) <= len(b) {
		return
	} else {
		err = errors.New("no enough space read hole record")
	}
	return
}

func (c *Corn) List() (keys []string) {
	for k := range c.Offsets {
		keys = append(keys, k)
	}
	return
}
