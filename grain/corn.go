package grain

import (
	"errors"
	"io"
	"io/fs"
	"log"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/bwmarrin/snowflake"
)

type KeyPath struct {
	FileName string
	Offset   int64
}

type Corn struct {
	ActiveFile          *DBFile
	Files               map[string]*DBFile  //filename-fd
	Index               map[string]*KeyPath //key-offset
	bytesPool, readPool sync.Pool
}

type HeaderInfo struct {
	Offset int64
	Total  uint32
}
type HeaderDecoder interface {
	Decode(b []byte) (HeaderInfo, error)
}

func Open(dir string) (*Corn, error) {
	var (
		corn = Corn{
			ActiveFile: nil,
			Files:      make(map[string]*DBFile),
			Index:      make(map[string]*KeyPath),
			bytesPool:  sync.Pool{New: newHeaderBuf},
			readPool:   sync.Pool{New: newReadBuf},
		}
	)

	// 检查目录是否存在
	if _, err := os.Stat(dir); os.IsNotExist(err) {
		// 如果目录不存在，创建目录
		if err := os.MkdirAll(dir, 0755); err != nil {
			return nil, err
		}

		// 生成唯一的文件名
		node, err := snowflake.NewNode(1)
		if err != nil {
			return nil, err
		}
		filePath := filepath.Join(dir, node.Generate().String()+".data")

		// 创建文件
		f, err := os.OpenFile(filePath, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0644)
		if err != nil {
			return nil, err
		}
		corn.ActiveFile = &DBFile{File: f}
	} else {
		// 如果目录存在，进行 Fold 操作
		if err := corn.Fold(dir); err != nil {
			return nil, err
		}
	}
	return &corn, nil
}

func OpenDB(names ...string) *Corn {
	var corn Corn = Corn{
		Files: make(map[string]*DBFile),
		Index: make(map[string]*KeyPath),
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
	err := corn.Fold("./")
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
	kp, ok = c.Index[key]
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
	c.Index[key] = &KeyPath{
		FileName: filename,
		Offset:   f.Offset,
	}
	f.Offset += int64(n)
	return nil
}

func (c *Corn) Delete(key string) error {
	kp, ok := c.Index[key]
	if !ok {
		return errors.New("key is not exsist")
	}
	err := c.Put(kp.FileName, key, "")
	delete(c.Index, key)
	return err
}

func (c *Corn) Fold(dir string) error {
	err := filepath.WalkDir(dir, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err // 如果在访问目录时出错，直接返回错误
		}
		if d.IsDir() {
			return nil // 如果是目录，跳过
		}

		if filepath.Ext(path) != ".data" {
			return nil // 如果不是.data文件，跳过
		}

		filename := filepath.Base(path)
		name := strings.TrimSuffix(filename, filepath.Ext(filename))

		// 检查对应的.hint文件是否存在
		hintPath := filepath.Join(filepath.Dir(path), name+".hint")
		if _, err := os.Stat(hintPath); os.IsNotExist(err) {
			return c.foldDataFile(path, filename)
		}
		return c.foldHintFile(hintPath, filename)
	})

	return err
}

func (c *Corn) foldDataFile(path, filename string) error {
	var (
		off    int64
		g      Grain
		b      = make([]byte, 512)
		dbfile DBFile
	)
	file, err := os.Open(path)
	if err != nil {
		return err
	}
	dbfile.File = file
	c.Files[filename] = &dbfile

	// 开始迭代键-值
	n, err := readRecord(&dbfile, off, b)
	if err != nil {
		return nil
	}
	Decode(&g, b[:n])
	if g.VSize > 0 {
		c.Index[string(g.Key)] = &KeyPath{
			FileName: filename,
			Offset:   off,
		}
	}
	off += int64(24 + g.KSize + g.VSize)

	for n > 0 {
		n, err = readRecord(&dbfile, off, b)
		if err != nil {
			return nil
		}
		Decode(&g, b)
		if g.VSize > 0 {
			c.Index[string(g.Key)] = &KeyPath{
				FileName: filename,
				Offset:   off,
			}
		}
		off += int64(24 + g.KSize + g.VSize)
	}
	return nil
}

func (c *Corn) foldHintFile(path, filename string) error {
	var (
		off    int64
		g      Hint
		b      = make([]byte, 512)
		dbfile DBFile
	)
	file, err := os.Open(path)
	if err != nil {
		return err
	}
	dbfile.File = file
	c.Files[filename] = &dbfile
	n, err := readHint(&dbfile, off, b)
	if err != nil {
		return err
	}
	HintDecode(&g, b[:n])
	c.Index[string(g.Key)] = &KeyPath{
		FileName: filename,
		Offset:   off,
	}

	off += int64(20 + g.KSize)
	for n > 0 {
		n, err = readHint(&dbfile, off, b)
		if err != nil {
			return err
		}
		HintDecode(&g, b)
		c.Index[string(g.Key)] = &KeyPath{
			FileName: filename,
			Offset:   off,
		}
		off += int64(20 + g.KSize)
	}
	return nil
}

func readRecord(f *DBFile, off int64, b []byte) (n int, err error) {
	n, err = f.Read(off, b)
	if err != nil && err != io.EOF {
		return
	}
	gh, err := DecodeHeader(b[:n])
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

func readHint(f *DBFile, off int64, b []byte) (n int, err error) {
	n, err = f.Read(off, b)
	if err != nil && err != io.EOF {
		return
	}
	gh, err := HintHeader(b[:n])
	if err != nil {
		return
	}
	if int(20+gh.KSize) <= len(b) {
		return
	} else {
		err = errors.New("no enough space read hole record")
	}
	return
}

func (c *Corn) List() (keys []string) {
	for k := range c.Index {
		keys = append(keys, k)
	}
	return
}

func (c *Corn) Merge(dir string) error {
	var (
		g Grain
		h Hint
		b []byte = make([]byte, 512)
	)
	err := filepath.WalkDir(dir, func(path string, d fs.DirEntry, err error) error {
		if !d.IsDir() && filepath.Ext(path) == ".db" {
			src, err := os.Open(path)
			if err != nil {
				return err
			}
			defer src.Close()
			// merge operation
			// ...
			dir, name := filepath.Split(path)
			name = strings.TrimRight(name, ".db ")
			dst, err := os.OpenFile(dir+name+".archive", os.O_RDWR|os.O_CREATE|os.O_TRUNC|os.O_APPEND, 0666)
			if err != nil {
				return err
			}
			hint, err := os.OpenFile(dir+name+".hint", os.O_RDWR|os.O_CREATE|os.O_TRUNC|os.O_APPEND, 0666)
			if err != nil {
				return err
			}
			defer hint.Close()
			defer dst.Close()
			//
			var off, doff int64 = 0, 0
			v := &DBFile{File: src}
			n, err := readRecord(v, off, b)
			if err != nil && err != io.EOF {
				return nil
			}
			Decode(&g, b)
			h.Offset = doff
			h.TimeStamp = g.TimeStamp
			h.KSize = g.KSize
			h.Key = g.Key
			hbytes, err := HintEncode(&h)
			if err != nil {
				return err
			}
			if kp, ok := c.Index[string(g.Key)]; ok && kp.Offset == g.Offset {
				dst.Write(b[:n])
				hint.Write(hbytes)
			}
			off += int64(24 + g.KSize + g.VSize)
			doff += int64(len(hbytes))

			for n > 0 {
				n, err = readRecord(v, off, b)
				if err != nil && err != io.EOF {
					return nil
				}
				Decode(&g, b)
				h.Offset = doff
				h.TimeStamp = g.TimeStamp
				h.KSize = g.KSize
				h.Key = g.Key
				hbytes, err := HintEncode(&h)
				if err != nil {
					return err
				}
				if kp, ok := c.Index[string(g.Key)]; ok && kp.Offset == g.Offset {
					dst.Write(b[:n])
					hint.Write(hbytes)
				}
				off += int64(24 + g.KSize + g.VSize)
				doff += int64(len(hbytes))
			}
		}
		return nil
	})
	return err
}

func (c *Corn) Sync() error {
	return c.ActiveFile.Sync()
}

func (c *Corn) Close() error {
	for _, f := range c.Files {
		if err := f.Close(); err != nil {
			return err
		}
	}
	return nil
}
