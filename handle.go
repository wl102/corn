package corn

import (
	"os"
	"path/filepath"
	"time"

	"github.com/bwmarrin/snowflake"
)

type Corn struct {
	NewFile *os.File
	OldFile []*os.File
	Index   map[keyType]*keydir
}

func OpenWithOpts(dirpath string, opts ...string) (*Corn, error) {
	err := os.MkdirAll(dirpath, 0666)
	if err != nil {
		return nil, err
	}
	// 生成唯一的文件名
	node, err := snowflake.NewNode(1)
	if err != nil {
		return nil, err
	}
	filePath := filepath.Join(dirpath, node.Generate().String()+".data")

	// 创建文件
	f, err := os.OpenFile(filePath, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0666)
	if err != nil {
		return nil, err
	}

	// todo
	// 加载数据库文件

	return &Corn{
		NewFile: f,
		Index:   make(map[string]*keydir),
	}, nil
}

func Open(dirpath string) (*Corn, error) {
	return nil, nil
}

// 获取对应key的值。如果key不存在，返回 "", false
func (c *Corn) Get(key string) (value string, ok bool) {
	if kd, exsist := c.Index[key]; exsist {
		val, err := kd.GetValue()
		if err != nil {
			return
		}
		value = string(val)
		ok = true
	}
	return
}

// 存储 key and value
func (c *Corn) Put(key, value string, ttl uint32) error {
	fs, err := c.NewFile.Stat()
	if err != nil {
		return err
	}
	timeStamp := uint32(time.Now().Unix())
	e := &entry{
		crc32:  0,
		tstamp: timeStamp,
		ttl:    ttl,
		ksz:    uint32(len(key)),
		vsz:    uint32(len(value)),
		key:    []byte(key),
		value:  []byte(value),
	}
	entry := Marshal(e)
	_, err = c.NewFile.Write(entry)
	if err != nil {
		return err
	}
	// 更新内存索引
	c.Index[key] = &keydir{
		file:     c.NewFile,
		vsz:      len(value),
		valuePos: fs.Size() + int64(hlen) + int64(len(key)),
		tstamp:   timeStamp,
	}
	return nil
}

// 删除一个key
func (c *Corn) Delete(key string) error {
	if _, ok := c.Index[key]; ok {
		err := c.Put(key, "", 0)
		delete(c.Index, key)
		return err
	}
	return nil
}

// 列出bitcask数据库中的所有键
func (c *Corn) ListKeys() []string {
	i := 0
	keys := make([]string, len(c.Index))
	for k := range c.Index {
		keys[i] = k
		i++
	}
	return keys
}

func (c *Corn) Fold() {}

func (c *Corn) Merge() {}

// 强制所有写数据同步到磁盘
func (c *Corn) Sync() error {
	return c.NewFile.Sync()
}

func (c *Corn) Close() error {
	err := c.Sync()
	if err != nil {
		return err
	}
	err = c.NewFile.Close()
	if err != nil {
		return err
	}
	for i := range c.OldFile {
		err := c.OldFile[i].Close()
		if err != nil {
			return err
		}
	}
	return nil
}
