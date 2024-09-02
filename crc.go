package corn

import "hash/crc32"

func CRC32(data []byte) (crc uint32, err error) {
	hash32 := crc32.NewIEEE()
	_, err = hash32.Write(data)
	if err != nil {
		return
	}
	crc = crc32.ChecksumIEEE(hash32.Sum(nil))
	return
}
