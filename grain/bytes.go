package grain

func newReadBuf() any {
	return make([]byte, 1024)
}

func newHeaderBuf() any {
	return make([]byte, 24)
}
