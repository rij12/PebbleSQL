package storage

type Table struct {
	HeapFile     *HeapFile             // Stores full rows
	Indexes      map[string]*BLinkTree // Named indexes
	KeyExtractor func(row []byte, indexName string) (uint64, error)
}
