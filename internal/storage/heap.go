package storage

import "errors"

var ErrPageFull = errors.New("page full")
var ErrRowDeleted = errors.New("row deleted")

type HeapFile struct {
	diskManager DiskManager
	pageIDs     []uint32
	// Add Free Space Manager here one day!
	// https://github.com/postgres/postgres/tree/master/src/backend/storage/freespace
}

func NewHeapFile(dm *PebbleSQLDiskManager, pageIDs []uint32) *HeapFile {

	pageID := dm.AllocatePage()
	page = New

	return &HeapFile{
		diskManager: dm,
		pageIDs:     pageIDs,
	}
}
