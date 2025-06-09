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
	page := NewEmptyHeapPage()
	err := dm.SavePage(pageID, page.Buf)
	if err != nil {
		panic(err)
	}

	return &HeapFile{
		diskManager: dm,
		pageIDs:     []uint32{pageID},
	}
}

func (heapFile HeapFile) insertRow(row []byte) error {

	// Need to implement an FSM Tree so
	// I don't need o load a page to check if there is free space.
	pageFound := false
	for _, pageID := range heapFile.pageIDs {
		page, err := heapFile.diskManager.LoadPage(pageID)
		if err != nil {
			return err
		}
		freeSpace := int(ReadHeader(page).FreeSpaceEnd) - int(ReadHeader(page).FreeSpaceOffset)

		// We found enough space.
		if freeSpace > len(row) {
			pageFound = true
			
		} else {
			continue
		}

	}

}
