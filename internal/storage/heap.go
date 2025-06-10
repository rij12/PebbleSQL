package storage

import (
	"errors"
	"log/slog"
	"os"
)

var ErrPageFull = errors.New("page full")
var ErrRowDeleted = errors.New("row deleted")
var ErrInsertFailed = errors.New("insert failed")
var logger = slog.New(slog.NewTextHandler(os.Stdout, nil))

const FIRST_SLOT_ID = 1

// TODO - do we need a lock a on PageIDs?
type HeapFile struct {
	diskManager DiskManager
	pageIDs     []uint32
	// Add Free Space Manager here one day!
	// https://github.com/postgres/postgres/tree/master/src/backend/storage/freespace
}

// TODO - Load Heap File with save data.
func NewHeapFile(dm *PebbleSQLDiskManager, pageIDs []uint32) *HeapFile {
	pageID := dm.AllocatePage()
	page := NewEmptyHeapPage(pageID)
	err := dm.SavePage(pageID, page.Buf)
	if err != nil {
		panic(err)
	}

	return &HeapFile{
		diskManager: dm,
		pageIDs:     []uint32{pageID},
	}
}

func (heapFile *HeapFile) insertRow(row []byte) (RowPointer, error) {
	// Need to implement an FSM Tree so
	// I don't need o load a page to check if there is free space.
	for _, pageID := range heapFile.pageIDs {
		page, err := heapFile.diskManager.LoadPage(pageID)
		if err != nil {
			return RowPointer{}, err
		}
		freeSpace := int(ReadHeader(page).FreeSpaceEnd) - int(ReadHeader(page).FreeSpaceOffset)
		// We found enough space.
		if freeSpace > len(row) {
			// Insert Row into page
			slotId, err := InsertRow(page, row)
			if err != nil {
				return RowPointer{}, ErrInsertFailed
			}
			return RowPointer{PageId: GetPageId(page), SlotId: slotId}, nil
		}
		logger.Debug("No free space left in page with ID: ", pageID)
	}
	heapPage, err := heapFile.allocatePage()
	if err != nil {
		return RowPointer{}, err
	}
	return RowPointer{GetPageId(heapPage.Buf), FIRST_SLOT_ID}, nil
}

func (heapFile *HeapFile) allocatePage() (*HeapPage, error) {
	newPageId := heapFile.diskManager.AllocatePageID()
	newPage := NewEmptyHeapPage(newPageId)
	heapFile.pageIDs = append(heapFile.pageIDs, newPageId)
	err := heapFile.diskManager.SavePage(newPageId, newPage.Buf)
	if err != nil {
		logger.Error("Could not save page", "error", err)
		return &HeapPage{}, err
	}
	return newPage, nil
}
