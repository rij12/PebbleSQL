package storage

import (
	"encoding/binary"
	"errors"
	"unsafe"
)

const (
	PageSize             = 4096
	PageHeaderSize       = 20
	OverflowHeaderSize   = 6
	OverflowDataSize     = PageSize - OverflowHeaderSize
	MagicNumberBLinkTree = 0xB10CDBDB
	CurrentPageVersion   = 1
	InlineThreshold      = PageSize / 4
)

type PageHeader struct {
	MagicNumber uint32 // 4 bytes  (offset 0)
	PageVersion uint16 // 2 bytes  (offset 4)
	IsLeaf      bool   // 1 byte   (offset 6)
	// padding byte here     1 byte (offset 7, padding)
	NumKeys         uint16 // 2 bytes (offset 8)
	FreeSpaceOffset uint16 // 2 bytes (offset 10)
	FreeSpaceEnd    uint16 // 2 bytes (offset 12)
	// padding 2 bytes here    (offset 14-15, padding)
	RightSiblingPageID uint32 // 4 bytes (offset 16)
}

type SlotEntry struct {
	Key            uint64
	ValueOffset    uint16
	ValueLength    uint16
	OverflowPageID uint32
	ChildPageID    uint32
	Deleted        bool
}

type OverflowPageHeader struct {
	NextOverflowPageID uint32 // 4 bytes
	DataLength         uint16 // 2 bytes
}

type BLinkTreePage struct {
	Buf []byte
}

type OverflowPage struct {
	Buf []byte
}

func WriteHeader(buf []byte, header *PageHeader) {
	if len(buf) < int(unsafe.Sizeof(PageHeader{})) {
		panic("buffer too small for PageHeader")
	}
	binary.LittleEndian.PutUint32(buf[0:4], header.MagicNumber)
	binary.LittleEndian.PutUint16(buf[4:6], header.PageVersion)
	buf[6] = boolToByte(header.IsLeaf)
	buf[7] = 0
	binary.LittleEndian.PutUint16(buf[8:10], header.NumKeys)
	binary.LittleEndian.PutUint16(buf[10:12], header.FreeSpaceOffset)
	binary.LittleEndian.PutUint16(buf[12:14], header.FreeSpaceEnd)
	buf[14] = 0
	buf[15] = 0
	binary.LittleEndian.PutUint32(buf[16:20], header.RightSiblingPageID)
}

func ReadHeader(buf []byte) *PageHeader {
	if len(buf) < int(unsafe.Sizeof(PageHeader{})) {
		panic("buffer too small for PageHeader")
	}
	return &PageHeader{
		MagicNumber:        binary.LittleEndian.Uint32(buf[0:4]),
		PageVersion:        binary.LittleEndian.Uint16(buf[4:6]),
		IsLeaf:             byteToBool(buf[6]),
		NumKeys:            binary.LittleEndian.Uint16(buf[8:10]),
		FreeSpaceOffset:    binary.LittleEndian.Uint16(buf[10:12]),
		FreeSpaceEnd:       binary.LittleEndian.Uint16(buf[12:14]),
		RightSiblingPageID: binary.LittleEndian.Uint32(buf[16:20]),
	}
}

func WriteSlot(buf []byte, slot *SlotEntry) {
	if len(buf) < int(unsafe.Sizeof(SlotEntry{})) {
		panic("buffer too small for SlotEntry")
	}
	binary.LittleEndian.PutUint64(buf[0:8], slot.Key)
	binary.LittleEndian.PutUint16(buf[8:10], slot.ValueOffset)
	binary.LittleEndian.PutUint16(buf[10:12], slot.ValueLength)
	binary.LittleEndian.PutUint32(buf[12:16], slot.OverflowPageID)
	binary.LittleEndian.PutUint32(buf[16:20], slot.ChildPageID)
}

func ReadSlot(buf []byte) SlotEntry {
	if len(buf) < int(unsafe.Sizeof(SlotEntry{})) {
		panic("buffer too small for SlotEntry")
	}
	return SlotEntry{
		Key:            binary.LittleEndian.Uint64(buf[0:8]),
		ValueOffset:    binary.LittleEndian.Uint16(buf[8:10]),
		ValueLength:    binary.LittleEndian.Uint16(buf[10:12]),
		OverflowPageID: binary.LittleEndian.Uint32(buf[12:16]),
		ChildPageID:    binary.LittleEndian.Uint32(buf[16:20]),
	}
}

func WriteOverflowHeader(buf []byte, header *OverflowPageHeader) {
	if len(buf) < OverflowHeaderSize {
		panic("buffer too small for OverflowPageHeader")
	}
	binary.LittleEndian.PutUint32(buf[0:4], header.NextOverflowPageID)
	binary.LittleEndian.PutUint16(buf[4:6], header.DataLength)
}

func ReadOverflowHeader(buf []byte) *OverflowPageHeader {
	if len(buf) < OverflowHeaderSize {
		panic("buffer too small for OverflowPageHeader")
	}
	return &OverflowPageHeader{
		NextOverflowPageID: binary.LittleEndian.Uint32(buf[0:4]),
		DataLength:         binary.LittleEndian.Uint16(buf[4:6]),
	}
}

func boolToByte(b bool) byte {
	if b {
		return 1
	}
	return 0
}

func byteToBool(b byte) bool {
	return b != 0
}

func (p *BLinkTreePage) InsertKeyValue(
	key uint64,
	value []byte,
	allocateOverflowPage func() (*OverflowPage, uint32),
) error {
	header := ReadHeader(p.Buf[:PageHeaderSize])
	slotSize := uint16(unsafe.Sizeof(SlotEntry{}))
	freeSpace := header.FreeSpaceEnd - header.FreeSpaceOffset
	valueSize := uint16(len(value))
	neededSpace := slotSize + valueSize
	// If entry size is greater than InlineThreshold, entry is forced to overflow pages.
	fitsInline := freeSpace >= neededSpace && len(value) <= InlineThreshold

	slot := SlotEntry{
		Key:            key,
		ValueOffset:    0,
		ValueLength:    0,
		OverflowPageID: 0,
		ChildPageID:    0, // Leaf node insert
	}

	if fitsInline && valueSize > 0 {
		// we can store the value directly in the page
		dataEnd := header.FreeSpaceEnd - valueSize
		copy(p.Buf[dataEnd:header.FreeSpaceEnd], value)
		slot.ValueOffset = dataEnd
		slot.ValueLength = valueSize
		// Move free space end backward
		header.FreeSpaceEnd = dataEnd
	} else {
		// value too large or not enough space - use overflow pages
		// Allocate first overflow page
		// Write first page chunk
		// allocate second overflow page (if required)
		// Write page
		remaining := len(value)
		currentData := value
		var firstOverFlowPageID uint32
		var lastOverFlowPage *OverflowPage
		firstOverFlowPageID = 0
		for remaining > 0 {
			overflowPage, overflowPageID := allocateOverflowPage()
			chunkSize := OverflowDataSize
			if remaining < OverflowDataSize {
				chunkSize = remaining
			}

			WriteOverflowHeader(overflowPage.Buf[:], &OverflowPageHeader{
				NextOverflowPageID: 0,
				DataLength:         uint16(chunkSize),
			})
			// Copy chunk directly
			copy(overflowPage.Buf[OverflowHeaderSize:OverflowHeaderSize+chunkSize],
				currentData[:chunkSize])

			if lastOverFlowPage != nil {
				// This not the first page, so we need to link this new page to the old one.
				// This is a linkedList of pages.
				// link last page to this new page
				WriteOverflowHeader(lastOverFlowPage.Buf[:], &OverflowPageHeader{
					NextOverflowPageID: overflowPageID,
					DataLength:         OverflowDataSize,
				})
				// Save previous overflow page to disk!
				//diskManager.SavePage(lastOverflowPageID, lastOverflowPage.Buf)
			} else {
				// First page in chain
				firstOverFlowPageID = overflowPageID
			}
			remaining -= chunkSize
			currentData = currentData[chunkSize:]
			lastOverFlowPage = overflowPage
		}
		// Save last overflow page (no next page)
		if lastOverFlowPage != nil {
			//diskManager.SavePage(lastOverflowPageID, lastOverflowPage.Buf)
		}
		slot.OverflowPageID = firstOverFlowPageID
	}

	// slot array needs to be sorted, after every insert, so we can use binary search later.
	insertPos := findInsertPosition(p.Buf, header.NumKeys, key)
	if insertPos < int(header.NumKeys) {
		shiftSlots(p.Buf, insertPos, header.NumKeys)
	}
	slotOffset := int(unsafe.Sizeof(PageHeader{})) + insertPos*int(slotSize)
	WriteSlot(p.Buf[slotOffset:slotOffset+int(slotSize)], &slot)

	// Write Headers information to buffer
	header.NumKeys++
	header.FreeSpaceOffset += slotSize
	WriteHeader(p.Buf[:PageHeaderSize], header)

	return nil
}

var ErrorKeyNotFound = errors.New("key not found")

func (p *BLinkTreePage) FindKey(
	key uint64,
	loadOverflowPage func(pageID uint32) (*OverflowPage, error),
) ([]byte, error) {
	header := ReadHeader(p.Buf[:20])

	low := 0
	high := int(header.NumKeys) - 1
	slotSize := int(unsafe.Sizeof(SlotEntry{}))

	for low <= high {
		mid := (low + high) / 2
		slotOffset := int(unsafe.Sizeof(PageHeader{})) + mid*slotSize
		slot := ReadSlot(p.Buf[slotOffset : slotOffset+slotSize])

		if key == slot.Key {
			// Found it
			if slot.OverflowPageID == 0 {
				// Inline value
				valueStart := slot.ValueOffset
				valueEnd := valueStart + slot.ValueLength
				value := make([]byte, slot.ValueLength)
				copy(value, p.Buf[valueStart:valueEnd])
				return value, nil
			} else {
				// Reconstruct from overflow pages
				var result []byte
				overflowPageID := slot.OverflowPageID

				for overflowPageID != 0 {
					overflowPage, err := loadOverflowPage(overflowPageID)
					if err != nil {
						return nil, err
					}

					overflowHeader := ReadOverflowHeader(overflowPage.Buf[:])
					dataStart := OverflowHeaderSize
					dataEnd := dataStart + int(overflowHeader.DataLength)

					result = append(result, overflowPage.Buf[dataStart:dataEnd]...)

					overflowPageID = overflowHeader.NextOverflowPageID
				}
				return result, nil
			}
		} else if key < slot.Key {
			high = mid - 1
		} else {
			low = mid + 1
		}
	}
	return nil, ErrorKeyNotFound
}

// Helper to find insert position (sorted order)
func findInsertPosition(buf []byte, numKeys uint16, key uint64) int {
	slotSize := int(unsafe.Sizeof(SlotEntry{}))
	for i := 0; i < int(numKeys); i++ {
		slotOffset := int(unsafe.Sizeof(PageHeader{})) + i*slotSize
		existingSlot := ReadSlot(buf[slotOffset : slotOffset+slotSize])
		if key < existingSlot.Key {
			return i
		}
	}
	return int(numKeys)
}

// Helper to shift slots forward
func shiftSlots(buf []byte, insertPos int, numKeys uint16) {
	slotSize := int(unsafe.Sizeof(SlotEntry{}))
	srcOffset := int(unsafe.Sizeof(PageHeader{})) + insertPos*slotSize
	dstOffset := srcOffset + slotSize
	moveSize := (int(numKeys) - insertPos) * slotSize

	copy(buf[dstOffset:dstOffset+moveSize], buf[srcOffset:srcOffset+moveSize])
}

func (p *BLinkTreePage) DeleteKey(key uint64) error {
	header := ReadHeader(p.Buf[:20])

	low := 0
	high := int(header.NumKeys) - 1
	slotSize := int(unsafe.Sizeof(SlotEntry{}))

	for low <= high {
		mid := (low + high) / 2
		slotOffset := int(unsafe.Sizeof(PageHeader{})) + mid*slotSize
		slot := ReadSlot(p.Buf[slotOffset : slotOffset+slotSize])

		if slot.Key == key {
			slot.Deleted = true
			WriteSlot(p.Buf[slotOffset:slotOffset+slotSize], &slot)
			return nil
		} else if key < slot.Key {
			high = mid - 1
		} else {
			low = mid + 1
		}
	}
	return ErrorKeyNotFound
}

// TODO - Also need a way to de-frag pages at a later point in time.
