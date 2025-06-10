package storage

import (
	"encoding/binary"
	"unsafe"
)

const (
	PageSize = 8192
)

type HeapPage struct {
	Buf []byte
}

type PageHeader struct {
	pageId          uint32
	NumSlots        uint16
	FreeSpaceOffset uint16 // grows up from header+slots
	FreeSpaceEnd    uint16 // shrinks down from end of page
}

type SlotEntry struct {
	ValueOffset uint16 // offset to row data in page
	ValueLength uint16 // length of row data
	Deleted     bool   // mark if row was deleted
}

func NewEmptyHeapPage(pageId uint32) *HeapPage {
	buf := make([]byte, PageSize)
	header := PageHeader{
		pageId:          pageId,
		NumSlots:        0,
		FreeSpaceOffset: uint16(unsafe.Sizeof(PageHeader{})),
		FreeSpaceEnd:    PageSize,
	}
	WriteHeader(buf[:], &header)
	return &HeapPage{Buf: buf}
}

func InsertRow(page []byte, row []byte) (uint16, error) {
	header := ReadHeader(page[:])

	slotSize := int(unsafe.Sizeof(SlotEntry{}))
	rowSize := len(row)
	neededSpace := uint16(slotSize + rowSize)
	available := header.FreeSpaceEnd - header.FreeSpaceOffset

	if available < neededSpace {
		return 0, ErrPageFull
	}

	// Write row data at end
	rowStart := header.FreeSpaceEnd - uint16(rowSize)
	copy(page[rowStart:header.FreeSpaceEnd], row)

	// Write new slot entry
	slot := SlotEntry{
		ValueOffset: rowStart,
		ValueLength: uint16(rowSize),
		Deleted:     false,
	}

	slotOffset := int(unsafe.Sizeof(PageHeader{})) + int(header.NumSlots)*slotSize
	WriteSlot(page[slotOffset:slotOffset+slotSize], &slot)

	// Update header
	header.NumSlots++
	header.FreeSpaceOffset += uint16(slotSize)
	header.FreeSpaceEnd = rowStart
	WriteHeader(page[:], &header)

	return header.NumSlots, nil
}

func (p *HeapPage) LoadRow(slotNum uint16) ([]byte, error) {
	slotSize := int(unsafe.Sizeof(SlotEntry{}))
	slotOffset := int(unsafe.Sizeof(PageHeader{})) + int(slotNum)*slotSize

	slot := ReadSlot(p.Buf[slotOffset : slotOffset+slotSize])
	if slot.Deleted {
		return nil, ErrRowDeleted
	}

	return p.Buf[slot.ValueOffset : slot.ValueOffset+slot.ValueLength], nil
}

func (p *HeapPage) DeleteRow(slotNum uint16) error {
	slotSize := int(unsafe.Sizeof(SlotEntry{}))
	slotOffset := int(unsafe.Sizeof(PageHeader{})) + int(slotNum)*slotSize

	slot := ReadSlot(p.Buf[slotOffset : slotOffset+slotSize])
	slot.Deleted = true
	WriteSlot(p.Buf[slotOffset:slotOffset+slotSize], &slot)
	return nil
}

func WriteHeader(buf []byte, header *PageHeader) {
	// Write pageId at offset 0
	binary.LittleEndian.PutUint32(buf[0:], header.pageId)
	// Write NumSlots at offset 2
	binary.LittleEndian.PutUint16(buf[2:], header.NumSlots)
	// Write FreeSpaceOffset at offset 4
	binary.LittleEndian.PutUint16(buf[4:], header.FreeSpaceOffset)
	// Write FreeSpaceEnd at offset 6
	binary.LittleEndian.PutUint16(buf[6:], header.FreeSpaceEnd)
}

func ReadHeader(buf []byte) PageHeader {
	return PageHeader{
		pageId:          binary.LittleEndian.Uint32(buf[0:]),
		NumSlots:        binary.LittleEndian.Uint16(buf[2:]),
		FreeSpaceOffset: binary.LittleEndian.Uint16(buf[4:]),
		FreeSpaceEnd:    binary.LittleEndian.Uint16(buf[6:]),
	}
}

func GetPageId(buf []byte) uint16 {
	return binary.LittleEndian.Uint16(buf[:2])
}

func WriteSlot(buf []byte, slot *SlotEntry) {
	binary.LittleEndian.PutUint16(buf[0:], slot.ValueOffset)
	binary.LittleEndian.PutUint16(buf[2:], slot.ValueLength)
	if slot.Deleted {
		buf[4] = 1
	} else {
		buf[4] = 0
	}
}

func ReadSlot(buf []byte) SlotEntry {
	return SlotEntry{
		ValueOffset: binary.LittleEndian.Uint16(buf[0:]),
		ValueLength: binary.LittleEndian.Uint16(buf[2:]),
		Deleted:     buf[4] == 1,
	}
}
