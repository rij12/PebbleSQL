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
	NumSlots        uint16
	FreeSpaceOffset uint16 // grows up from header+slots
	FreeSpaceEnd    uint16 // shrinks down from end of page
}

type SlotEntry struct {
	ValueOffset uint16 // offset to row data in page
	ValueLength uint16 // length of row data
	Deleted     bool   // mark if row was deleted
}

func NewEmptyHeapPage() *HeapPage {
	buf := make([]byte, PageSize)
	header := PageHeader{
		NumSlots:        0,
		FreeSpaceOffset: uint16(unsafe.Sizeof(PageHeader{})),
		FreeSpaceEnd:    PageSize,
	}
	WriteHeader(buf[:], &header)
	return &HeapPage{Buf: buf}
}

func (p *HeapPage) InsertRow(row []byte) (uint16, error) {
	header := ReadHeader(p.Buf[:])

	slotSize := int(unsafe.Sizeof(SlotEntry{}))
	rowSize := len(row)
	neededSpace := uint16(slotSize + rowSize)
	available := header.FreeSpaceEnd - header.FreeSpaceOffset

	if available < neededSpace {
		return 0, ErrPageFull
	}

	// Write row data at end
	rowStart := header.FreeSpaceEnd - uint16(rowSize)
	copy(p.Buf[rowStart:header.FreeSpaceEnd], row)

	// Write new slot entry
	slot := SlotEntry{
		ValueOffset: rowStart,
		ValueLength: uint16(rowSize),
		Deleted:     false,
	}

	slotOffset := int(unsafe.Sizeof(PageHeader{})) + int(header.NumSlots)*slotSize
	WriteSlot(p.Buf[slotOffset:slotOffset+slotSize], &slot)

	// Update header
	header.NumSlots++
	header.FreeSpaceOffset += uint16(slotSize)
	header.FreeSpaceEnd = rowStart
	WriteHeader(p.Buf[:], &header)

	return header.NumSlots - 1, nil
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
	binary.LittleEndian.PutUint16(buf[0:], header.NumSlots)
	binary.LittleEndian.PutUint16(buf[2:], header.FreeSpaceOffset)
	binary.LittleEndian.PutUint16(buf[4:], header.FreeSpaceEnd)
}

func ReadHeader(buf []byte) PageHeader {
	return PageHeader{
		NumSlots:        binary.LittleEndian.Uint16(buf[0:]),
		FreeSpaceOffset: binary.LittleEndian.Uint16(buf[2:]),
		FreeSpaceEnd:    binary.LittleEndian.Uint16(buf[4:]),
	}
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
