package storage

import (
	"fmt"
	"os"
	"sync"
	"sync/atomic"
)

type DiskManager interface {
	AllocatePage() uint32
	AllocatePageID() uint32
	SavePage(pageID uint32, buf []byte) error
	LoadPage(pageID uint32) ([]byte, error)
	Close() error
}

type PebbleSQLDiskManager struct {
	mu         sync.Mutex
	file       *os.File
	nextPageID uint32
}

func NewDiskManager(filename string) (DiskManager, error) {
	file, err := os.OpenFile(filename, os.O_RDWR|os.O_CREATE, 0666)
	if err != nil {
		return nil, err
	}

	// Calculate nextPageID based on file size
	stat, err := file.Stat()
	if err != nil {
		return nil, err
	}
	nextPageID := uint32(stat.Size() / PageSize)

	return &PebbleSQLDiskManager{
		file:       file,
		nextPageID: nextPageID,
	}, nil
}

func (d *PebbleSQLDiskManager) AllocatePageID() uint32 {
	return atomic.AddUint32(&d.nextPageID, 1) - 1
}

func (d *PebbleSQLDiskManager) AllocatePage() uint32 {
	return d.AllocatePageID()
}

func (d *PebbleSQLDiskManager) SavePage(pageID uint32, buf []byte) error {
	if len(buf) != PageSize {
		return fmt.Errorf("buffer must be exactly one page")
	}

	d.mu.Lock()
	defer d.mu.Unlock()

	offset := int64(pageID) * int64(PageSize)
	_, err := d.file.WriteAt(buf, offset)
	return err
}

func (d *PebbleSQLDiskManager) LoadPage(pageID uint32) ([]byte, error) {
	d.mu.Lock()
	defer d.mu.Unlock()

	buf := make([]byte, PageSize)
	offset := int64(pageID) * int64(PageSize)
	_, err := d.file.ReadAt(buf, offset)
	if err != nil {
		return nil, err
	}
	return buf, nil
}

func (d *PebbleSQLDiskManager) Close() error {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.file.Close()
}
