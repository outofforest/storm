package filedev

import (
	"io"
	"os"

	"github.com/pkg/errors"
)

var _ io.ReadWriteSeeker = &FileDev{}

// FileDev uses file handle as a device.
type FileDev struct {
	file *os.File
	size int64
}

// New returns new filedev.
func New(file *os.File) *FileDev {
	size, err := file.Seek(0, io.SeekEnd)
	if err != nil {
		panic(errors.WithStack(err))
	}
	return &FileDev{
		file: file,
		size: size,
	}
}

// Seek seeks the position.
func (fd *FileDev) Seek(offset int64, whence int) (int64, error) {
	n, err := fd.file.Seek(offset, whence)
	return n, errors.WithStack(err)
}

// Read reads data from the file.
func (fd *FileDev) Read(p []byte) (int, error) {
	n, err := fd.file.Read(p)
	return n, errors.WithStack(err)
}

// Write writes data to the file.
func (fd *FileDev) Write(p []byte) (int, error) {
	n, err := fd.file.Write(p)
	return n, errors.WithStack(err)
}

// Sync syncs data to the file.
func (fd *FileDev) Sync() error {
	return errors.WithStack(fd.file.Sync())
}

// Size returns the byte size of the file.
func (fd *FileDev) Size() int64 {
	return fd.size
}
