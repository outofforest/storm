package memdev

import (
	"io"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestSeekStart(t *testing.T) {
	assertT := assert.New(t)

	dev := newDev()

	o, err := dev.Seek(-1, io.SeekStart)
	assertT.Error(err)
	assertT.EqualValues(0, o)

	o, err = dev.Seek(0, io.SeekStart)
	assertT.NoError(err)
	assertT.EqualValues(0, o)

	o, err = dev.Seek(5, io.SeekStart)
	assertT.NoError(err)
	assertT.EqualValues(5, o)

	o, err = dev.Seek(10, io.SeekStart)
	assertT.NoError(err)
	assertT.EqualValues(10, o)

	o, err = dev.Seek(11, io.SeekStart)
	assertT.Error(err)
	assertT.EqualValues(0, o)
}

func TestSeekCurrent(t *testing.T) {
	assertT := assert.New(t)

	dev := newDev()

	o, err := dev.Seek(-1, io.SeekCurrent)
	assertT.Error(err)
	assertT.EqualValues(0, o)

	o, err = dev.Seek(11, io.SeekCurrent)
	assertT.Error(err)
	assertT.EqualValues(0, o)

	o, err = dev.Seek(10, io.SeekCurrent)
	assertT.NoError(err)
	assertT.EqualValues(10, o)

	o, err = dev.Seek(1, io.SeekCurrent)
	assertT.Error(err)
	assertT.EqualValues(0, o)

	o, err = dev.Seek(-5, io.SeekCurrent)
	assertT.NoError(err)
	assertT.EqualValues(5, o)

	o, err = dev.Seek(-6, io.SeekCurrent)
	assertT.Error(err)
	assertT.EqualValues(0, o)

	o, err = dev.Seek(-5, io.SeekCurrent)
	assertT.NoError(err)
	assertT.EqualValues(0, o)

	o, err = dev.Seek(7, io.SeekCurrent)
	assertT.NoError(err)
	assertT.EqualValues(7, o)

	o, err = dev.Seek(-1, io.SeekCurrent)
	assertT.NoError(err)
	assertT.EqualValues(6, o)

	o, err = dev.Seek(2, io.SeekCurrent)
	assertT.NoError(err)
	assertT.EqualValues(8, o)

	o, err = dev.Seek(0, io.SeekCurrent)
	assertT.NoError(err)
	assertT.EqualValues(8, o)
}

func TestSeekEnd(t *testing.T) {
	assertT := assert.New(t)

	dev := newDev()

	o, err := dev.Seek(0, io.SeekEnd)
	assertT.NoError(err)
	assertT.EqualValues(10, o)

	o, err = dev.Seek(1, io.SeekEnd)
	assertT.Error(err)
	assertT.EqualValues(0, o)

	o, err = dev.Seek(-1, io.SeekEnd)
	assertT.NoError(err)
	assertT.EqualValues(9, o)

	o, err = dev.Seek(-10, io.SeekEnd)
	assertT.NoError(err)
	assertT.EqualValues(0, o)

	o, err = dev.Seek(-11, io.SeekEnd)
	assertT.Error(err)
	assertT.EqualValues(0, o)

	o, err = dev.Seek(-5, io.SeekEnd)
	assertT.NoError(err)
	assertT.EqualValues(5, o)
}

func TestRead(t *testing.T) {
	assertT := assert.New(t)

	dev := newDev()

	n, err := dev.Read(nil)
	assertT.NoError(err)
	assertT.EqualValues(0, n)

	n, err = dev.Read([]byte{})
	assertT.NoError(err)
	assertT.EqualValues(0, n)

	buf := make([]byte, 3)
	n, err = dev.Read(buf)
	assertT.NoError(err)
	assertT.EqualValues(3, n)
	assertT.EqualValues([]byte{0x00, 0x01, 0x02}, buf)

	o, err := dev.Seek(1, io.SeekStart)
	assertT.NoError(err)
	assertT.EqualValues(1, o)
	n, err = dev.Read(buf)
	assertT.NoError(err)
	assertT.EqualValues(3, n)
	assertT.EqualValues([]byte{0x01, 0x02, 0x03}, buf)

	o, err = dev.Seek(-1, io.SeekEnd)
	assertT.NoError(err)
	assertT.EqualValues(9, o)
	n, err = dev.Read(buf)
	assertT.NoError(err)
	assertT.EqualValues(1, n)
	assertT.EqualValues([]byte{0x09, 0x02, 0x03}, buf)

	o, err = dev.Seek(0, io.SeekEnd)
	assertT.NoError(err)
	assertT.EqualValues(10, o)
	n, err = dev.Read(buf)
	assertT.NoError(err)
	assertT.EqualValues(0, n)
	assertT.EqualValues([]byte{0x09, 0x02, 0x03}, buf)

	o, err = dev.Seek(0, io.SeekStart)
	assertT.NoError(err)
	assertT.EqualValues(0, o)
	n, err = dev.Read(buf)
	assertT.NoError(err)
	assertT.EqualValues(3, n)
	n, err = dev.Read(buf)
	assertT.NoError(err)
	assertT.EqualValues(3, n)
	assertT.EqualValues([]byte{0x03, 0x04, 0x05}, buf)
}

func TestWrite(t *testing.T) {
	assertT := assert.New(t)

	dev := newDev()

	n, err := dev.Write(nil)
	assertT.NoError(err)
	assertT.EqualValues(0, n)

	n, err = dev.Write([]byte{})
	assertT.NoError(err)
	assertT.EqualValues(0, n)

	buf := []byte{0x10, 0x11, 0x12}
	n, err = dev.Write(buf)
	assertT.NoError(err)
	assertT.EqualValues(3, n)
	assertT.EqualValues([]byte{0x10, 0x11, 0x12, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09}, dev.data)

	o, err := dev.Seek(1, io.SeekStart)
	assertT.NoError(err)
	assertT.EqualValues(1, o)
	n, err = dev.Write(buf)
	assertT.NoError(err)
	assertT.EqualValues(3, n)
	assertT.EqualValues([]byte{0x10, 0x10, 0x11, 0x12, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09}, dev.data)

	o, err = dev.Seek(-1, io.SeekEnd)
	assertT.NoError(err)
	assertT.EqualValues(9, o)
	n, err = dev.Write(buf)
	assertT.NoError(err)
	assertT.EqualValues(1, n)
	assertT.EqualValues([]byte{0x10, 0x10, 0x11, 0x12, 0x04, 0x05, 0x06, 0x07, 0x08, 0x10}, dev.data)

	o, err = dev.Seek(0, io.SeekEnd)
	assertT.NoError(err)
	assertT.EqualValues(10, o)
	n, err = dev.Write(buf)
	assertT.NoError(err)
	assertT.EqualValues(0, n)
	assertT.EqualValues([]byte{0x10, 0x10, 0x11, 0x12, 0x04, 0x05, 0x06, 0x07, 0x08, 0x10}, dev.data)

	o, err = dev.Seek(2, io.SeekStart)
	assertT.NoError(err)
	assertT.EqualValues(2, o)
	n, err = dev.Write(buf)
	assertT.NoError(err)
	assertT.EqualValues(3, n)
	n, err = dev.Write(buf)
	assertT.NoError(err)
	assertT.EqualValues(3, n)
	assertT.EqualValues([]byte{0x10, 0x10, 0x10, 0x11, 0x12, 0x10, 0x11, 0x12, 0x08, 0x10}, dev.data)
}

func TestSync(t *testing.T) {
	assertT := assert.New(t)

	dev := newDev()
	assertT.NoError(dev.Sync())
}

func newDev() *MemDev {
	const size = 10

	dev := New(size)
	for i := 0; i < size; i++ {
		dev.data[i] = byte(i)
	}

	return dev
}
