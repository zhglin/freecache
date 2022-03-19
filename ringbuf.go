package freecache

import (
	"bytes"
	"errors"
	"fmt"
	"io"
)

var ErrOutOfRange = errors.New("out of range")

// RingBuf Ring buffer has a fixed size, when data exceeds the
// size, old data will be overwritten by new data.
// It only contains the data in the stream from begin to end
// 环形缓冲区具有固定的大小，当数据超过该大小时，旧数据将被新数据覆盖。它只包含流中从头到尾的数据
// end随着写入的数据一直增加，
// 当end>len(rb.data)时rb.begin = rb.end - int64(len(rb.data))
// index始终是写入的起始位置，在0-len(rb.data)之间
// begin与index相差n倍的len(rb.data)
type RingBuf struct {
	begin int64 // beginning offset of the data stream.	数据流的开始偏移量。写入过程中不变
	end   int64 // ending offset of the data stream. 数据流的结束偏移量。
	data  []byte
	index int //range from '0' to 'len(rb.data)-1' //  写入的起始下标 write中一直变化
}

// NewRingBuf 创建并初始化ringBuf
func NewRingBuf(size int, begin int64) (rb RingBuf) {
	rb.data = make([]byte, size)
	rb.Reset(begin)
	return
}

// Reset the ring buffer
//
// Parameters:
//     begin: beginning offset of the data stream
// 初始化ringBuf
func (rb *RingBuf) Reset(begin int64) {
	rb.begin = begin
	rb.end = begin
	rb.index = 0
}

// Dump Create a copy of the buffer.
// 创建并返回缓冲区的副本。
func (rb *RingBuf) Dump() []byte {
	dump := make([]byte, len(rb.data))
	copy(dump, rb.data)
	return dump
}

// 已可读的形式返回ringBuf信息
func (rb *RingBuf) String() string {
	return fmt.Sprintf("[size:%v, start:%v, end:%v, index:%v]", len(rb.data), rb.begin, rb.end, rb.index)
}

// Size 返回buf总长
func (rb *RingBuf) Size() int64 {
	return int64(len(rb.data))
}

// Begin 返回buf起始位置
func (rb *RingBuf) Begin() int64 {
	return rb.begin
}

// End 返回buff结束位置
func (rb *RingBuf) End() int64 {
	return rb.end
}

// ReadAt read up to len(p), at off of the data stream.
// 从off位置开始读取len(p)长度的数据到p中。
func (rb *RingBuf) ReadAt(p []byte, off int64) (n int, err error) {
	if off > rb.end || off < rb.begin {
		err = ErrOutOfRange
		return
	}
	readOff := rb.getDataOff(off)
	// off到rb.end的总长度，校验有没有回环 ----------e-----e
	readEnd := readOff + int(rb.end-off)
	// 未回环
	if readEnd <= len(rb.data) {
		n = copy(p, rb.data[readOff:readEnd])
	} else {
		// 已回环
		n = copy(p, rb.data[readOff:])
		// 复制的长度小于p的长度
		if n < len(p) {
			n += copy(p[n:], rb.data[:readEnd-len(rb.data)]) // readEnd-len(rb.data) 回环的长度
		}
	}
	if n < len(p) {
		err = io.EOF
	}
	return
}

// 根据off找到有效的起始位置
// 写入元素时候的偏移量始终是end的位置
// 那么off的值就会大于len(rb.data)，需要进行修正。
// 回环的时候不能直接用off-len(rb.data)，因为end可能会超过len(rb.data)的很多倍。
//       index
// |----------|-----
//       begin      end
// off的值是rb.end值，rb.end与rb.begin相差len(rb.data)
// rb.index - rb.begin + off
func (rb *RingBuf) getDataOff(off int64) int {
	var dataOff int
	if rb.end-rb.begin < int64(len(rb.data)) { // 未回环并且空间未用完，这个时候begin=0，直接减
		dataOff = int(off - rb.begin)
	} else { // 空间已写完或者已经回环了， 其实 = off % len(len(rb.begin)
		dataOff = rb.index + int(off-rb.begin)
	}
	if dataOff >= len(rb.data) {
		dataOff -= len(rb.data)
	}
	return dataOff
}

// Slice returns a slice of the supplied range of the ring buffer. It will
// not alloc unless the requested range wraps the ring buffer.
// 返回循环缓冲区所提供范围的一个切片。除非请求的范围包装了循环缓冲区，否则它不会进行分配。
func (rb *RingBuf) Slice(off, length int64) ([]byte, error) {
	if off > rb.end || off < rb.begin {
		return nil, ErrOutOfRange
	}
	readOff := rb.getDataOff(off)

	// 如果未回环，直接返回切片
	readEnd := readOff + int(length)
	if readEnd <= len(rb.data) {
		return rb.data[readOff:readEnd:readEnd], nil
	}

	// 已回环，进行复制
	buf := make([]byte, length)
	n := copy(buf, rb.data[readOff:])
	if n < int(length) {
		n += copy(buf[n:], rb.data[:readEnd-len(rb.data)])
	}
	if n < int(length) {
		return nil, io.EOF
	}
	return buf, nil
}

// 写入数据 回环写
func (rb *RingBuf) Write(p []byte) (n int, err error) {
	// 校验要写入的长度
	if len(p) > len(rb.data) {
		err = ErrOutOfRange
		return
	}
	// 从0开始往rb.data中写
	for n < len(p) {
		written := copy(rb.data[rb.index:], p[n:])
		rb.end += int64(written) // 增加未指针
		n += written
		rb.index += written
		// index最大只能==len(rb.data)
		if rb.index >= len(rb.data) {
			// 达到最大值时index置为0，从头开始写回环写
			rb.index -= len(rb.data)
		}
	}

	// end一直累加会远远大于begin
	// begin和end始终相差len(rb.data)
	if int(rb.end-rb.begin) > len(rb.data) {
		rb.begin = rb.end - int64(len(rb.data))
	}
	return
}

// WriteAt 在off位置写入p
func (rb *RingBuf) WriteAt(p []byte, off int64) (n int, err error) {
	if off+int64(len(p)) > rb.end || off < rb.begin {
		err = ErrOutOfRange
		return
	}
	writeOff := rb.getDataOff(off)
	writeEnd := writeOff + int(rb.end-off)
	if writeEnd <= len(rb.data) {
		n = copy(rb.data[writeOff:writeEnd], p)
	} else {
		n = copy(rb.data[writeOff:], p)
		if n < len(p) {
			n += copy(rb.data[:writeEnd-len(rb.data)], p[n:])
		}
	}
	return
}

// EqualAt 从off出读取len(p)长度的数据，并进行比较
func (rb *RingBuf) EqualAt(p []byte, off int64) bool {
	if off+int64(len(p)) > rb.end || off < rb.begin {
		return false
	}
	readOff := rb.getDataOff(off)
	readEnd := readOff + len(p)
	// 未回环直接比较
	if readEnd <= len(rb.data) {
		return bytes.Equal(p, rb.data[readOff:readEnd])
	} else {
		// 已回环了，分开比较
		firstLen := len(rb.data) - readOff
		equal := bytes.Equal(p[:firstLen], rb.data[readOff:])
		if equal {
			secondLen := len(p) - firstLen
			equal = bytes.Equal(p[firstLen:], rb.data[:secondLen])
		}
		return equal
	}
}

// Evacuate read the data at off, then write it to the the data stream,
// Keep it from being overwritten by new data.
// 在off读取数据，然后写入数据流，防止它被新的数据覆盖。
// off，length的元素写入到index的位置，空出位置
func (rb *RingBuf) Evacuate(off int64, length int) (newOff int64) {
	// oldOff := seg.rb.End() + seg.vacuumLen - seg.rb.Size()
	// 所以会符合这个条件
	if off+int64(length) > rb.end || off < rb.begin {
		return -1
	}
	readOff := rb.getDataOff(off)
	if readOff == rb.index { // 相等的时候，说明index+length就是这条记录，不需要复制了，这种情况肯定已经回环了
		// no copy evacuate
		rb.index += length
		if rb.index >= len(rb.data) {
			rb.index -= len(rb.data)
		}
	} else if readOff < rb.index { // off,length的元素移动到index之后
		var n = copy(rb.data[rb.index:], rb.data[readOff:readOff+length])
		rb.index += n
		// 后面空间不足，在0处接着加
		if rb.index == len(rb.data) {
			rb.index = copy(rb.data, rb.data[readOff+n:readOff+length])
		}
	} else { // off在index右边
		var readEnd = readOff + length // 需要读取的截止位置
		var n int
		if readEnd <= len(rb.data) { // 这个位置没超过data长度 直接copy
			n = copy(rb.data[rb.index:], rb.data[readOff:readEnd])
			rb.index += n
		} else { // 数据处在尾部+头部
			n = copy(rb.data[rb.index:], rb.data[readOff:]) // 复制尾部数据
			rb.index += n
			var tail = length - n // 复制剩余的头部数据
			n = copy(rb.data[rb.index:], rb.data[:tail])
			rb.index += n                 // 头部复制的元素长度
			if rb.index == len(rb.data) { // 未复制完的数据复制到data的头部
				rb.index = copy(rb.data, rb.data[n:tail])
			}
		}
	}
	newOff = rb.end         // 新的off位置
	rb.end += int64(length) // 下一条数据的off
	if rb.begin < rb.end-int64(len(rb.data)) {
		rb.begin = rb.end - int64(len(rb.data))
	}
	return
}

func (rb *RingBuf) Resize(newSize int) {
	if len(rb.data) == newSize {
		return
	}
	newData := make([]byte, newSize)
	var offset int
	if rb.end-rb.begin == int64(len(rb.data)) {
		offset = rb.index
	}
	if int(rb.end-rb.begin) > newSize {
		discard := int(rb.end-rb.begin) - newSize
		offset = (offset + discard) % len(rb.data)
		rb.begin = rb.end - int64(newSize)
	}
	n := copy(newData, rb.data[offset:])
	if n < newSize {
		copy(newData[n:], rb.data[:offset])
	}
	rb.data = newData
	rb.index = 0
}

// Skip 跳过length的长度空间，相当于在index写入length长度的空数据
func (rb *RingBuf) Skip(length int64) {
	rb.end += length
	rb.index += int(length)
	for rb.index >= len(rb.data) {
		rb.index -= len(rb.data)
	}
	if int(rb.end-rb.begin) > len(rb.data) {
		rb.begin = rb.end - int64(len(rb.data))
	}
}
