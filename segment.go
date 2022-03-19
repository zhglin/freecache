package freecache

import (
	"errors"
	"sync/atomic"
	"unsafe"
)

const HASH_ENTRY_SIZE = 16
const ENTRY_HDR_SIZE = 24

var ErrLargeKey = errors.New("The key is larger than 65535")
var ErrLargeEntry = errors.New("The entry size is larger than 1/1024 of cache size")
var ErrNotFound = errors.New("Entry not found")

// entry pointer struct points to an entry in ring buffer
// 入口指针结构体指向循环缓冲区中的一个入口
type entryPtr struct {
	offset   int64  // entry offset in ring buffer  在ringBuf中的偏移量
	hash16   uint16 // entries are ordered by hash16 in a slot.
	keyLen   uint16 // used to compare a key
	reserved uint32
}

// entry header struct in ring buffer, followed by key and value.
// 循环缓冲区中的入口头结构体，后跟键和值。总长24个字节
type entryHdr struct {
	accessTime uint32 // 最后访问时间    4
	expireAt   uint32 // 过期时间，绝对时间 4
	keyLen     uint16 // key的长度 2
	hash16     uint16 // key hash值的低16位 2
	valLen     uint32 // value的长度 4
	valCap     uint32 // value的容量 4
	deleted    bool   // 被删除的标记 1
	slotId     uint8  // 在segment.slotData中的下标1
	reserved   uint16 // 2
}

// a segment contains 256 slots, a slot is an array of entry pointers ordered by hash16 value
// the entry can be looked up by hash value of the key.
// 包含256个槽位，一个槽位是一个条目的数组，指针按hash16值排序，条目可以根据键的哈希值查找。
// 一个槽位包含2个指针元素(rb,slotsData)，整个cache包含256*2的指针
type segment struct {
	// 存储数据的环形队列
	rb        RingBuf // ring buffer that stores data 存储数据的buf
	segId     int     // id
	_         uint32
	missCount int64 // 未命中的计数
	// 已命中的计数
	hitCount   int64
	entryCount int64 // slotsData中已记录的key总数
	// RingBuffer中entry的总数，包括过期和标记删除的entry
	totalCount int64 // number of entries in ring buffer, including deleted entries.
	// 用于计算最近使用最少的条目。 RingBuffer中每个entry最近一次访问的时间戳总和，删除元素会减
	totalTime int64 // used to calculate least recent used entry.
	timer     Timer // Timer giving current time 给出当前时间的计时器
	// 环头置换到环未的次数
	totalEvacuate int64 // used for debug
	// 过期的元素数量
	totalExpired int64 // used for debug
	// key覆盖重写的key次数
	overwrites int64 // used for debug
	// 重置过期时间key次数
	touched int64 // used for debug
	// 初始值是seg.rb的长度，记录RingBuf中的剩余空间
	vacuumLen int64      // up to vacuumLen, new data can be written without overwriting old data.在vacuumLen之前，可以在不覆盖旧数据的情况下写入新数据。
	slotLens  [256]int32 // The actual length for every slot. 每个槽的实际长度。
	slotCap   int32      // max number of entry pointers a slot can hold. 最大入口数一个槽可容纳的指针数。
	slotsData []entryPtr // shared by all 256 slots hash值的低八位
	// 元素在环形数组中的索引信息，记录在rb中的起始位置
	// slotData hash值的低8位，最大256个entry，超过256个就会进行扩容，以2的幂次方进行扩容
	// 例如库扩容前下标[0,1,2,3]2倍扩容后下标为[0,0,1,1,2,2,3,3]
	// slotCap记录的就是slotsData每次扩容后最初256个元素中每个下标能存几个entry
	// slotLens代表256个下标中，每个下标已经存在的元素数
}

// 初始化segment存储
func newSegment(bufSize int, segId int, timer Timer) (seg segment) {
	seg.rb = NewRingBuf(bufSize, 0)
	seg.segId = segId
	seg.timer = timer
	seg.vacuumLen = int64(bufSize)
	seg.slotCap = 1
	seg.slotsData = make([]entryPtr, 256*seg.slotCap)
	return
}

// 写入kv到缓存
func (seg *segment) set(key, value []byte, hashVal uint64, expireSeconds int) (err error) {
	// 校验key长度
	if len(key) > 65535 {
		return ErrLargeKey
	}
	// 校验key，value总长度
	maxKeyValLen := len(seg.rb.data)/4 - ENTRY_HDR_SIZE
	if len(key)+len(value) > maxKeyValLen {
		// Do not accept large entry. 不要接受大的条目。
		return ErrLargeEntry
	}

	// 计算过期时间
	now := seg.timer.Now()
	expireAt := uint32(0)
	if expireSeconds > 0 {
		expireAt = now + uint32(expireSeconds)
	}

	// 查看key是否已存在
	slotId := uint8(hashVal >> 8)
	hash16 := uint16(hashVal >> 16)
	slot := seg.getSlot(slotId)
	idx, match := seg.lookup(slot, hash16, key)

	// key已存在
	var hdrBuf [ENTRY_HDR_SIZE]byte
	hdr := (*entryHdr)(unsafe.Pointer(&hdrBuf[0]))
	if match {
		matchedPtr := &slot[idx]
		seg.rb.ReadAt(hdrBuf[:], matchedPtr.offset)
		hdr.slotId = slotId
		hdr.hash16 = hash16
		hdr.keyLen = uint16(len(key))
		originAccessTime := hdr.accessTime
		hdr.accessTime = now
		hdr.expireAt = expireAt
		hdr.valLen = uint32(len(value))
		// value长度在减少 直接覆盖写入
		if hdr.valCap >= hdr.valLen {
			//in place overwrite
			atomic.AddInt64(&seg.totalTime, int64(hdr.accessTime)-int64(originAccessTime))
			seg.rb.WriteAt(hdrBuf[:], matchedPtr.offset)                              // 写入头数据
			seg.rb.WriteAt(value, matchedPtr.offset+ENTRY_HDR_SIZE+int64(hdr.keyLen)) // 因为是覆盖，不用再写key了
			atomic.AddInt64(&seg.overwrites, 1)
			return
		}
		// avoid unnecessary memory copy. 避免不必要的内存拷贝。
		// 长度增加，内存不够，删除重写
		seg.delEntryPtr(slotId, slot, idx)
		match = false
		// increase capacity and limit entry len.
		// 增加容量和限制输入len。
		for hdr.valCap < hdr.valLen {
			hdr.valCap *= 2
		}
		if hdr.valCap > uint32(maxKeyValLen-len(key)) {
			hdr.valCap = uint32(maxKeyValLen - len(key))
		}
	} else {
		hdr.slotId = slotId
		hdr.hash16 = hash16
		hdr.keyLen = uint16(len(key))
		hdr.accessTime = now
		hdr.expireAt = expireAt
		hdr.valLen = uint32(len(value))
		hdr.valCap = uint32(len(value))
		if hdr.valCap == 0 { // avoid infinite loop when increasing capacity. 增加容量时避免无限循环。
			hdr.valCap = 1 // 保证上面的*2的扩容不会一直为0
		}
	}

	// 需要的长度
	entryLen := ENTRY_HDR_SIZE + int64(len(key)) + int64(hdr.valCap)
	// ringBuf空出entryLen的长度
	slotModified := seg.evacuate(entryLen, slotId, now)
	if slotModified {
		// the slot has been modified during evacuation, we need to looked up for the 'idx' again.
		// otherwise there would be index out of bound error.
		// 在扩容过程中插槽被修改，我们需要再次查找'idx'。否则会出现索引越界错误。
		slot = seg.getSlot(slotId)
		idx, match = seg.lookup(slot, hash16, key)
		// assert(match == false)
	}
	// 添加slot数据
	newOff := seg.rb.End()
	seg.insertEntryPtr(slotId, hash16, newOff, idx, hdr.keyLen) // 写入索引
	// 写入ringbuf
	seg.rb.Write(hdrBuf[:])                     // [:]从头切到尾，等价于复制整个SLICE
	seg.rb.Write(key)                           // 写入key
	seg.rb.Write(value)                         // 写入value
	seg.rb.Skip(int64(hdr.valCap - hdr.valLen)) // 写入冗余长度，因为rb的写是value的实际长度不包含冗余的空间
	atomic.AddInt64(&seg.totalTime, int64(now))
	atomic.AddInt64(&seg.totalCount, 1)
	seg.vacuumLen -= entryLen // 记录rb的剩余长度
	return
}

// 更新key的过期时间
func (seg *segment) touch(key []byte, hashVal uint64, expireSeconds int) (err error) {
	if len(key) > 65535 {
		return ErrLargeKey
	}

	// 查找key，未查到直接返回
	slotId := uint8(hashVal >> 8)
	hash16 := uint16(hashVal >> 16)
	slot := seg.getSlot(slotId)
	idx, match := seg.lookup(slot, hash16, key)
	if !match {
		err = ErrNotFound
		return
	}

	// 从rb中读取头数据
	matchedPtr := &slot[idx]
	var hdrBuf [ENTRY_HDR_SIZE]byte
	seg.rb.ReadAt(hdrBuf[:], matchedPtr.offset)
	hdr := (*entryHdr)(unsafe.Pointer(&hdrBuf[0]))

	// 校验当前元素是否已过期，过期直接删除返回
	now := seg.timer.Now()
	if hdr.expireAt != 0 && hdr.expireAt <= now {
		seg.delEntryPtr(slotId, slot, idx)
		atomic.AddInt64(&seg.totalExpired, 1)
		err = ErrNotFound
		atomic.AddInt64(&seg.missCount, 1)
		return
	}

	// 重新计算过期时间
	expireAt := uint32(0)
	if expireSeconds > 0 {
		expireAt = now + uint32(expireSeconds)
	}

	// 设置并写入
	originAccessTime := hdr.accessTime
	hdr.accessTime = now
	hdr.expireAt = expireAt
	//in place overwrite
	atomic.AddInt64(&seg.totalTime, int64(hdr.accessTime)-int64(originAccessTime))
	seg.rb.WriteAt(hdrBuf[:], matchedPtr.offset)
	atomic.AddInt64(&seg.touched, 1)
	return
}

// 校验并空出ringBuf中entryLen的长度空间，空间不足就想办法删掉元素
func (seg *segment) evacuate(entryLen int64, slotId uint8, now uint32) (slotModified bool) {
	var oldHdrBuf [ENTRY_HDR_SIZE]byte
	consecutiveEvacuate := 0
	for seg.vacuumLen < entryLen { // 说明ringBuf空间不足,写不进去了
		oldOff := seg.rb.End() + seg.vacuumLen - seg.rb.Size() // 标记最早记录的起始位置
		seg.rb.ReadAt(oldHdrBuf[:], oldOff)                    // 读取头数据
		oldHdr := (*entryHdr)(unsafe.Pointer(&oldHdrBuf[0]))
		oldEntryLen := ENTRY_HDR_SIZE + int64(oldHdr.keyLen) + int64(oldHdr.valCap)
		// 已经被标记删除了
		if oldHdr.deleted {
			consecutiveEvacuate = 0
			atomic.AddInt64(&seg.totalTime, -int64(oldHdr.accessTime))
			atomic.AddInt64(&seg.totalCount, -1)
			seg.vacuumLen += oldEntryLen // 直接增加剩余空间
			continue
		}

		// 是否过期
		expired := oldHdr.expireAt != 0 && oldHdr.expireAt < now
		// int64(oldHdr.accessTime) <= atomic.LoadInt64(&seg.totalTime)/atomic.LoadInt64(&seg.totalCount)
		// 表示RingBuf中的entry最近一次访问时间戳的平均值，当一个entry的accessTime小于等于这个平均值，则认为这个entry是可以被置换掉的。
		leastRecentUsed := int64(oldHdr.accessTime)*atomic.LoadInt64(&seg.totalCount) <= atomic.LoadInt64(&seg.totalTime)
		// 如果未过期，未满足置换，超过五次后就直接删掉当前元素
		if expired || leastRecentUsed || consecutiveEvacuate > 5 {
			seg.delEntryPtrByOffset(oldHdr.slotId, oldHdr.hash16, oldOff)
			if oldHdr.slotId == slotId {
				slotModified = true
			}
			consecutiveEvacuate = 0
			atomic.AddInt64(&seg.totalTime, -int64(oldHdr.accessTime))
			atomic.AddInt64(&seg.totalCount, -1)
			seg.vacuumLen += oldEntryLen
			if expired {
				atomic.AddInt64(&seg.totalExpired, 1)
			} else {
				atomic.AddInt64(&seg.totalEvacuate, 1)
			}
		} else {
			// 当前元素未过期也不满足置换就移到rb的未部
			// evacuate an old entry that has been accessed recently for better cache hit rate.
			// 清除最近被访问过的旧入口，以获得更好的缓存命中率。
			newOff := seg.rb.Evacuate(oldOff, int(oldEntryLen))
			// off变动需要更新slot中的索引
			seg.updateEntryPtr(oldHdr.slotId, oldHdr.hash16, oldOff, newOff)
			consecutiveEvacuate++
			atomic.AddInt64(&seg.totalEvacuate, 1)
		}
	}
	return
}

// 获取key对应的value
func (seg *segment) get(key, buf []byte, hashVal uint64, peek bool) (value []byte, expireAt uint32, err error) {
	hdr, ptr, err := seg.locate(key, hashVal, peek)
	if err != nil {
		return
	}
	expireAt = hdr.expireAt

	// 构建返回的数据空间
	if cap(buf) >= int(hdr.valLen) {
		value = buf[:hdr.valLen]
	} else {
		value = make([]byte, hdr.valLen)
	}

	// 读取的时候跳过头信息+key信息
	seg.rb.ReadAt(value, ptr.offset+ENTRY_HDR_SIZE+int64(hdr.keyLen))
	if !peek {
		atomic.AddInt64(&seg.hitCount, 1)
	}
	return
}

// view provides zero-copy access to the element's value, without copying to
// an intermediate buffer.
// 提供对元素值的零拷贝访问，而无需复制到中间缓冲区。
func (seg *segment) view(key []byte, fn func([]byte) error, hashVal uint64, peek bool) (err error) {
	hdr, ptr, err := seg.locate(key, hashVal, peek)
	if err != nil {
		return
	}
	start := ptr.offset + ENTRY_HDR_SIZE + int64(hdr.keyLen)
	val, err := seg.rb.Slice(start, int64(hdr.valLen))
	if err != nil {
		return err
	}
	err = fn(val)
	if !peek {
		atomic.AddInt64(&seg.hitCount, 1)
	}
	return
}

// 返回key对应的信息
func (seg *segment) locate(key []byte, hashVal uint64, peek bool) (hdr *entryHdr, ptr *entryPtr, err error) {
	// 查找
	slotId := uint8(hashVal >> 8)
	hash16 := uint16(hashVal >> 16)
	slot := seg.getSlot(slotId)
	idx, match := seg.lookup(slot, hash16, key)

	// 未查到
	if !match {
		err = ErrNotFound
		if !peek {
			atomic.AddInt64(&seg.missCount, 1) // 增加未命中的计数
		}
		return
	}
	ptr = &slot[idx]

	var hdrBuf [ENTRY_HDR_SIZE]byte
	seg.rb.ReadAt(hdrBuf[:], ptr.offset)
	hdr = (*entryHdr)(unsafe.Pointer(&hdrBuf[0]))
	// 检查是否过期
	if !peek {
		// 过期删除 返回notFound
		now := seg.timer.Now()
		if hdr.expireAt != 0 && hdr.expireAt <= now {
			seg.delEntryPtr(slotId, slot, idx)
			atomic.AddInt64(&seg.totalExpired, 1)
			err = ErrNotFound
			atomic.AddInt64(&seg.missCount, 1)
			return
		}

		// 更新访问时间
		atomic.AddInt64(&seg.totalTime, int64(now-hdr.accessTime))
		hdr.accessTime = now
		seg.rb.WriteAt(hdrBuf[:], ptr.offset)
	}
	return hdr, ptr, err
}

// 删除元素
func (seg *segment) del(key []byte, hashVal uint64) (affected bool) {
	slotId := uint8(hashVal >> 8)
	hash16 := uint16(hashVal >> 16)
	slot := seg.getSlot(slotId)
	idx, match := seg.lookup(slot, hash16, key)
	if !match {
		return false
	}
	seg.delEntryPtr(slotId, slot, idx)
	return true
}

// 返回剩余的过期时间
func (seg *segment) ttl(key []byte, hashVal uint64) (timeLeft uint32, err error) {
	slotId := uint8(hashVal >> 8)
	hash16 := uint16(hashVal >> 16)
	slot := seg.getSlot(slotId)
	idx, match := seg.lookup(slot, hash16, key)
	if !match {
		err = ErrNotFound
		return
	}
	ptr := &slot[idx]
	now := seg.timer.Now()

	var hdrBuf [ENTRY_HDR_SIZE]byte
	seg.rb.ReadAt(hdrBuf[:], ptr.offset)
	hdr := (*entryHdr)(unsafe.Pointer(&hdrBuf[0]))

	// 已过期返回0，并且不进行数据删除的操作
	if hdr.expireAt == 0 {
		timeLeft = 0
		return
	} else if hdr.expireAt != 0 && hdr.expireAt >= now {
		timeLeft = hdr.expireAt - now
		return
	}
	err = ErrNotFound
	return
}

// 对slot进行扩容
func (seg *segment) expand() {
	newSlotData := make([]entryPtr, seg.slotCap*2*256)
	for i := 0; i < 256; i++ {
		off := int32(i) * seg.slotCap
		copy(newSlotData[off*2:], seg.slotsData[off:off+seg.slotLens[i]]) // off*2是因为seg.slotCap还没*2
	}
	seg.slotCap *= 2
	seg.slotsData = newSlotData
}

// 更新slot中的索引offset中的值
func (seg *segment) updateEntryPtr(slotId uint8, hash16 uint16, oldOff, newOff int64) {
	slot := seg.getSlot(slotId)
	idx, match := seg.lookupByOff(slot, hash16, oldOff)
	if !match {
		return
	}
	ptr := &slot[idx]
	ptr.offset = newOff
}

// 添加slot
func (seg *segment) insertEntryPtr(slotId uint8, hash16 uint16, offset int64, idx int, keyLen uint16) {
	// slot已满 进行扩容
	if seg.slotLens[slotId] == seg.slotCap {
		seg.expand() // 扩容
	}
	seg.slotLens[slotId]++              // 记录当前slot数量
	atomic.AddInt64(&seg.entryCount, 1) // 增加计数
	slot := seg.getSlot(slotId)
	// idx后移 空出第idx的位置
	copy(slot[idx+1:], slot[idx:])
	slot[idx].offset = offset // 偏移量
	slot[idx].hash16 = hash16
	slot[idx].keyLen = keyLen
}

// 匹配成功后删除元素
func (seg *segment) delEntryPtrByOffset(slotId uint8, hash16 uint16, offset int64) {
	slot := seg.getSlot(slotId)
	idx, match := seg.lookupByOff(slot, hash16, offset)
	if !match {
		return
	}
	seg.delEntryPtr(slotId, slot, idx)
}

// 删除元素 删除slot中的索引 rb中标记删除
func (seg *segment) delEntryPtr(slotId uint8, slot []entryPtr, idx int) {
	offset := slot[idx].offset
	var entryHdrBuf [ENTRY_HDR_SIZE]byte
	seg.rb.ReadAt(entryHdrBuf[:], offset) // 从offset读取ENTRY_HDR_SIZE个字节
	entryHdr := (*entryHdr)(unsafe.Pointer(&entryHdrBuf[0]))
	entryHdr.deleted = true                // 标记删除
	seg.rb.WriteAt(entryHdrBuf[:], offset) // 从offset写入ENTRY_HDR_SIZE个字节
	copy(slot[idx:], slot[idx+1:])         // 在slot中删除slot[idx]
	// 计数变更
	seg.slotLens[slotId]--
	atomic.AddInt64(&seg.entryCount, -1)
}

// 二分查找，找到第一个entryPtr.hash16 >= hash16的entry索引下标
func entryPtrIdx(slot []entryPtr, hash16 uint16) (idx int) {
	high := len(slot) // 长度
	for idx < high {
		mid := (idx + high) >> 1
		oldEntry := &slot[mid] // 不会出现nil指针
		if oldEntry.hash16 < hash16 {
			idx = mid + 1
		} else {
			high = mid
		}
	}
	return
}

// 查看key是否在[]entryPtr中
func (seg *segment) lookup(slot []entryPtr, hash16 uint16, key []byte) (idx int, match bool) {
	idx = entryPtrIdx(slot, hash16) // 大于等于hash16的起始下标
	for idx < len(slot) {
		ptr := &slot[idx]
		// 第一个大于等于的下标hash16不匹配
		if ptr.hash16 != hash16 {
			break
		}
		// 长度相等并且key也相等
		match = int(ptr.keyLen) == len(key) && seg.rb.EqualAt(key, ptr.offset+ENTRY_HDR_SIZE)
		if match {
			return
		}
		idx++
	}
	return
}

// 根据元素的hash16以及offset位置进行元素的匹配
func (seg *segment) lookupByOff(slot []entryPtr, hash16 uint16, offset int64) (idx int, match bool) {
	idx = entryPtrIdx(slot, hash16)
	for idx < len(slot) {
		ptr := &slot[idx]
		if ptr.hash16 != hash16 {
			break
		}
		match = ptr.offset == offset
		if match {
			return
		}
		idx++
	}
	return
}

func (seg *segment) resetStatistics() {
	atomic.StoreInt64(&seg.totalEvacuate, 0)
	atomic.StoreInt64(&seg.totalExpired, 0)
	atomic.StoreInt64(&seg.overwrites, 0)
	atomic.StoreInt64(&seg.hitCount, 0)
	atomic.StoreInt64(&seg.missCount, 0)
}

func (seg *segment) clear() {
	bufSize := len(seg.rb.data)
	seg.rb.Reset(0)
	seg.vacuumLen = int64(bufSize)
	seg.slotCap = 1
	seg.slotsData = make([]entryPtr, 256*seg.slotCap)
	for i := 0; i < len(seg.slotLens); i++ {
		seg.slotLens[i] = 0
	}

	atomic.StoreInt64(&seg.hitCount, 0)
	atomic.StoreInt64(&seg.missCount, 0)
	atomic.StoreInt64(&seg.entryCount, 0)
	atomic.StoreInt64(&seg.totalCount, 0)
	atomic.StoreInt64(&seg.totalTime, 0)
	atomic.StoreInt64(&seg.totalEvacuate, 0)
	atomic.StoreInt64(&seg.totalExpired, 0)
	atomic.StoreInt64(&seg.overwrites, 0)
}

// 返回slotId在seg.slotsData中的切片
func (seg *segment) getSlot(slotId uint8) []entryPtr {
	slotOff := int32(slotId) * seg.slotCap // 下标起始位置
	// s[i:l:c] i 是起始偏移的起始位置，l 是起始偏移的长度结束位置， l-i 就是新 slice 的长度， c 是起始偏移的容量结束位置，c-i 就是新 slice 的容量。
	return seg.slotsData[slotOff : slotOff+seg.slotLens[slotId] : slotOff+seg.slotCap]
}
