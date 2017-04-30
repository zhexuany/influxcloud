package hh

import (
	"encoding/binary"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"sync"
	"time"

	"github.com/uber-go/zap"
)

// Possible errors returned by a hinted handoff queue.
var (
	ErrNotOpen     = fmt.Errorf("queue not open")
	ErrQueueFull   = fmt.Errorf("queue is full")
	ErrSegmentFull = fmt.Errorf("segment is full")
)

const (
	defaultSegmentSize = 10 * 1024 * 1024
	footerSize         = 8
)

// queue is a bounded, disk-backed, append-only type that combines queue and
// log semantics.  byte slices can be appended and read back in-order.
// The queue maintains a pointer to the current head
// byte slice and can re-read from the head until it has been advanced.
//
// Internally, the queue writes byte slices to multiple segment files so
// that disk space can be reclaimed. When a segment file is larger than
// the max segment size, a new file is created.   Segments are removed
// after their head pointer has advanced past the last entry.  The first
// segment is the head, and the last segment is the tail.  Reads are from
// the head segment and writes tail segment.
//
// queues can have a max size configured such that when the size of all
// segments on disk exceeds the size, write will fail.
//
// ┌─────┐
// │Head │
// ├─────┘
// │
// ▼
// ┌─────────────────┐ ┌─────────────────┐┌─────────────────┐
// │Segment 1 - 10MB │ │Segment 2 - 10MB ││Segment 3 - 10MB │
// └─────────────────┘ └─────────────────┘└─────────────────┘
//                                                          ▲
//                                                          │
//                                                          │
//                                                     ┌─────┐
//                                                     │Tail │
//                                                     └─────┘
type queue struct {
	mu sync.RWMutex

	// Directory to create segments
	dir string

	// The head and tail segments.  Reads are from the beginning of head,
	// writes are appended to the tail.
	head, tail *segment

	// The maximum size in bytes of a segment file before a new one should be created
	maxSegmentSize int64

	// The maximum size allowed in bytes of all segments before writes will return
	// an error
	maxSize int64

	// The segments that exist on disk
	segments segments

	// Logger print userful logs
	Logger zap.Logger
}

type queuePos struct {
	head string
	tail string
}

type segments []*segment

// make segments can be sorted
func (s segments) Len() int           { return len(s) }
func (s segments) Swap(i, j int)      { s[i], s[j] = s[j], s[i] }
func (s segments) Less(i, j int) bool { return s[i].segmentID > s[j].segmentID } // segments sorted according to its segmentID

// newQueue create a queue that will store segments in dir and that will
// consume more than maxSize on disk.
func newQueue(dir string, maxSize int64) (*queue, error) {
	q := &queue{
		dir:            dir,
		maxSegmentSize: defaultSegmentSize,
		maxSize:        maxSize,
		segments:       segments{},
	}
	return q, nil
}

// WithLogger sets the internal logger to the logger passed in
func (l *queue) WithLogger(log zap.Logger) {
	l.Logger = log.With(zap.String("service", "cluster"))
}

// Open opens the queue for reading and writing
func (l *queue) Open() error {
	l.mu.Lock()
	defer l.mu.Unlock()

	segments, err := l.loadSegments()
	if err != nil {
		return err
	}
	l.segments = segments

	if len(l.segments) == 0 {
		if err := l.addSegment(); err != nil {
			return err
		}
	}

	l.head = l.segments[0]
	l.tail = l.segments[len(l.segments)-1]

	// If the head has been fully advanced and the segment size is modified,
	// existing segments an get stuck and never allow clients to advance further.
	// This advances the segment if the current head is already at the end.
	_, err = l.head.current()
	if err == io.EOF {
		return l.trimHead()
	}

	return nil
}

// Close stops the queue for reading and writing
func (l *queue) Close() error {
	l.mu.Lock()
	defer l.mu.Unlock()

	for _, s := range l.segments {
		if err := s.close(); err != nil {
			return err
		}
	}
	l.head = nil
	l.tail = nil
	l.segments = nil
	return nil
}

// Remove removes all underlying file-based resources for the queue.
// It is an error to call this on an open queue.
func (l *queue) Remove() error {
	l.mu.Lock()
	defer l.mu.Unlock()

	if l.head != nil || l.tail != nil || l.segments != nil {
		return fmt.Errorf("queue is open")
	}

	return os.RemoveAll(l.dir)
}

// SetMaxSegmentSize updates the max segment size for new and existing
// segments.
func (l *queue) SetMaxSegmentSize(size int64) error {
	l.mu.Lock()
	defer l.mu.Unlock()

	l.maxSegmentSize = size

	for _, s := range l.segments {
		s.SetMaxSegmentSize(size)
	}

	if l.tail.diskUsage() >= l.maxSegmentSize {
		if err := l.addSegment(); err != nil {
			return err
		}
	}
	return nil
}

func (l *queue) PurgeOlderThan(when time.Time) error {
	l.mu.Lock()
	defer l.mu.Unlock()

	if len(l.segments) == 0 {
		return nil
	}

	cutoff := when.Truncate(time.Second)
	for {
		mod, err := l.head.lastModified()
		if err != nil {
			return err
		}

		if mod.After(cutoff) || mod.Equal(cutoff) {
			return nil
		}

		// If this is the last segment, first append a new one allowing
		// trimming to proceed.
		if len(l.segments) == 1 {
			if err := l.addSegment(); err != nil {
				return err
			}
		}

		if err := l.trimHead(); err != nil {
			return err
		}
	}
}

// LastModified returns the last time the queue was modified.
func (l *queue) LastModified() (time.Time, error) {
	l.mu.RLock()
	defer l.mu.RUnlock()

	if l.tail != nil {
		return l.tail.lastModified()
	}
	return time.Time{}.UTC(), nil
}

func (l *queue) Position() (*queuePos, error) {
	l.mu.RLock()
	defer l.mu.RUnlock()

	qp := &queuePos{}
	if l.head != nil {
		qp.head = fmt.Sprintf("%s:%d", l.head.path, l.head.pos)
	}
	if l.tail != nil {
		qp.tail = fmt.Sprintf("%s:%d", l.tail.path, l.tail.filePos())
	}
	return qp, nil
}

// Empty will return true if queue's head is nil
func (l *queue) Empty() bool {
	l.mu.RLock()
	empty := l.head.empty()
	l.mu.RUnlock()

	return empty
}

func (l *queue) TotalBytes() int64 {
	var totalB int64
	for _, seg := range l.segments {
		totalB += seg.totalBytes()
	}
	return totalB
}

// diskUsage returns the total size on disk used by the queue
func (l *queue) diskUsage() int64 {
	var size int64
	for _, s := range l.segments {
		size += s.diskUsage()
	}
	return size
}

// addSegment creates a new empty segment file
func (l *queue) addSegment() error {
	nextID, err := l.nextSegmentID()
	if err != nil {
		return err
	}

	segment, err := newSegment(filepath.Join(l.dir, strconv.FormatUint(nextID, 10)), l.maxSegmentSize)
	if err != nil {
		return err
	}

	l.segments = append(l.segments, segment)
	l.tail = segment
	return nil
}

// loadSegments loads all segments on disk
func (l *queue) loadSegments() (segments, error) {
	segments := segments{}

	files, err := ioutil.ReadDir(l.dir)
	if err != nil {
		return segments, err
	}

	for _, segment := range files {
		// Segments should be files.  Skip anything that is not a dir.
		if segment.IsDir() {
			continue
		}

		// Segments file names are all numeric
		_, err := strconv.ParseUint(segment.Name(), 10, 64)
		if err != nil {
			continue
		}

		path := filepath.Join(l.dir, segment.Name())
		l.Logger.Info("creating segement: " + path)
		segment, err := newSegment(path, l.maxSegmentSize)
		if err != nil {
			return segments, err
		}

		// check segment is empty or not. if it is empty, it has to be
		// remove from current directory
		if segment.empty() {
			if err := segment.close(); err != nil {
				return segments, err
			}
			if err := os.Remove(path); err != nil {
				return segments, err
			}
		}

		// segment is not empty and it can be append into segments safely
		segments = append(segments, segment)
	}
	sort.Sort(segments)

	return segments, nil
}

// nextSegmentID returns the next segment ID that is free
func (l *queue) nextSegmentID() (uint64, error) {
	segments, err := ioutil.ReadDir(l.dir)
	if err != nil {
		return 0, err
	}

	var maxID uint64
	for _, segment := range segments {
		// Segments should be files.  Skip anything that is not a dir.
		if segment.IsDir() {
			continue
		}

		// Segments file names are all numeric
		segmentID, err := strconv.ParseUint(segment.Name(), 10, 64)
		if err != nil {
			continue
		}

		if segmentID > maxID {
			maxID = segmentID
		}
	}

	return maxID + 1, nil
}

// totalSegments will return the number of segments.
// if segments is nil
func (l *queue) totalSegments() int64 {
	l.mu.RLock()
	defer l.mu.RUnlock()
	num := len(l.segments)

	// check head is empty or not if num is less than or equal 1
	if num <= 1 && l.head.empty() {
		return 0
	}
	return int64(num)
}

// Append appends a byte slice to the end of the queue
func (l *queue) Append(b []byte) error {
	l.mu.Lock()
	defer l.mu.Unlock()

	if l.tail == nil {
		return ErrNotOpen
	}

	if l.diskUsage()+int64(len(b)) > l.maxSize {
		return ErrQueueFull
	}

	// Append the entry to the tail, if the segment is full,
	// try to create new segment and retry the append
	err := l.tail.append(b)
	if err == ErrSegmentFull {
		if err := l.addSegment(); err != nil {
			return err
		}
		return l.tail.append(b)
	}
	return nil
}

// Current returns the current byte slice at the head of the queue
func (l *queue) Current() ([]byte, error) {
	l.mu.RLock()
	defer l.mu.RUnlock()
	if l.head == nil {
		return nil, ErrNotOpen
	}

	return l.head.current()
}

// PeekN returns the next n bytes without advancing the head segement
func (l *queue) PeekN(n int64) ([]byte, error) {
	l.mu.RLock()
	defer l.mu.RUnlock()

	if l.head == nil {
		return nil, ErrNotOpen
	}

	return l.head.peek(n)
}

// Advance moves the head point to the next byte slice in the queue
func (l *queue) Advance() error {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.head == nil {
		return ErrNotOpen
	}

	err := l.head.advance()
	if err == io.EOF {
		if err := l.trimHead(); err != nil {
			return err
		}
	}

	return nil
}

func (l *queue) trimHead() error {
	// if head is the only segment and is already full in the queue
	// before triming current head, a new head need be created
	if len(l.segments) == 1 && l.head.full() {
		if err := l.addSegment(); err != nil {
			return err
		}
	}

	if len(l.segments) > 1 {
		l.segments = l.segments[1:]

		if err := l.head.close(); err != nil {
			return err
		}
		if err := os.Remove(l.head.path); err != nil {
			return err
		}
		l.head = l.segments[0]
	}
	return nil
}

// Segment is a queue using a single file.  The structure of a segment is a series
// lengths + block with a single footer point to the position in the segment of the
// current head block.
//
// ┌──────────────────────────┐ ┌──────────────────────────┐ ┌────────────┐
// │         Block 1          │ │         Block 2          │ │   Footer   │
// └──────────────────────────┘ └──────────────────────────┘ └────────────┘
// ┌────────────┐┌────────────┐ ┌────────────┐┌────────────┐ ┌────────────┐
// │Block 1 Len ││Block 1 Body│ │Block 2 Len ││Block 2 Body│ │Head Offset │
// │  8 bytes   ││  N bytes   │ │  8 bytes   ││  N bytes   │ │  8 bytes   │
// └────────────┘└────────────┘ └────────────┘└────────────┘ └────────────┘
//
// The footer holds the pointer to the head entry at the end of the segment to allow writes
// to eek to the end and write sequentially (vs having to seek back to the beginning of
// the segment to update the head pointer).  Reads must seek to the end then back into the
// segment offset stored in the footer.
//
// Segments store arbitrary byte slices and leave the serialization to the caller.  Segments
// are created with a max size and will block writes when the segment is full.
type segment struct {
	mu sync.RWMutex

	size int64
	file *os.File
	path string

	pos         int64
	currentSize int64
	maxSize     int64

	segmentID uint64
}

var mutex sync.RWMutex

func newSegment(path string, maxSize int64) (*segment, error) {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0600)
	if err != nil {
		return nil, err
	}

	id, err := strconv.ParseUint(filepath.Base(path), 10, 64)
	if err != nil {
		return nil, err
	}

	stats, err := os.Stat(path)
	if err != nil {
		return nil, err
	}

	// check whether file size is less than maxSize passed as parameter
	var size int64
	if maxSize >= stats.Size() {
		size = stats.Size()
	} else {
		// if maxSize is below than the actual file size, this usualy means
		// a file should be truncated or unterminated to get rid of extra
		// contents. A repair operation need to be done on the way of
		// opening this segment. After repair, the file's size should agree
		// with maxSize
		size = maxSize
	}
	s := &segment{file: f, path: path, size: size, maxSize: maxSize, segmentID: id}

	// after segment creation, open it with mutex protection
	mutex.Lock()
	defer mutex.Unlock()
	if err := s.open(); err != nil {
		return nil, err
	}

	return s, nil
}

func (l *segment) open() error {
	// If it's a new segment then write the location of the current record in this segment
	if l.size == 0 {
		l.pos = 0
		l.currentSize = 0

		if err := l.writeUint64(uint64(l.pos)); err != nil {
			return err
		}

		if err := l.file.Sync(); err != nil {
			return err
		}

		l.size = footerSize

		return nil
	}
	// Existing segment so read the current position and the size of the current
	// block
	if err := l.seekEnd(-footerSize); err != nil {
		return err
	}

	pos, err := l.readUint64()
	if err != nil {
		return err
	}

	// check whether pos is above the difference between footerSize and l.szie,
	// if yes, then file need to be repaired.
	if int64(pos) > l.size-footerSize {
		if err := l.repair(); err != nil {
			return err
		}
	}

	// After file was repaired, we can try to read this segment again.
	// We need seek offset to current position and then try to read file,
	// If failed then just return such error, no need to repair again.
	l.pos = int64(pos)
	if err := l.seekToCurrent(); err != nil {
		return err
	}

	// after repair, if pos is still larger than the end of segment, just return
	if l.pos >= l.size-footerSize {
		return nil
	}

	// otherwise, read a uint64 in this segment
	currentSize, err := l.readUint64()
	if err != nil {
		return err
	}
	l.currentSize = int64(currentSize)

	// If currentSize is below or
	if l.currentSize > l.size-footerSize {
		if err := l.repair(); err != nil {
			return err
		}
		l.pos = 0
		return l.open()
	}

	//create a byte array with currentSize for reading
	buf := make([]byte, currentSize)
	if err := l.readBytes(buf); err != nil {
		if err := l.repair(); err != nil {
			return err
		}
		l.pos = 0
		return l.open()
	}

	// seek to l.pos
	if err := l.seek(l.pos + 8); err != nil {
		return err
	}

	// check whether open reached final block of segment
	if l.pos == l.size {
		// Since file already reached final block, this indicated all
		// block before final one is already repaired. It can be truncated.
		if err := l.file.Truncate(l.maxSize); err != nil {
			return err
		}

		if err := l.seek(l.pos); err != nil {
			return err
		}

		// After repaired all blocks, this segment need to update
		// footer at the end of file. Since this segment is just opened
		// footer should initializes as 0.
		if err := l.writeUint64(0); err != nil {
			return err
		}

		if err := l.file.Sync(); err != nil {
			return err
		}
		l.pos = 0

		// Last step: make sure all blocks are repaired.
		return l.open()
	}
	return nil
}

// full will return true if segment's size agree with maxSize
func (l *segment) full() bool {
	l.mu.RLock()
	defer l.mu.RUnlock()

	return l.size == l.maxSize
}

//
//
//
//
//
//
//
//
//
//
//
//
//
//
//
func (l *segment) repair() error {
	// starting to repair this segment. First seek to the beginning of file
	if err := l.seek(0); err != nil {
		return err
	}

	// The way of repairing is of reading from beginning until file reached the
	// maxSize according to the following rule:
	//  1. First eight bytes are the length of the contents of a block
	//  2. The last eight bytes of file is footer, this need update at the end of
	//     repairing process.
	// If file is malformed, all extra block must be truncated. In addition, the
	// last eight bytes must be reserved footer which store the header entry info.
	// start from the first block, block by block
LOOP:
	filevPos := l.filePos()
	// check wehter current pos reached the end of segement
	if filevPos != l.maxSize-footerSize {
		// ┌──────────────────────────┐
		// │         Block x          │
		// └──────────────────────────┘
		// ┌────────────┐┌────────────┐
		// │Block 1 Len ││Block 1 Body│
		// │  8 bytes   ││  N bytes   │
		// └────────────┘└────────────┘
		//               | currentSize |
		if currentSize, err := l.readUint64(); err != nil {
			// After retrieving currentSize, we need set offset to next
			// read offset relative to current location.
			// After seek operation, we need check the returned offset with
			// l.maxSize. If returned offset of current pos in file is larger
			// than l.maxSize, then this imdicates finsih of examining all blocks.
			// The way of checking we success
			if ret, _ := l.file.Seek(int64(currentSize), os.SEEK_CUR); ret < l.maxSize || l.filePos() < l.maxSize {
				// reach this condition branch imdicating exmine blocks is not
				// yet finished. Need jump to the beginning.
				goto LOOP
			}
		}
	} else if l.pos != l.maxSize-footerSize {
		// l.pos does not agree with the difference between l.maxSize and
		// footerSize. Extra contents in such segment need to be truncated.
		// New size must be less thant or equal to l.maxSize.
		if err := os.Truncate(l.path, l.maxSize); err != nil {
			return err
		}
	}

	// Everthing thing seems ready to go. This segment is already repaired.
	// But the footerSize has to be updated. Seek to the end and updaate footer.
	if err := l.seek(l.pos); err != nil {
		return err
	}

	if err := l.writeUint64(0); err != nil {
		return err
	}

	if err := l.file.Sync(); err != nil {
		return err
	}

	l.pos += 8
	return nil
}

// append adds byte slice to the end of segment
func (l *segment) append(b []byte) error {
	l.mu.Lock()
	defer l.mu.Unlock()

	if l.file == nil {
		return ErrNotOpen
	}

	if l.size+int64(len(b)) > l.maxSize {
		return ErrSegmentFull
	}

	if err := l.seekEnd(-footerSize); err != nil {
		return err
	}

	// First write will write the length of block
	// as 8 bytes into file. This can be used for
	// retrieving data from file.
	// Second write will write actual data into
	// file.
	// Third write will update footer. This is the
	// last eight bytes which record current read
	// position.
	if err := l.writeUint64(uint64(len(b))); err != nil {
		return err
	}

	if err := l.writeBytes(b); err != nil {
		return err
	}

	if err := l.writeUint64(uint64(l.pos)); err != nil {
		return err
	}

	if err := l.file.Sync(); err != nil {
		return err
	}

	if l.currentSize == 0 {
		l.currentSize = int64(len(b))
	}

	l.size += int64(len(b)) + 8 // uint64 for slice length

	return nil

}

// empty will return true if file has no content
func (l *segment) empty() bool {
	l.mu.RLock()
	defer l.mu.RUnlock()
	return l.size == 0
}

// current returns byte slice that the current segment points
func (l *segment) current() ([]byte, error) {
	l.mu.Lock()
	defer l.mu.Unlock()

	if int64(l.pos) == l.size-footerSize {
		return nil, io.EOF
	}

	if err := l.seekToCurrent(); err != nil {
		return nil, err
	}

	// read the record size
	sz, err := l.readUint64()
	if err != nil {
		return nil, err
	}
	l.currentSize = int64(sz)

	if int64(sz) > l.maxSize {
		return nil, fmt.Errorf("record size out of range: max %d: got %d", l.maxSize, sz)
	}

	b := make([]byte, sz)
	if err := l.readBytes(b); err != nil {
		return nil, err
	}

	return b, nil
}

// TODO zhexuany finished this when alpha is ready
// peek returns the next n blocks in segement without advancing the pos.
func (l *segment) peek(n int64) ([]byte, error) {
	l.mu.Lock()
	defer l.mu.Unlock()

	if l.size == 0 {
		return nil, nil
	}

	if err := l.seekToCurrent(); err != nil {
		return nil, err
	}

	var buf []byte

	for i := 0; i < int(n); i++ {
		if l.pos == l.size-footerSize {
			return nil, io.EOF
		}

		// read the record size
		currentSize, err := l.readUint64()
		if err == io.EOF {
			return nil, err
		} else if err != nil {
			return buf, err
		}
		l.currentSize = int64(currentSize)
		// check currentSize
		if l.currentSize > 0 {
			// need check currentSize against maxSize.
			// In some case, file is malformed and
			// currentSize is not correct.
			if int64(currentSize) > l.maxSize {
				return nil, fmt.Errorf("record size out of range: max %d: got %d", l.maxSize, currentSize)
			}
		}

		// prepare bytes array for reading
		b := make([]byte, currentSize)
		if err := l.readBytes(b); err != nil {
			return nil, err
		}
		buf = append(buf, b...)
	}
	return buf, nil
}

// advance advances the current value pointer
func (l *segment) advance() error {
	l.mu.Lock()
	defer l.mu.Unlock()

	if l.file == nil {
		return ErrNotOpen
	}

	// If we're at the end of the file, can't advance
	if int64(l.pos) == l.size-footerSize {
		l.currentSize = 0
		return io.EOF
	}

	if err := l.seekEnd(-footerSize); err != nil {
		return err
	}

	pos := l.pos + l.currentSize + 8
	if err := l.writeUint64(uint64(pos)); err != nil {
		return err
	}

	if err := l.file.Sync(); err != nil {
		return err
	}
	l.pos = pos

	if err := l.seekToCurrent(); err != nil {
		return err
	}

	sz, err := l.readUint64()
	if err != nil {
		return err
	}
	l.currentSize = int64(sz)

	if int64(l.pos) == l.size-footerSize {
		l.currentSize = 0
		return io.EOF
	}

	return nil
}

func (l *segment) totalBytes() int64 {
	l.mu.RLock()
	totalB := l.size - footerSize
	l.mu.RUnlock()
	return totalB
}

func (l *segment) close() error {
	l.mu.Lock()
	defer l.mu.Unlock()
	if err := l.file.Close(); err != nil {
		return err
	}
	l.file = nil
	return nil
}

func (l *segment) lastModified() (time.Time, error) {
	l.mu.RLock()
	defer l.mu.RUnlock()

	stats, err := os.Stat(l.file.Name())
	if err != nil {
		return time.Time{}, err
	}
	return stats.ModTime().UTC(), nil
}

func (l *segment) diskUsage() int64 {
	l.mu.RLock()
	defer l.mu.RUnlock()
	return l.size
}

func (l *segment) SetMaxSegmentSize(size int64) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.maxSize = size
}

func (l *segment) seekToCurrent() error {
	return l.seek(int64(l.pos))
}

func (l *segment) seek(pos int64) error {
	n, err := l.file.Seek(pos, os.SEEK_SET)
	if err != nil {
		return err
	}

	if n != pos {
		return fmt.Errorf("bad seek. exp %v, got %v", 0, n)
	}

	return nil
}

func (l *segment) seekEnd(pos int64) error {
	_, err := l.file.Seek(pos, os.SEEK_END)
	if err != nil {
		return err
	}

	return nil
}

func (l *segment) filePos() int64 {
	n, _ := l.file.Seek(0, os.SEEK_CUR)
	return n
}

func (l *segment) readUint64() (uint64, error) {
	b := make([]byte, 8)
	if err := l.readBytes(b); err != nil {
		return 0, err
	}
	return binary.BigEndian.Uint64(b), nil
}

func (l *segment) writeUint64(sz uint64) error {
	var buf [8]byte
	binary.BigEndian.PutUint64(buf[:], sz)
	return l.writeBytes(buf[:])
}

func (l *segment) writeBytes(b []byte) error {
	n, err := l.file.Write(b)
	if err != nil {
		return err
	}

	if n != len(b) {
		return fmt.Errorf("short write. got %d, exp %d", n, len(b))
	}
	return nil
}

func (l *segment) readBytes(b []byte) error {
	n, err := l.file.Read(b)
	if err != nil {
		return err
	}

	if n != len(b) {
		return fmt.Errorf("bad read. exp %v, got %v", 0, n)
	}
	return nil
}
