// Package memcache provides a memcache client
package memcache

import (
	"bufio"
	"encoding/binary"
	"errors"
	"io"
	"net"
	"unsafe"
)

// Similar to:
// http://code.google.com/appengine/docs/go/memcache/reference.html

var (
	// ErrCacheMiss means that a Get failed because the item wasn't present.
	ErrCacheMiss = errors.New("memcache: cache miss")

	// ErrCASConflict means that a CompareAndSwap call failed due to the
	// cached value being modified between the Get and the CompareAndSwap.
	// If the cached value was simply evicted rather than replaced,
	// ErrNotStored will be returned instead.
	ErrCASConflict = errors.New("memcache: compare-and-swap conflict")

	// ErrNotStored means that a conditional write operation (i.e. Add or
	// CompareAndSwap) failed because the condition was not satisfied.
	ErrNotStored = errors.New("memcache: item not stored")

	// ErrServer means that a server error occurred.
	ErrServerError = errors.New("memcache: server error")

	// ErrNoStats means that no statistics were available.
	ErrNoStats = errors.New("memcache: no statistics available")

	// ErrMalformedKey is returned when an invalid key is used.
	// Keys must be at maximum 250 bytes long and not
	// contain whitespace or control characters.
	ErrMalformedKey = errors.New("malformed: key is too long or contains invalid characters")

	// ErrNoServers is returned when no servers are configured or available.
	ErrNoServers = errors.New("memcache: no servers configured or available")

	// ErrMagicByte is returned when magic byte of the packet is invalid.
	ErrMagicByte = errors.New("memcache: invalid magic byte")
)

// Item is an item to be got or stored in a memcached server.
type Item struct {
	// Key is the Item's key (250 bytes maximum).
	Key string

	// Value is the Item's value.
	Value []byte

	// Flags are server-opaque flags whose semantics are entirely
	// up to the app.
	Flags uint32

	// Expiration is the cache expiration time, in seconds: either a relative
	// time from now (up to 1 month), or an absolute Unix epoch time.
	// Zero means the Item has no expiration time.
	Expiration int32

	// Compare and swap ID.
	casid uint64
}

const (
	magicReq = 0x80
	magicRes = 0x81
)

const (
	statusNoError = iota
	statusKeyNotFound
	statusKeyExists
	statusValueTooLarge
	statusInvalidArguments
	statusItemNotStored
	statusInvalidIncrDecr // Incr/Decr on non-numeric value.
	statusVBucketError    // The vbucket belongs to another server
	statusAuthenticationError
	statusAuthenticationContinue
	statusUnknownCommand   = 0x81
	statusOutOfMemory      = 0x82
	statusNotSupported     = 0x83
	statusInternalError    = 0x84
	statusBusy             = 0x85
	statusTemporaryFailure = 0x85
)

const (
	opGet = iota
	opSet
	opAdd
	opReplace
	opDelete
	opIncrement
	opDecrement
	opQuit
	opFlush
	opGetQ
	opNoop
	opVersion
	opGetK
	opGetKQ
	opAppend
	opPrepend
	opStat
	opSetQ
)

// Conn is a network connection to memcached.
type Conn struct {
	rw   *bufio.ReadWriter
	conn net.Conn
}

func Connect(network, addr string) (*Conn, error) {
	conn, err := net.Dial(network, addr)
	if err != nil {
		return nil, err
	}
	return New(conn), nil
}

func New(conn net.Conn) *Conn {
	return &Conn{
		rw:   bufio.NewReadWriter(bufio.NewReader(conn), bufio.NewWriter(conn)),
		conn: conn,
	}
}

func writeHeader(buf []byte, op byte, keyLen uint16, extraLen byte,
	vbucket uint16, bodyLen uint32, casid uint64) {
	buf[0] = magicReq                              // magic byte
	buf[1] = op                                    // opcode
	binary.BigEndian.PutUint16(buf[2:4], keyLen)   // key length
	buf[4] = extraLen                              // extra length
	buf[5] = 0                                     // data type
	binary.BigEndian.PutUint16(buf[6:8], vbucket)  // vbucket id
	binary.BigEndian.PutUint32(buf[8:12], bodyLen) // total body length
	binary.BigEndian.PutUint32(buf[12:16], 0)      // opaque
	binary.BigEndian.PutUint64(buf[16:24], casid)  // cas
}

type resp struct {
	keyLen   uint16
	extraLen byte
	status   uint16
	bodyLen  uint32
	casid    uint64
	body     []byte
}

func (r *resp) statusError() error {
	switch r.status {
	case statusKeyNotFound:
		return ErrCacheMiss
	case statusItemNotStored:
		return ErrNotStored
	case statusKeyExists:
		return ErrCASConflict
	default:
		panic(r.status)
	}
}

func (c *Conn) readResp(buf []byte) (r resp, err error) {
	if _, err = io.ReadAtLeast(c.rw, buf[:24], 24); err != nil {
		return
	}
	if buf[0] != magicRes { // check magic byte
		err = ErrMagicByte
		return
	}
	// opcode := buf[1]
	r.keyLen = binary.BigEndian.Uint16(buf[2:4])
	r.extraLen = buf[4]
	// r.dataType := buf[5]
	r.status = binary.BigEndian.Uint16(buf[6:8])
	r.bodyLen = binary.BigEndian.Uint32(buf[8:12])
	// r.opaque := binary.BigEndian.Uint32(buf[12:16])
	r.casid = binary.BigEndian.Uint64(buf[16:24])

	if r.bodyLen > 0 {
		r.body = make([]byte, r.bodyLen)
		if _, err = io.ReadAtLeast(c.rw, r.body, int(r.bodyLen)); err != nil {
			return
		}
	}
	return
}

func (c *Conn) Get(key string) (*Item, error) {
	var (
		buf     = make([]byte, 24)
		keyLen  = uint16(len(key))
		bodyLen = uint32(len(key))
	)
	writeHeader(buf, opGet, keyLen, 0, 0, bodyLen, 0)
	if _, err := c.rw.Write(buf); err != nil {
		return nil, err
	}
	if _, err := c.rw.WriteString(key); err != nil {
		return nil, err
	}
	if err := c.rw.Flush(); err != nil {
		return nil, err
	}

	r, err := c.readResp(buf)
	if err != nil {
		return nil, err
	}
	if r.status != statusNoError {
		return nil, r.statusError()
	}
	kl := int(r.keyLen)
	el := int(r.extraLen)
	vpos := kl + el
	flags := binary.BigEndian.Uint32(r.body[kl:vpos])
	value := r.body[vpos:]
	return &Item{
		Key:   key,
		Value: value,
		Flags: flags,
		casid: r.casid,
	}, nil
}

func (c *Conn) set(op byte, cas bool, item *Item) error {
	var (
		casid   = uint64(0)
		buf     = make([]byte, 24+8)
		keyLen  = uint16(len(item.Key))
		bodyLen = uint32(8 + len(item.Key) + len(item.Value))
	)
	if cas {
		casid = uint64(item.casid)
	}
	writeHeader(buf, op, keyLen, 8, 0, bodyLen, casid)
	binary.BigEndian.PutUint32(buf[24:28], item.Flags)
	binary.BigEndian.PutUint32(buf[28:32], *(*uint32)(unsafe.Pointer(&item.Expiration)))
	if _, err := c.rw.Write(buf); err != nil {
		return err
	}
	if _, err := c.rw.WriteString(item.Key); err != nil {
		return err
	}
	if _, err := c.rw.Write(item.Value); err != nil {
		return err
	}
	if err := c.rw.Flush(); err != nil {
		return err
	}

	r, err := c.readResp(buf)
	if err != nil {
		return err
	}
	if r.status != statusNoError {
		return r.statusError()
	}
	item.casid = r.casid
	return nil
}

func (c *Conn) Set(item *Item) error {
	return c.set(opSet, false, item)
}

func (c *Conn) Add(item *Item) error {
	return c.set(opAdd, false, item)
}

func (c *Conn) Replace(item *Item) error {
	return c.set(opReplace, false, item)
}

func (c *Conn) CompareAndSwap(item *Item) error {
	return c.set(opSet, false, item)
}

func (c *Conn) Delete(key string) error {
	var (
		buf     = make([]byte, 24)
		keyLen  = uint16(len(key))
		bodyLen = uint32(len(key))
	)
	writeHeader(buf, opDelete, keyLen, 0, 0, bodyLen, 0)
	if _, err := c.rw.Write(buf); err != nil {
		return err
	}
	if _, err := c.rw.WriteString(key); err != nil {
		return err
	}

	r, err := c.readResp(buf)
	if err != nil {
		return err
	}
	if r.status != statusNoError {
		return r.statusError()
	}
	return nil
}

func (c *Conn) incrOrDecr(op byte, key string, delta uint64, initialValue uint64, expiration int) (newValue uint64, err error) {
	var (
		buf     = make([]byte, 24+20)
		keyLen  = uint16(len(key))
		bodyLen = uint32(20 + len(key))
	)
	writeHeader(buf, op, keyLen, 4, 0, bodyLen, 0)
	binary.BigEndian.PutUint64(buf, delta)
	binary.BigEndian.PutUint64(buf, initialValue)
	binary.BigEndian.PutUint32(buf, uint32(expiration))
	if _, err = c.rw.Write(buf); err != nil {
		return
	}
	if _, err = c.rw.WriteString(key); err != nil {
		return
	}
	if err = c.rw.Flush(); err != nil {
		return
	}

	r, err := c.readResp(buf)
	if err != nil {
		return
	}
	if r.status != statusNoError {
		err = r.statusError()
		return
	}
	newValue = binary.BigEndian.Uint64(r.body)
	return
}

func (c *Conn) Increment(key string, delta uint64, initialValue uint64, expiration int) (newValue uint64, err error) {
	return c.incrOrDecr(opIncrement, key, delta, initialValue, expiration)
}

func (c *Conn) Decrement(key string, delta uint64, initialValue uint64, expiration int) (newValue uint64, err error) {
	return c.incrOrDecr(opDecrement, key, delta, initialValue, expiration)
}

func (c *Conn) Flush(expiration int) error {
	buf := make([]byte, 24+4)
	writeHeader(buf, opFlush, 0, 4, 0, 0, 0)
	binary.BigEndian.PutUint32(buf, uint32(expiration))
	if _, err := c.rw.Write(buf); err != nil {
		return err
	}
	if err := c.rw.Flush(); err != nil {
		return err
	}

	r, err := c.readResp(buf)
	if err != nil {
		return err
	}
	if r.status != statusNoError {
		return r.statusError()
	}
	return nil
}
