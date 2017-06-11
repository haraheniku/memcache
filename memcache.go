// Package memcache provides a memcache client
package memcache

import (
	"bufio"
	"encoding/binary"
	"errors"
	"hash/crc32"
	"io"
	"net"
	"unsafe"

	"golang.org/x/sync/errgroup"
)

// Credits to:
// https://github.com/bradfitz/gomemcache/blob/master/memcache/memcache.go

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

type Client struct {
	servers []string
}

func New(servers ...string) *Client {
	return &Client{servers}
}

func (c *Client) pickServer(key string) string {
	h := crc32.ChecksumIEEE(*(*[]byte)(unsafe.Pointer(&key)))
	return c.servers[int(h)%len(c.servers)]
}

func (c *Client) getConnWithKey(key string) (*Conn, error) {
	return c.getConn(c.pickServer(key))
}

func (c *Client) getConn(addr string) (*Conn, error) {
	return Connect("tcp", addr)
}

func (c *Client) putConn(conn *Conn) error {
	return conn.Close()
}

func (c *Client) Get(key string) (*Item, error) {
	conn, err := c.getConnWithKey(key)
	if err != nil {
		return nil, err
	}
	defer c.putConn(conn)

	return conn.Get(key)
}

func (c *Client) GetMulti(keys []string) (map[string]*Item, error) {
	bins := make(map[string][]string)
	for _, key := range keys {
		addr := c.pickServer(key)
		bins[addr] = append(bins[addr], key)
	}

	var g errgroup.Group
	result := make([]map[string]*Item, len(c.servers))
	for i, addr := range c.servers {
		i, addr := i, addr
		g.Go(func() error {
			conn, err := c.getConn(addr)
			if err != nil {
				return err
			}
			defer c.putConn(conn)

			items, err := conn.GetMulti(bins[addr])
			if err != nil {
				return err
			}
			result[i] = items
			return nil
		})
	}
	if err := g.Wait(); err != nil {
		return nil, err
	}

	items := make(map[string]*Item)
	for _, m := range result {
		for key, item := range m {
			items[key] = item
		}
	}
	return items, nil
}

func (c *Client) Set(item *Item) error {
	conn, err := c.getConnWithKey(item.Key)
	if err != nil {
		return err
	}
	defer c.putConn(conn)

	return conn.Set(item)
}

func (c *Client) Add(item *Item) error {
	conn, err := c.getConnWithKey(item.Key)
	if err != nil {
		return err
	}
	defer c.putConn(conn)

	return conn.Add(item)
}

func (c *Client) Replace(item *Item) error {
	conn, err := c.getConnWithKey(item.Key)
	if err != nil {
		return err
	}
	defer c.putConn(conn)

	return conn.Replace(item)
}

func (c *Client) CompareAndSwap(item *Item) error {
	conn, err := c.getConnWithKey(item.Key)
	if err != nil {
		return err
	}
	defer c.putConn(conn)

	return conn.CompareAndSwap(item)
}

func (c *Client) Increment(key string, delta uint64, initialValue uint64, expiration int) (uint64, error) {
	conn, err := c.getConnWithKey(key)
	if err != nil {
		return 0, err
	}
	defer c.putConn(conn)

	return conn.Increment(key, delta, initialValue, expiration)
}

func (c *Client) Decrement(key string, delta uint64, initialValue uint64, expiration int) (uint64, error) {
	conn, err := c.getConnWithKey(key)
	if err != nil {
		return 0, err
	}
	defer c.putConn(conn)

	return conn.Decrement(key, delta, initialValue, expiration)

}

func (c *Client) Flush(expiration int) error {
	var g errgroup.Group
	for _, server := range c.servers {
		addr := server
		g.Go(func() error {
			conn, err := c.getConn(addr)
			if err != nil {
				return err
			}
			return conn.Flush(expiration)
		})
	}

	return g.Wait()
}

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
	return NewConn(conn), nil
}

func NewConn(conn net.Conn) *Conn {
	return &Conn{
		rw:   bufio.NewReadWriter(bufio.NewReader(conn), bufio.NewWriter(conn)),
		conn: conn,
	}
}

func (c *Conn) Close() error {
	return c.conn.Close()
}

func setHeader(buf []byte, opcode byte, keyLen int, extraLen int,
	vbucket uint16, bodyLen int, casid uint64) {
	buf[0] = magicReq                                      // magic byte
	buf[1] = opcode                                        // opcode
	binary.BigEndian.PutUint16(buf[2:4], uint16(keyLen))   // key length
	buf[4] = byte(extraLen)                                // extra length
	buf[5] = 0                                             // data type
	binary.BigEndian.PutUint16(buf[6:8], vbucket)          // vbucket id
	binary.BigEndian.PutUint32(buf[8:12], uint32(bodyLen)) // total body length
	binary.BigEndian.PutUint32(buf[12:16], 0)              // opaque
	binary.BigEndian.PutUint64(buf[16:24], casid)          // cas
}

func isValidKey(key string) bool {
	if len(key) > 250 {
		return false
	}
	for _, c := range key {
		// [\x21-\x7e\x80-\xff]
		if c < 0x20 || c == 0x7f {
			return false
		}
	}
	return true
}

func (c *Conn) writeReq(head []byte, key string, value []byte) error {
	if _, err := c.rw.Write(head); err != nil {
		return err
	}
	if key != "" {
		if _, err := c.rw.WriteString(key); err != nil {
			return err
		}
	}
	if len(value) > 0 {
		if _, err := c.rw.Write(value); err != nil {
			return err
		}
	}
	return nil
}

type resp struct {
	opcode   byte
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
	r.opcode = buf[1]
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
	if !isValidKey(key) {
		return nil, ErrMalformedKey
	}

	buf := make([]byte, 24)
	setHeader(buf, opGet, len(key), 0, 0, len(key), 0)
	if err := c.writeReq(buf, key, nil); err != nil {
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
	flags := binary.BigEndian.Uint32(r.body[:el])
	value := r.body[el+kl:]
	return &Item{
		Key:   key,
		Value: value,
		Flags: flags,
		casid: r.casid,
	}, nil
}

func (c *Conn) GetMulti(keys []string) (map[string]*Item, error) {
	buf := make([]byte, 24)
	for _, key := range keys[:len(keys)-1] {
		if !isValidKey(key) {
			return nil, ErrMalformedKey
		}
		setHeader(buf, opGetKQ, len(key), 0, 0, len(key), 0)
		if err := c.writeReq(buf, key, nil); err != nil {
			return nil, err
		}
	}
	key := keys[len(keys)-1]
	if !isValidKey(key) {
		return nil, ErrMalformedKey
	}
	setHeader(buf, opGetK, len(key), 0, 0, len(key), 0)
	if err := c.writeReq(buf, key, nil); err != nil {
		return nil, err
	}
	if err := c.rw.Flush(); err != nil {
		return nil, err
	}

	opcode := -1
	result := make(map[string]*Item)
	for opcode != opGetK {
		r, err := c.readResp(buf)
		if err != nil {
			return nil, err
		}
		opcode = int(r.opcode)
		if r.status != statusNoError {
			if r.status == statusKeyNotFound {
				continue
			}
			return nil, r.statusError()
		}
		kl := int(r.keyLen)
		el := int(r.extraLen)
		flags := binary.BigEndian.Uint32(r.body[:el])
		key := string(r.body[el : el+kl])
		value := r.body[el+kl:]
		result[key] = &Item{
			Key:   key,
			Value: value,
			Flags: flags,
			casid: r.casid,
		}
	}
	return result, nil
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
	return c.set(opSet, true, item)
}

func (c *Conn) set(opcode byte, cas bool, item *Item) error {
	if !isValidKey(item.Key) {
		return ErrMalformedKey
	}

	var (
		casid    = uint64(0)
		keyLen   = len(item.Key)
		extraLen = 8
		bodyLen  = extraLen + len(item.Key) + len(item.Value)
		buf      = make([]byte, 24+extraLen)
	)
	if cas {
		casid = uint64(item.casid)
	}
	setHeader(buf, opcode, keyLen, extraLen, 0, bodyLen, casid)
	binary.BigEndian.PutUint32(buf[24:28], item.Flags)
	binary.BigEndian.PutUint32(buf[28:32], *(*uint32)(unsafe.Pointer(&item.Expiration)))
	if err := c.writeReq(buf, item.Key, item.Value); err != nil {
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

func (c *Conn) Delete(key string) error {
	if !isValidKey(key) {
		return ErrMalformedKey
	}

	buf := make([]byte, 24)
	setHeader(buf, opDelete, len(key), 0, 0, len(key), 0)
	if err := c.writeReq(buf, key, nil); err != nil {
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

func (c *Conn) Increment(key string, delta uint64, initialValue uint64, expiration int) (uint64, error) {
	return c.incrOrDecr(opIncrement, key, delta, initialValue, expiration)
}

func (c *Conn) Decrement(key string, delta uint64, initialValue uint64, expiration int) (uint64, error) {
	return c.incrOrDecr(opDecrement, key, delta, initialValue, expiration)
}

func (c *Conn) incrOrDecr(opcode byte, key string, delta uint64, initialValue uint64, expiration int) (newValue uint64, err error) {
	if !isValidKey(key) {
		err = ErrMalformedKey
		return
	}

	var (
		keyLen   = len(key)
		extraLen = 20
		bodyLen  = extraLen + len(key)
		buf      = make([]byte, 24+extraLen)
	)
	setHeader(buf, opcode, keyLen, extraLen, 0, bodyLen, 0)
	binary.BigEndian.PutUint64(buf, delta)
	binary.BigEndian.PutUint64(buf, initialValue)
	binary.BigEndian.PutUint32(buf, uint32(expiration))
	if err = c.writeReq(buf, key, nil); err != nil {
		return
	}
	if err = c.rw.Flush(); err != nil {
		return
	}

	var r resp
	r, err = c.readResp(buf)
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

func (c *Conn) Flush(expiration int) error {
	var (
		extraLen = 4
		buf      = make([]byte, 24+extraLen)
	)
	setHeader(buf, opFlush, 0, extraLen, 0, 0, 0)
	binary.BigEndian.PutUint32(buf, uint32(expiration))
	if err := c.writeReq(buf, "", nil); err != nil {
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
