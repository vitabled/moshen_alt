package s3

import (
	"context"
	"fmt"
	"io"
	"net"
	"sync"
	"time"
)

// S3Conn implements net.Conn by tunneling data through S3 PUT/GET.
//
// Write path: data → buffer → PUT [prefix]/[sessionID]/c2s/[seqN].bin
// Read path:  GET [prefix]/[sessionID]/s2c/[seqN].bin → buffer → data
//
// The Read() method uses exponential backoff: aggressive polling when
// data flows, backing off to save battery when the bucket is empty.
type S3Conn struct {
	client    *Client
	sessionID string

	// Write side
	writeBuf []byte
	writeSeq uint64
	writeMu  sync.Mutex

	// Read side — exponential backoff polling
	readBuf      []byte
	readPos      int
	readSeq      uint64
	readMu       sync.Mutex
	pollInterval time.Duration
	minPoll      time.Duration
	maxPoll      time.Duration

	// Lifecycle
	closed    chan struct{}
	closeOnce sync.Once

	// net.Conn deadlines
	readDeadline  atomicTime
	writeDeadline atomicTime
}

func newS3Conn(client *Client, sessionID string) *S3Conn {
	c := &S3Conn{
		client:       client,
		sessionID:    sessionID,
		writeBuf:     make([]byte, 0, client.opts.ChunkSize),
		pollInterval: client.opts.PollMin(),
		minPoll:      client.opts.PollMin(),
		maxPoll:      client.opts.PollMax(),
		closed:       make(chan struct{}),
	}
	go c.writeLoop()
	return c
}

// Read polls S3 for downstream data with fixed polling interval.
func (c *S3Conn) Read(b []byte) (int, error) {
	c.readMu.Lock()
	defer c.readMu.Unlock()

	for {
		// Return buffered data first
		if c.readPos < len(c.readBuf) {
			n := copy(b, c.readBuf[c.readPos:])
			c.readPos += n
			if c.readPos >= len(c.readBuf) {
				c.putReadBufLocked()
				c.readPos = 0
			}
			return n, nil
		}

		// Check if closed
		select {
		case <-c.closed:
			c.putReadBufLocked()
			return 0, io.EOF
		default:
		}

		// Check read deadline
		if dl := c.readDeadline.Load(); !dl.IsZero() && time.Now().After(dl) {
			return 0, &timeoutError{op: "read"}
		}

		// Poll S3 for next downstream chunk
		ctx := c.contextWithDeadline(c.readDeadline.Load())
		key := c.client.s2cKey(c.sessionID, c.readSeq)
		data, found, err := c.client.getObject(ctx, key)

		if err != nil {
			// On transient errors, wait and retry
			select {
			case <-c.closed:
				c.putReadBufLocked()
				return 0, io.EOF
			case <-time.After(c.pollInterval):
			}
			continue
		}

		if found && len(data) > 0 {
			// Data received — keep polling at fixed interval
			c.putReadBufLocked() // Free any previously unused read buffer
			c.readBuf = data
			c.readPos = 0
			c.readSeq++

			n := copy(b, c.readBuf[c.readPos:])
			c.readPos += n
			if c.readPos >= len(c.readBuf) {
				c.putReadBufLocked()
				c.readPos = 0
			}
			return n, nil
		}

		// Empty — wait before next poll
		select {
		case <-c.closed:
			c.putReadBufLocked()
			return 0, io.EOF
		case <-time.After(c.pollInterval):
		}
	}
}

// Write buffers data and flushes via S3 PUT when the chunk is full.
// Remaining data is flushed asynchronously by writeLoop.
func (c *S3Conn) Write(b []byte) (int, error) {
	c.writeMu.Lock()
	defer c.writeMu.Unlock()

	select {
	case <-c.closed:
		return 0, io.ErrClosedPipe
	default:
	}

	written := 0
	for len(b) > 0 {
		space := c.client.opts.ChunkSize - len(c.writeBuf)
		if space <= 0 {
			if err := c.flushLocked(); err != nil {
				return written, err
			}
			space = c.client.opts.ChunkSize
		}

		n := len(b)
		if n > space {
			n = space
		}
		c.writeBuf = append(c.writeBuf, b[:n]...)
		b = b[n:]
		written += n

		// Flush when buffer is full
		if len(c.writeBuf) >= c.client.opts.ChunkSize {
			if err := c.flushLocked(); err != nil {
				return written, err
			}
		}
	}

	return written, nil
}

// flushLocked uploads the current write buffer as one S3 object.
// Caller must hold writeMu.
func (c *S3Conn) flushLocked() error {
	if len(c.writeBuf) == 0 {
		return nil
	}

	ctx := c.contextWithDeadline(c.writeDeadline.Load())
	key := c.client.c2sKey(c.sessionID, c.writeSeq)

	buf := chunkPool.Get().([]byte)
	n := copy(buf, c.writeBuf)
	data := buf[:n]
	c.writeBuf = c.writeBuf[:0]

	err := c.client.putObject(ctx, key, data)
	chunkPool.Put(buf)
	if err != nil {
		return fmt.Errorf("s3: PUT c2s/%06d.bin failed: %w", c.writeSeq, err)
	}
	c.writeSeq++
	return nil
}

func (c *S3Conn) putReadBufLocked() {
	if c.readBuf != nil {
		if cap(c.readBuf) == 65536 {
			chunkPool.Put(c.readBuf[:65536])
		}
		c.readBuf = nil
	}
}

// Close signals shutdown. Best-effort cleanup of session objects is not
// done here to avoid blocking — the server side handles expiry.
func (c *S3Conn) Close() error {
	c.closeOnce.Do(func() {
		close(c.closed)
	})
	c.readMu.Lock()
	c.putReadBufLocked()
	c.readMu.Unlock()
	return nil
}

func (c *S3Conn) LocalAddr() net.Addr  { return s3Addr{} }
func (c *S3Conn) RemoteAddr() net.Addr { return s3Addr{c.client.opts.Endpoint} }

func (c *S3Conn) SetDeadline(t time.Time) error {
	c.readDeadline.Store(t)
	c.writeDeadline.Store(t)
	return nil
}

func (c *S3Conn) SetReadDeadline(t time.Time) error {
	c.readDeadline.Store(t)
	return nil
}

func (c *S3Conn) SetWriteDeadline(t time.Time) error {
	c.writeDeadline.Store(t)
	return nil
}

// ---------- helpers ----------

// writeLoop flushes the write buffer every 20ms.
func (c *S3Conn) writeLoop() {
	ticker := time.NewTicker(20 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-c.closed:
			c.writeMu.Lock()
			_ = c.flushLocked()
			c.writeMu.Unlock()
			return
		case <-ticker.C:
			c.writeMu.Lock()
			_ = c.flushLocked()
			c.writeMu.Unlock()
		}
	}
}

// contextWithDeadline wraps context.Background with an optional deadline.
func (c *S3Conn) contextWithDeadline(dl time.Time) context.Context {
	ctx := context.Background()
	if !dl.IsZero() {
		var cancel context.CancelFunc
		ctx, cancel = context.WithDeadline(ctx, dl)
		_ = cancel // the caller's flow handles cancellation via closed channel
	}
	return ctx
}

// ---------- net.Addr ----------

type s3Addr struct{ endpoint string }

func (a s3Addr) Network() string { return "s3" }
func (a s3Addr) String() string {
	if a.endpoint == "" {
		return "s3://local"
	}
	return a.endpoint
}

// ---------- atomicTime ----------

// atomicTime is a thread-safe wrapper around time.Time.
type atomicTime struct {
	mu sync.Mutex
	t  time.Time
}

func (a *atomicTime) Store(t time.Time) {
	a.mu.Lock()
	a.t = t
	a.mu.Unlock()
}

func (a *atomicTime) Load() time.Time {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.t
}

// ---------- errors ----------

type timeoutError struct{ op string }

func (e *timeoutError) Error() string   { return fmt.Sprintf("s3: %s timeout", e.op) }
func (e *timeoutError) Timeout() bool   { return true }
func (e *timeoutError) Temporary() bool { return true }
