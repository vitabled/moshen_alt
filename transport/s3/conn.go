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
	return &S3Conn{
		client:       client,
		sessionID:    sessionID,
		writeBuf:     make([]byte, 0, client.opts.ChunkSize),
		pollInterval: client.opts.PollMin(),
		minPoll:      client.opts.PollMin(),
		maxPoll:      client.opts.PollMax(),
		closed:       make(chan struct{}),
	}
}

// Read polls S3 for downstream data with exponential backoff.
func (c *S3Conn) Read(b []byte) (int, error) {
	c.readMu.Lock()
	defer c.readMu.Unlock()

	for {
		// Return buffered data first
		if c.readPos < len(c.readBuf) {
			n := copy(b, c.readBuf[c.readPos:])
			c.readPos += n
			if c.readPos >= len(c.readBuf) {
				c.readBuf = nil
				c.readPos = 0
			}
			return n, nil
		}

		// Check if closed
		select {
		case <-c.closed:
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
			// On transient errors, back off and retry
			select {
			case <-c.closed:
				return 0, io.EOF
			case <-time.After(c.pollInterval):
			}
			c.backoff()
			continue
		}

		if found && len(data) > 0 {
			// Data received — reset to aggressive polling
			c.readBuf = data
			c.readPos = 0
			c.readSeq++
			c.pollInterval = c.minPoll

			n := copy(b, c.readBuf[c.readPos:])
			c.readPos += n
			if c.readPos >= len(c.readBuf) {
				c.readBuf = nil
				c.readPos = 0
			}
			return n, nil
		}

		// Empty — wait with backoff before next poll
		select {
		case <-c.closed:
			return 0, io.EOF
		case <-time.After(c.pollInterval):
		}
		c.backoff()
	}
}

// Write buffers data and flushes via S3 PUT when the chunk is full.
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

	// Flush any remaining data immediately — latency matters more than
	// batching for an interactive tunnel.
	if len(c.writeBuf) > 0 {
		if err := c.flushLocked(); err != nil {
			return written, err
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

	data := make([]byte, len(c.writeBuf))
	copy(data, c.writeBuf)
	c.writeBuf = c.writeBuf[:0]

	err := c.client.putObject(ctx, key, data)
	if err != nil {
		return fmt.Errorf("s3: PUT c2s/%06d.bin failed: %w", c.writeSeq, err)
	}
	c.writeSeq++
	return nil
}

// Close signals shutdown. Best-effort cleanup of session objects is not
// done here to avoid blocking — the server side handles expiry.
func (c *S3Conn) Close() error {
	c.closeOnce.Do(func() {
		close(c.closed)
	})
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

// backoff doubles the polling interval, capped at maxPoll.
func (c *S3Conn) backoff() {
	c.pollInterval *= 2
	if c.pollInterval > c.maxPoll {
		c.pollInterval = c.maxPoll
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
