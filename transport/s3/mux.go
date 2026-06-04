package s3

import (
	"encoding/binary"
	"io"
	"net"
	"sync"
	"time"
)

// MuxStream wraps a StreamID and implements net.Conn.
type MuxStream struct {
	id      uint32
	session *MuxSession
	mu      sync.Mutex
	readBuf []byte
	readCond *sync.Cond
	closed  bool
	closeMu sync.Mutex
}

func (s *MuxStream) Read(b []byte) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	for len(s.readBuf) == 0 && !s.closed {
		s.readCond.Wait()
	}

	if len(s.readBuf) > 0 {
		n := copy(b, s.readBuf)
		s.readBuf = s.readBuf[n:]
		return n, nil
	}

	return 0, io.EOF
}

func (s *MuxStream) Write(b []byte) (int, error) {
	const maxPayload = 65535
	written := 0
	for len(b) > 0 {
		chunkSize := len(b)
		if chunkSize > maxPayload {
			chunkSize = maxPayload
		}
		err := s.session.writeFrame(s.id, 1, b[:chunkSize])
		if err != nil {
			return written, err
		}
		written += chunkSize
		b = b[chunkSize:]
	}
	return written, nil
}

func (s *MuxStream) Close() error {
	s.closeMu.Lock()
	defer s.closeMu.Unlock()

	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return nil
	}
	s.closed = true
	s.readCond.Broadcast()
	s.mu.Unlock()

	s.session.mu.Lock()
	delete(s.session.streams, s.id)
	s.session.mu.Unlock()

	// Send CLOSE frame to notify remote side
	_ = s.session.writeFrame(s.id, 3, nil)
	return nil
}

func (s *MuxStream) pushData(data []byte) {
	s.mu.Lock()
	s.readBuf = append(s.readBuf, data...)
	s.readCond.Signal()
	s.mu.Unlock()
}

func (s *MuxStream) closeInternal() {
	s.mu.Lock()
	if !s.closed {
		s.closed = true
		s.readCond.Broadcast()
	}
	s.mu.Unlock()
}

func (s *MuxStream) LocalAddr() net.Addr            { return s.session.conn.LocalAddr() }
func (s *MuxStream) RemoteAddr() net.Addr           { return s.session.conn.RemoteAddr() }
func (s *MuxStream) SetDeadline(t time.Time) error  { return nil }
func (s *MuxStream) SetReadDeadline(t time.Time) error { return nil }
func (s *MuxStream) SetWriteDeadline(t time.Time) error { return nil }

// MuxSession multiplexes multiple streams over a single connection.
type MuxSession struct {
	conn    net.Conn
	mu      sync.Mutex
	streams map[uint32]*MuxStream
	nextID  uint32
	closed  bool
}

func NewMuxSession(conn net.Conn) *MuxSession {
	s := &MuxSession{
		conn:    conn,
		streams: make(map[uint32]*MuxStream),
		nextID:  1,
	}
	go s.readLoop()
	return s
}

func (s *MuxSession) OpenStream() (*MuxStream, error) {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return nil, io.ErrClosedPipe
	}
	id := s.nextID
	s.nextID += 2

	stream := &MuxStream{
		id:      id,
		session: s,
	}
	stream.readCond = sync.NewCond(&stream.mu)
	s.streams[id] = stream
	s.mu.Unlock()

	// Send OPEN frame
	err := s.writeFrame(id, 2, nil)
	if err != nil {
		s.mu.Lock()
		delete(s.streams, id)
		s.mu.Unlock()
		return nil, err
	}

	return stream, nil
}

func (s *MuxSession) writeFrame(streamID uint32, frameType byte, payload []byte) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.closed {
		return io.ErrClosedPipe
	}

	length := len(payload)
	header := make([]byte, 7+length)
	binary.BigEndian.PutUint32(header[0:4], streamID)
	header[4] = frameType
	binary.BigEndian.PutUint16(header[5:7], uint16(length))
	if length > 0 {
		copy(header[7:], payload)
	}

	_, err := s.conn.Write(header)
	return err
}

func (s *MuxSession) readLoop() {
	header := make([]byte, 7)
	for {
		_, err := io.ReadFull(s.conn, header)
		if err != nil {
			s.Close()
			return
		}

		streamID := binary.BigEndian.Uint32(header[0:4])
		frameType := header[4]
		length := binary.BigEndian.Uint16(header[5:7])

		var payload []byte
		if length > 0 {
			payload = make([]byte, length)
			_, err = io.ReadFull(s.conn, payload)
			if err != nil {
				s.Close()
				return
			}
		}

		s.mu.Lock()
		stream, exists := s.streams[streamID]
		s.mu.Unlock()

		switch frameType {
		case 1: // DATA
			if exists {
				stream.pushData(payload)
			}
		case 2: // OPEN
			// Clients ignore server-side stream initiations
		case 3: // CLOSE
			if exists {
				stream.closeInternal()
				s.mu.Lock()
				delete(s.streams, streamID)
				s.mu.Unlock()
			}
		}
	}
}

func (s *MuxSession) Close() error {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return nil
	}
	s.closed = true
	s.mu.Unlock()

	s.closeAll()
	return s.conn.Close()
}

func (s *MuxSession) isClosed() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.closed
}

func (s *MuxSession) closeAll() {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, stream := range s.streams {
		stream.closeInternal()
	}
	s.streams = make(map[uint32]*MuxStream)
}
