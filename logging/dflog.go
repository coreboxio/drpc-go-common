package logging

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"net"
	"sync"
	"time"
)

const (
	MAX_LOG_QUEUE_SIZE = 1000
)

type DLog struct {
	host      string
	timeout   time.Duration
	connPool  int
	queue     chan []byte
	stop      chan struct{}
	flushCond *sync.Cond
	flushWg   sync.WaitGroup
}

func NewDLog(host string, timeout time.Duration, connPool int) *DLog {
	l := &DLog{
		host:      host,
		timeout:   timeout,
		connPool:  connPool,
		queue:     make(chan []byte, MAX_LOG_QUEUE_SIZE),
		stop:      make(chan struct{}),
		flushCond: sync.NewCond(&sync.Mutex{}),
	}

	for i := 0; i < connPool; i++ {
		l.flushWg.Add(1)
		go l.flush()
	}

	return l
}

func (l *DLog) _connect() (net.Conn, error) {
	conn, err := net.DialTimeout("unix", l.host, l.timeout)
	if err != nil {
		return nil, err
	}
	return conn, nil
}

func (l *DLog) _close(conn net.Conn) {
	if conn != nil {
		conn.Close()
	}
}

func (l *DLog) _send(conn net.Conn, data []byte) error {
	_, err := conn.Write(data)
	return err
}

func (l *DLog) _read(conn net.Conn) (bool, error) {
	reply := make([]byte, 1)
	_, err := conn.Read(reply)
	if err != nil || reply[0] != '1' {
		return false, err
	}
	return true, nil
}

func (l *DLog) flush() {
	defer l.flushWg.Done()
	var conn net.Conn
	defer l._close(conn)

	for {
		select {
		case <-l.stop:
			return
		default:
			l.flushCond.L.Lock()
			l.flushCond.Wait()
			l.flushCond.L.Unlock()

			buffer := l.collectLogs()
			if len(buffer) == 0 {
				continue
			}

			if conn == nil {
				var err error
				conn, err = l._connect()
				if err != nil {
					fmt.Println("Failed to connect:", err)
					l.requeueLogs(buffer)
					continue
				}
			}

			for _, data := range buffer {
				if err := l._send(conn, data); err != nil || !l.ensureResponse(conn) {
					l._close(conn)
					conn, _ = l._connect()
					if conn != nil {
						l._send(conn, data)
						l.ensureResponse(conn)
					}
				}
			}
		}
	}
}

func (l *DLog) collectLogs() [][]byte {
	buffer := [][]byte{}
	for {
		select {
		case log := <-l.queue:
			buffer = append(buffer, log)
		default:
			return buffer
		}
	}
}

func (l *DLog) requeueLogs(buffer [][]byte) {
	for _, data := range buffer {
		select {
		case l.queue <- data:
		default:
			fmt.Println("Queue full, dropping logs")
		}
	}
}

func (l *DLog) ensureResponse(conn net.Conn) bool {
	ok, err := l._read(conn)
	if err != nil || !ok {
		fmt.Println("Failed to get a valid response")
		return false
	}
	return true
}

func (l *DLog) Write(tag string, data string) error {
	payload, err := l.buildPayload(tag, data)
	if err != nil {
		return err
	}
	select {
	case l.queue <- payload:
		l.flushCond.Signal()
		return nil
	default:
		return errors.New("Log Queue reached max size, log dropped")
	}
}

func (l *DLog) buildPayload(tag string, data string) ([]byte, error) {
	var buf bytes.Buffer

	if err := binary.Write(&buf, binary.LittleEndian, uint8(len(tag))); err != nil {
		return nil, err
	}
	buf.WriteString(tag)

	if err := binary.Write(&buf, binary.LittleEndian, uint16(0)); err != nil {
		return nil, err
	}

    if err := binary.Write(&buf, binary.LittleEndian, uint32(0)); err != nil {
		return nil, err
	}

	if err := binary.Write(&buf, binary.LittleEndian, uint32(len(data))); err != nil {
		return nil, err
	}
	buf.WriteString(data)

	return buf.Bytes(), nil
}

func (l *DLog) Destroy() {
	close(l.stop)
	l.flushCond.Broadcast()
	l.flushWg.Wait()
	close(l.queue)
}
