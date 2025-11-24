package drpc

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"net"
	"reflect"
	"runtime"
	"sync"
	"time"

	globalConfig "github.com/drpc/drpc-go-common/config"
	"github.com/drpc/drpc-go-common/logging"
)

var (
	cfgTCPServer      globalConfig.GlobalConfig
	initOnce          sync.Once
	dataBufferPoolTCP = sync.Pool{
		New: func() interface{} {
			return &rawData{}
		},
	}
)

type TCPServerClient struct {
	seqNum       uint64
	questSeqNum  uint32
	conn         net.Conn
	send         chan []byte
	connected    bool
	answerMap    map[uint32]*connCallback
	mu           sync.RWMutex
	ticker       *time.Ticker
	lastPongTime time.Time
	buffer       []byte
}

type QuestProcessorInterfaceTCP interface {
	ConnectionConnected(client *TCPServerClient)
	ConnectionClosed(client *TCPServerClient)
}

func (c *TCPServerClient) Destroy() {
	c.conn.Close()
	c.ticker.Stop()

	closeMap := make(map[uint32]*connCallback)
	c.mu.Lock()
	for seqNum, callback := range c.answerMap {
		closeMap[seqNum] = callback
	}
	for seqNum := range closeMap {
		delete(c.answerMap, seqNum)
	}
	c.mu.Unlock()

	for seqNum, callback := range closeMap {
		answer := newErrorAnswerWithSeqNum(seqNum, DRPC_EC_CORE_CONNECTION_CLOSED, "Connection is closed.")
		go callAnswerCallback(answer, callback)
	}
}

func (c *TCPServerClient) checkPingPong() {
	if time.Since(c.lastPongTime) > time.Duration(cfgTCPServer.GetInt("tcp.ping_timeout_seconds", 30))*time.Second {
		logging.Warn("%s, ping timeout", c.Str())
		c.Destroy()
	}
}

func (c *TCPServerClient) nextSeqNum() uint32 {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.questSeqNum++
	return c.questSeqNum
}

func (c *TCPServerClient) SendAnswer(answer *Answer) error {
	defer func() {
		if r := recover(); r != nil {
			logging.Error("Send answer error. %s seqNum: %d, panic: %v.", c.Str(), uint32(answer.seqNum), r)
		}
	}()

	data, err := answer.Raw()
	if err != nil {
		return err
	}

	if cfgTCPServer.GetBool("answer_log") {
		cost := time.Now().UnixMilli() - answer.questTime
		logging.Force("[ANSWER] %s, seqNum: %d, cost: %v, answer: %v", c.Str(), uint32(answer.seqNum), cost, answer.ToJson())
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	if c.connected {
		c.send <- data
	} else {
		logging.Info("Connection is closed, %s, seqNum: %d", c.Str(), uint32(answer.seqNum))
		return fmt.Errorf("connection is closed")
	}
	return nil
}

func (c *TCPServerClient) SendQuest(quest *Quest, timeout ...time.Duration) *Answer {
	defer func() {
		if r := recover(); r != nil {
			logging.Error("Send quest error. %s seqNum: %d, panic: %v.", c.Str(), uint32(quest.seqNum), r)
		}
	}()

	quest.seqNum = c.nextSeqNum()
	data, err := quest.Raw()
	if err != nil && quest.IsTwoWay() {
		return newErrorAnswerWithSeqNum(quest.seqNum, DRPC_EC_CORE_UNKNOWN_ERROR, "")
	}

	if quest.IsOneWay() {
		c.mu.Lock()
		defer c.mu.Unlock()
		if c.connected {
			c.send <- data
		}
		return nil
	}

	if cfgTCPServer.GetBool("duplex_quest_log") {
		logging.Force("[DUPLEX.QUEST] %s, seqNum: %d, method: %s, quest: %v", c.Str(), uint32(quest.seqNum), quest.method, quest.ToJson())
	}

	realTimeout := time.Duration(cfgTCPServer.GetInt("tcp.default_timeout_seconds", 5)) * time.Second
	if len(timeout) == 1 && timeout[0] != 0 {
		realTimeout = timeout[0]
	} else if len(timeout) > 1 {
		panic("Invalid params when call SendQuest")
	}

	answerChan := make(chan *Answer)

	cb := &connCallback{}
	cb.timeout = time.Now().Unix() + int64(realTimeout/time.Second)
	cb.callbackFunc = func(answer *Answer) {
		if answer == nil {
			answer = newErrorAnswerWithSeqNum(quest.seqNum, DRPC_EC_PROTO_UNKNOWN_ERROR, "")
		}
		answerChan <- answer
	}

	c.mu.Lock()
	c.answerMap[quest.seqNum] = cb

	if c.connected {
		c.send <- data
	} else {
		c.mu.Unlock()
		logging.Info("Connection is closed, %s, seqNum: %d", c.Str(), uint32(quest.seqNum))
		return newErrorAnswerWithSeqNum(quest.seqNum, DRPC_EC_CORE_CONNECTION_CLOSED, "Connection is closed.")
	}

	c.mu.Unlock()

	answer := <-answerChan

	return answer
}

func (c *TCPServerClient) SendQuestWithLambda(quest *Quest, callback func(answer *Answer), timeout ...time.Duration) {
	defer func() {
		if r := recover(); r != nil {
			logging.Error("Send quest error. %s seqNum: %d, panic: %v.", r, uint32(quest.seqNum), uint32(quest.seqNum))
		}
	}()

	quest.seqNum = c.nextSeqNum()
	data, err := quest.Raw()
	if err != nil && quest.IsTwoWay() {
		go callback(newErrorAnswerWithSeqNum(quest.seqNum, DRPC_EC_CORE_UNKNOWN_ERROR, ""))
		return
	}

	if quest.IsOneWay() {
		c.mu.Lock()
		if c.connected {
			c.send <- data
		}
		c.mu.Unlock()
		go callback(nil)
		return
	}

	if cfgTCPServer.GetBool("duplex_quest_log") {
		logging.Force("[DUPLEX.QUEST] %s, seqNum: %d, method: %s, quest: %v", c.Str(), uint32(quest.seqNum), quest.method, quest.ToJson())
	}

	realTimeout := time.Duration(cfgTCPServer.GetInt("tcp.default_timeout_seconds", 5)) * time.Second
	if len(timeout) == 1 && timeout[0] != 0 {
		realTimeout = timeout[0]
	} else if len(timeout) > 1 {
		panic("Invalid params when call SendQuest")
	}

	cb := &connCallback{}
	cb.timeout = time.Now().Unix() + int64(realTimeout/time.Second)
	cb.callbackFunc = func(answer *Answer) {
		if answer == nil {
			callback(newErrorAnswerWithSeqNum(quest.seqNum, DRPC_EC_PROTO_UNKNOWN_ERROR, ""))
			return
		}
		callback(answer)
	}

	c.mu.Lock()
	c.answerMap[quest.seqNum] = cb

	if c.connected {
		c.send <- data
	} else {
		c.mu.Unlock()
		logging.Info("Connection is closed, %s, seqNum: %d", c.Str(), uint32(quest.seqNum))
		callback(newErrorAnswerWithSeqNum(quest.seqNum, DRPC_EC_CORE_CONNECTION_CLOSED, "Connection is closed."))
		return
	}

	c.mu.Unlock()
}

func dealAnswerTCP(client *TCPServerClient, answer *Answer) {
	defer func() {
		if r := recover(); r != nil {
			logging.Error("Deal answer panic. seqNum: %d, panic: %v.", answer.SeqNum(), r)
		}
	}()

	if cfgTCPServer.GetBool("duplex_answer_log") {
		logging.Force("[DUPLEX.ANSWER] %s, seqNum: %d, answer: %v", client.Str(), uint32(answer.seqNum), answer.ToJson())
	}

	client.mu.Lock()
	callback, ok := client.answerMap[answer.seqNum]
	if ok {
		delete(client.answerMap, answer.seqNum)
		client.mu.Unlock()

		go callAnswerCallback(answer, callback)
	} else {
		client.mu.Unlock()
		logging.Error("Received invalid answer, seqNum: %d", answer.seqNum)
	}

}

func dealQuestTCP(client *TCPServerClient, quest *Quest) {
	defer func() {
		if r := recover(); r != nil {
			logging.Error("Process quest panic. Method: %s, panic: %v.", quest.Method(), r)
			if quest.IsTwoWay() {
				client.SendAnswer(NewErrorAnswer(quest, DRPC_EC_CORE_UNKNOWN_ERROR, "Unknown error"))
			}
		}
	}()

	if cfgTCPServer.GetBool("quest_log") {
		logging.Force("[QUEST] %s, seqNum: %d, method: %s, quest: %v", client.Str(), uint32(quest.seqNum), quest.method, quest.ToJson())
	}

	if quest.IsTwoWay() && quest.Method() == "*ping" {

		answer := NewAnswer(quest)
		answer.Param("mts", time.Now().UnixMilli())

		client.SendAnswer(answer)
		return
	}

	methodUpper := capitalizeFirstLetter(quest.Method())
	value := reflect.ValueOf(managerTCP.questProcessor)
	method := value.MethodByName(methodUpper)

	if !method.IsValid() || method.Kind() != reflect.Func {
		logging.Warn("Unknown method, %s, method: %s, quest: %v", client.Str(), quest.Method(), quest.ToJson())

		if quest.IsTwoWay() {
			client.SendAnswer(NewErrorAnswer(quest, DRPC_EC_CORE_UNKNOWN_METHOD, "Unknown method"))
		}
		return
	}

	args := []reflect.Value{
		reflect.ValueOf(client),
		reflect.ValueOf(quest),
	}

	answerReflect := method.Call(args)

	managerTCP.AddQpsStats(quest.Method())

	if quest.IsTwoWay() && len(answerReflect) < 1 {
		logging.Error("No available answer returned, method: %s", quest.Method())
		client.SendAnswer(NewErrorAnswer(quest, DRPC_EC_CORE_UNKNOWN_ERROR, "Unknown error"))
		return
	}

	if quest.IsTwoWay() {
		answer := answerReflect[0].Interface().(*Answer)
		if answer != nil {
			client.SendAnswer(answer)
		}
	}
}

func (c *TCPServerClient) cleanTimeoutedCallback() {
	now := time.Now()
	curr := now.Unix()
	timeoutedMap := make(map[uint32]*connCallback)

	c.mu.Lock()
	for seqNum, callback := range c.answerMap {
		if callback.timeout <= curr {
			timeoutedMap[seqNum] = callback
		}
	}
	for seqNum := range timeoutedMap {
		delete(c.answerMap, seqNum)
	}
	c.mu.Unlock()

	for seqNum, callback := range timeoutedMap {
		answer := newErrorAnswerWithSeqNum(seqNum, DRPC_EC_CORE_TIMEOUT, "Quest is timeout.")
		go callAnswerCallback(answer, callback)
	}
}

func (c *TCPServerClient) Str() string {
	return fmt.Sprintf("connSeq: %d", c.seqNum)
}

type ConnectionManagerTCP struct {
	connections    map[uint64]*TCPServerClient
	register       chan *TCPServerClient
	unregister     chan *TCPServerClient
	seqNum         uint64
	mu             sync.RWMutex
	questProcessor QuestProcessorInterfaceTCP
	lastStatTime   time.Time
	ticker         *time.Ticker
	qpsStatMap     map[string]uint32
}

var managerTCP = ConnectionManagerTCP{
	connections:  make(map[uint64]*TCPServerClient),
	register:     make(chan *TCPServerClient),
	unregister:   make(chan *TCPServerClient),
	seqNum:       0,
	lastStatTime: time.Now(),
	ticker:       time.NewTicker(1 * time.Second),
	qpsStatMap:   make(map[string]uint32),
}

func (mgr *ConnectionManagerTCP) AddQpsStats(method string) {
	if cfgTCPServer.GetInt("tcp.stat_interval_seconds", 30) <= 0 {
		return
	}
	mgr.mu.Lock()
	defer mgr.mu.Unlock()
	mgr.qpsStatMap[method]++
}

func (mgr *ConnectionManagerTCP) PrintStatus() {
	if time.Since(mgr.lastStatTime) < time.Duration(cfgTCPServer.GetInt("tcp.stat_interval_seconds", 30))*time.Second {
		return
	}

	mgr.lastStatTime = time.Now()

	logging.Force("[STATS.CONN.TCP] connNum: %v, goroutine: %d", len(mgr.connections), runtime.NumGoroutine())

	statInterval := cfgTCPServer.GetInt("tcp.stat_interval_seconds", 30)
	statStr := ""

	mgr.mu.Lock()
	for method, count := range mgr.qpsStatMap {
		qps := float64(count) / float64(statInterval)
		statStr += fmt.Sprintf("%s: %.2f, ", method, qps)
	}
	mgr.qpsStatMap = make(map[string]uint32)
	mgr.mu.Unlock()

	if statStr != "" {
		logging.Force("[STATS.QPS.TCP] %s", statStr)
	}
}

func (mgr *ConnectionManagerTCP) nextSeqNum() uint64 {
	mgr.mu.Lock()
	defer mgr.mu.Unlock()

	mgr.seqNum++
	return mgr.seqNum
}

func StartTCPServer(address string, questProcessor QuestProcessorInterfaceTCP) {
	initOnce.Do(func() {
		cfgTCPServer = globalConfig.GetConfig()
	})

	listener, err := net.Listen("tcp", address)
	if err != nil {
		logging.Error("Failed to start TCP server: %v", err)
		return
	}
	defer listener.Close()
	logging.Info("TCP server started on %s", address)

	managerTCP.questProcessor = questProcessor

	go managerTCP.Loop()

	for {
		conn, err := listener.Accept()
		if err != nil {
			logging.Warn("Failed to accept connection: %v", err)
			continue
		}

		go handleTCPServerConnection(conn)
	}
}

func handleTCPServerConnection(conn net.Conn) {

	client := &TCPServerClient{
		conn:         conn,
		seqNum:       managerTCP.nextSeqNum(),
		questSeqNum:  0,
		send:         make(chan []byte, cfgTCPServer.GetInt("tcp.send_channel_buffer", 512)),
		connected:    false,
		answerMap:    make(map[uint32]*connCallback),
		ticker:       time.NewTicker(1 * time.Second),
		lastPongTime: time.Now(),
		buffer:       make([]byte, 0),
	}

	managerTCP.register <- client

	go client.readPump()
	go client.writePump()
}

func (m *ConnectionManagerTCP) Loop() {
	defer func() {
		m.ticker.Stop()
	}()

	for {
		select {
		case client := <-m.register:
			m.mu.Lock()
			m.connections[client.seqNum] = client
			client.connected = true
			m.mu.Unlock()
			logging.Info("[TCP.SERVER] %s connected, remoteAddr: %v, localAddr: %v", client.Str(), client.conn.RemoteAddr(), client.conn.LocalAddr())

			go managerTCP.questProcessor.ConnectionConnected(client)

		case client := <-m.unregister:
			m.mu.Lock()
			if _, ok := m.connections[client.seqNum]; ok {
				delete(m.connections, client.seqNum)
				close(client.send)
				client.conn.Close()
				client.connected = false
				logging.Info("[TCP.SERVER] %s disconnected, remoteAddr: %v, localAddr: %v", client.Str(), client.conn.RemoteAddr(), client.conn.LocalAddr())
			}
			m.mu.Unlock()

			go managerTCP.questProcessor.ConnectionClosed(client)
		case <-m.ticker.C:
			m.PrintStatus()
		}
	}
}

func (c *TCPServerClient) writePump() {
	defer func() {
		c.ticker.Stop()
	}()

	for {
		select {
		case message, ok := <-c.send:
			if !ok {
				logging.Info("%s, close connection", c.Str())
				return
			}

			c.conn.SetWriteDeadline(time.Now().Add(time.Duration(cfgTCPServer.GetInt("tcp.write_deadline_seconds", 60)) * time.Second))
			_, err := c.conn.Write(message)
			if err != nil {
				logging.Info("%s, write error: %v", c.Str(), err)
				return
			}

		case <-c.ticker.C:
			go c.cleanTimeoutedCallback()
			go c.checkPingPong()
		}
	}
}

func (c *TCPServerClient) readPump() {
	defer func() {
		c.conn.Close()
		managerTCP.unregister <- c
	}()

	buf := make([]byte, 4096)
	for {
		c.conn.SetReadDeadline(time.Now().Add(time.Duration(cfgTCPServer.GetInt("tcp.read_deadline_seconds", 60)) * time.Second))
		n, err := c.conn.Read(buf)
		if err != nil {
			logging.Info("%s, read error: %v", c.Str(), err)
			break
		}
		c.decodePackageTCP(buf[:n])
	}
}

func (c *TCPServerClient) decodePackageTCP(data []byte) {
	c.buffer = append(c.buffer, data...)

	for {
		if len(c.buffer) < 8 {
			return
		}

		dataBuffer := dataBufferPoolTCP.Get().(*rawData)
		defer dataBufferPoolTCP.Put(dataBuffer)

		dataBuffer.header = c.buffer[:8]

		var payloadSize uint32
		headReader := bytes.NewReader(dataBuffer.header[4:])
		binary.Read(headReader, binary.LittleEndian, &payloadSize)

		if payloadSize > cfgTCPServer.GetUint32("tcp.max_payload_size", 8388608) {
			logging.Warn("Read huge payload, %s, size: %d", c.Str(), payloadSize)
			c.Destroy()
			return
		}

		var bodySize int
		switch dataBuffer.header[2] {
		case MessageTypeTwoWay:
			methodLen := uint32(dataBuffer.header[3])

			bodySize = int(4 + methodLen + payloadSize)
			if len(c.buffer) < 8+bodySize {
				return
			}

			dataBuffer.body = c.buffer[8 : 8+bodySize]

			quest, err := NewQuestWithBuffer(dataBuffer.header, dataBuffer.body)
			if err != nil {
				logging.Warn("Decode quest failed, %s, err: %v", c.Str(), err)
				c.Destroy()
				return
			}

			c.lastPongTime = time.Now()
			go dealQuestTCP(c, quest)
		case MessageTypeOneWay:
			methodLen := uint32(dataBuffer.header[3])

			bodySize = int(methodLen + payloadSize)
			if len(c.buffer) < 8+bodySize {
				return
			}

			dataBuffer.body = c.buffer[8 : 8+bodySize]

			quest, err := NewQuestWithBuffer(dataBuffer.header, dataBuffer.body)
			if err != nil {
				logging.Warn("Decode quest failed, %s, err: %v", c.Str(), err)
				c.Destroy()
				return
			}

			c.lastPongTime = time.Now()
			go dealQuestTCP(c, quest)
		case MessageTypeAnswer:
			bodySize = int(4 + payloadSize)
			if len(c.buffer) < 8+bodySize {
				return
			}

			dataBuffer.body = c.buffer[8 : 8+bodySize]
			answer, err := NewAnswerWithBuffer(dataBuffer.header, dataBuffer.body)
			if err != nil {
				logging.Warn("Decode answer failed, %s, err: %v", c.Str(), err)
				c.Destroy()
				return
			}

			c.lastPongTime = time.Now()
			go dealAnswerTCP(c, answer)
		}

		c.buffer = c.buffer[8+bodySize:]
	}
}
