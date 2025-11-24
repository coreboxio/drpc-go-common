package drpc

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"net/http"
	"reflect"
	"runtime"
	"sync"
	"time"

	globalConfig "github.com/coreboxio/drpc-go-common/config"
	"github.com/coreboxio/drpc-go-common/logging"
	"github.com/coreboxio/drpc-go-common/utils"

	"github.com/gorilla/websocket"
)

var (
	cfgWSServer      globalConfig.GlobalConfig
	upgrader         websocket.Upgrader
	initUpgrader     sync.Once
	dataBufferPoolWS = sync.Pool{
		New: func() interface{} {
			return &rawData{}
		},
	}
)

type WSServerClient struct {
	httpRequest  *http.Request
	seqNum       uint64
	questSeqNum  uint32
	conn         *websocket.Conn
	send         chan []byte
	connected    bool
	answerMap    map[uint32]*connCallback
	mu           sync.RWMutex
	ticker       *time.Ticker
	lastPongTime time.Time
}

func (c *WSServerClient) GetHttpRequest() *http.Request {
	return c.httpRequest
}

func (c *WSServerClient) GetQueryParam(key string) string {
	return c.httpRequest.URL.Query().Get(key)
}

func (c *WSServerClient) Destory() {
	c.conn.Close()
	c.ticker.Stop()

	closeMap := make(map[uint32]*connCallback)
	{
		c.mu.Lock()

		for seqNum, callback := range c.answerMap {
			closeMap[seqNum] = callback
		}

		for seqNum, _ := range closeMap {
			delete(c.answerMap, seqNum)
		}

		c.mu.Unlock()
	}

	for seqNum, callback := range closeMap {
		answer := newErrorAnswerWithSeqNum(seqNum, DRPC_EC_CORE_CONNECTION_CLOSED, "Connection is closed.")
		go callAnswerCallback(answer, callback)
	}
}

func (c *WSServerClient) checkPingPong() {
	if time.Since(c.lastPongTime) > time.Duration(cfgWSServer.GetInt("websocket.ping_timeout_seconds", 30))*time.Second {
		logging.Warn("%s, ping timeout", c.Str())
		c.Destory()
	}
}

func (c *WSServerClient) nextSeqNum() uint32 {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.questSeqNum++
	return c.questSeqNum
}

func (c *WSServerClient) SendAnswer(answer *Answer) error {
	defer func() {
		if r := recover(); r != nil {
			logging.Error("Send answer error. %s seqNum: %d, panic: %v.", c.Str(), uint32(answer.seqNum), r)
		}
	}()

	data, err := answer.Raw()
	if err != nil {
		return err
	}

	if cfgWSServer.GetBool("answer_log") {
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

func (c *WSServerClient) SendQuest(quest *Quest, timeout ...time.Duration) *Answer {

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

	if cfgWSServer.GetBool("duplex_quest_log") {
		logging.Force("[DUPLEX.QUEST] %s, seqNum: %d, method: %s, quest: %v", c.Str(), uint32(quest.seqNum), quest.method, quest.ToJson())
	}

	realTimeout := time.Duration(cfgWSServer.GetInt("websocket.default_timeout_seconds", 5)) * time.Second
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

func (c *WSServerClient) SendQuestWithLambda(quest *Quest, callback func(answer *Answer), timeout ...time.Duration) {
	defer func() {
		if r := recover(); r != nil {
			logging.Error("Send quest error. %s seqNum: %d, panic: %v.", c.Str(), uint32(quest.seqNum), uint32(quest.seqNum), r)
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

	if cfgWSServer.GetBool("duplex_quest_log") {
		logging.Force("[DUPLEX.QUEST] %s, seqNum: %d, method: %s, quest: %v", c.Str(), uint32(quest.seqNum), quest.method, quest.ToJson())
	}

	realTimeout := time.Duration(cfgWSServer.GetInt("websocket.default_timeout_seconds", 5)) * time.Second
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

func (c *WSServerClient) cleanTimeoutedCallback() {
	now := time.Now()
	curr := now.Unix()
	timeoutedMap := make(map[uint32]*connCallback)
	{
		c.mu.Lock()

		for seqNum, callback := range c.answerMap {
			if callback.timeout <= curr {
				timeoutedMap[seqNum] = callback
			}
		}

		for seqNum, _ := range timeoutedMap {
			delete(c.answerMap, seqNum)
		}

		c.mu.Unlock()
	}

	for seqNum, callback := range timeoutedMap {

		answer := newErrorAnswerWithSeqNum(seqNum, DRPC_EC_CORE_TIMEOUT, "Quest is timeout.")
		go callAnswerCallback(answer, callback)
	}
}

type QuestProcessorInterfaceWS interface {
	ConnectionConnected(client *WSServerClient)
	ConnectionClosed(client *WSServerClient)
}

func (c *WSServerClient) Str() string {
	return fmt.Sprintf("connSeq: %d, query: %s", c.seqNum, c.httpRequest.URL.Query().Encode())
}

type ConnectionManagerWS struct {
	connections    map[uint64]*WSServerClient
	register       chan *WSServerClient
	unregister     chan *WSServerClient
	seqNum         uint64
	mu             sync.RWMutex
	questProcessor QuestProcessorInterfaceWS
	lastStatTime   time.Time
	ticker         *time.Ticker
	qpsStatMap     map[string]uint32
}

func (mgr *ConnectionManagerWS) nextSeqNum() uint64 {
	mgr.mu.Lock()
	defer mgr.mu.Unlock()

	mgr.seqNum++
	return mgr.seqNum
}

var managerWS = ConnectionManagerWS{
	connections:  make(map[uint64]*WSServerClient),
	register:     make(chan *WSServerClient),
	unregister:   make(chan *WSServerClient),
	seqNum:       0,
	lastStatTime: time.Now(),
	ticker:       time.NewTicker(1 * time.Second),
	qpsStatMap:   make(map[string]uint32),
}

func (mgr *ConnectionManagerWS) AddQpsStats(method string) {
	if cfgWSServer.GetInt("websocket.stat_interval_seconds", 30) <= 0 {
		return
	}
	mgr.mu.Lock()
	defer mgr.mu.Unlock()
	mgr.qpsStatMap[method]++
}

func (mgr *ConnectionManagerWS) PrintStatus() {
	if time.Since(mgr.lastStatTime) < time.Duration(cfgWSServer.GetInt("websocket.stat_interval_seconds", 30))*time.Second {
		return
	}

	mgr.lastStatTime = time.Now()

	logging.Force("[STATS.CONN.WS] connNum: %v, goroutine: %d", len(mgr.connections), runtime.NumGoroutine())

	statInterval := cfgWSServer.GetInt("websocket.stat_interval_seconds", 30)
	statStr := ""

	mgr.mu.Lock()
	for method, count := range mgr.qpsStatMap {
		qps := float64(count) / float64(statInterval)
		statStr += fmt.Sprintf("%s: %.2f, ", method, qps)
	}
	mgr.qpsStatMap = make(map[string]uint32)
	mgr.mu.Unlock()

	if statStr != "" {
		logging.Force("[STATS.QPS.WS] %s", statStr)
	}
}

func StartWebsocketServer(questProcessor QuestProcessorInterfaceWS) {

	initUpgrader.Do(func() {
		cfgWSServer = globalConfig.GetConfig()
		upgrader = websocket.Upgrader{
			ReadBufferSize:  cfgWSServer.GetInt("websocket.read_buffer_size", 1024),
			WriteBufferSize: cfgWSServer.GetInt("websocket.write_buffer_size", 1024),
			CheckOrigin:     func(r *http.Request) bool { return true },
		}
	})

	managerWS.questProcessor = questProcessor
	go managerWS.Start()
}

func WebsocketHandler(w http.ResponseWriter, r *http.Request) {
	conn, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		logging.Error("Websocket upgrade fail: %v", err)
		return
	}

	client := &WSServerClient{
		httpRequest:  r,
		conn:         conn,
		seqNum:       managerWS.nextSeqNum(),
		questSeqNum:  0,
		send:         make(chan []byte, cfgWSServer.GetInt("websocket.send_channel_buffer", 512)),
		connected:    false,
		answerMap:    make(map[uint32]*connCallback),
		ticker:       time.NewTicker(1 * time.Second),
		lastPongTime: time.Now(),
	}

	managerWS.register <- client

	go client.readPump()
	go client.writePump()
}

func dealAnswerWS(client *WSServerClient, answer *Answer) {
	defer func() {
		if r := recover(); r != nil {
			logging.Error("Deal answer panic. seqNum: %d, panic: %v.", answer.SeqNum, r)
		}
	}()

	if cfgWSServer.GetBool("duplex_answer_log") {
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

func dealQuestWS(client *WSServerClient, quest *Quest) {

	defer func() {
		if r := recover(); r != nil {
			logging.Error("Process quest panic. Method: %s, panic: %v.", quest.Method(), r)
			if quest.IsTwoWay() {
				client.SendAnswer(NewErrorAnswer(quest, DRPC_EC_CORE_UNKNOWN_ERROR, "Unknown error"))
			}
		}
	}()

	if cfgWSServer.GetBool("quest_log") {
		logging.Force("[QUEST] %s, seqNum: %d, method: %s, quest: %v", client.Str(), uint32(quest.seqNum), quest.method, quest.ToJson())
	}

	if quest.IsTwoWay() && quest.Method() == "*ping" {

		answer := NewAnswer(quest)
		answer.Param("mts", time.Now().UnixMilli())

		client.SendAnswer(answer)
		return
	}

	methodUpper := capitalizeFirstLetter(quest.Method())
	value := reflect.ValueOf(managerWS.questProcessor)
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

	managerWS.AddQpsStats(quest.Method())

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

func decodePackageWS(client *WSServerClient, data []byte) {
	if len(data) >= 8 {

		dataBuffer := dataBufferPoolWS.Get().(*rawData)

		defer func() {
			dataBufferPoolWS.Put(dataBuffer)
		}()

		dataBuffer.header = data[:8]
		dataBuffer.body = data[8:]

		var payloadSize uint32
		headReader := bytes.NewReader(dataBuffer.header[4:])
		binary.Read(headReader, binary.LittleEndian, &payloadSize)

		if payloadSize > cfgWSServer.GetUint32("websocket.max_payload_size", 8388608) {
			logging.Warn("Read huge payload, %s, size: %d", client.Str(), payloadSize)
			client.Destory()
			return
		}

		switch dataBuffer.header[2] {

		case MessageTypeOneWay, MessageTypeTwoWay:

			quest, err := NewQuestWithBuffer(dataBuffer.header, dataBuffer.body)
			if err != nil {
				logging.Warn("Decode quest failed, %s, err: %v", client.Str(), err)
				client.Destory()
				return
			}

			client.lastPongTime = time.Now()
			dealQuestWS(client, quest)

		case MessageTypeAnswer:
			answer, err := NewAnswerWithBuffer(dataBuffer.header, dataBuffer.body)
			if err != nil {
				logging.Warn("Decode answer failed, %s, err: %v", client.Str(), err)
				client.Destory()
				return
			}

			client.lastPongTime = time.Now()
			dealAnswerWS(client, answer)
		}
	}
}

func (m *ConnectionManagerWS) Start() {
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
			logging.Info("[WS.SERVER] %s connected, fromIP: %s, remoteAddr: %v, localAddr: %v", client.Str(), utils.GetIPv4(client.httpRequest), client.conn.RemoteAddr(), client.conn.LocalAddr())

			go managerWS.questProcessor.ConnectionConnected(client)

		case client := <-m.unregister:
			m.mu.Lock()
			if _, ok := m.connections[client.seqNum]; ok {
				delete(m.connections, client.seqNum)
				close(client.send)
				client.conn.Close()
				client.connected = false
				logging.Info("[WS.SERVER] %s disconnected, fromIP: %s, remoteAddr: %v, localAddr: %v", client.Str(), utils.GetIPv4(client.httpRequest), client.conn.RemoteAddr(), client.conn.LocalAddr())
			}
			m.mu.Unlock()

			go managerWS.questProcessor.ConnectionClosed(client)

		case <-m.ticker.C:
			m.PrintStatus()
		}
	}
}

func (c *WSServerClient) readPump() {
	defer func() {
		managerWS.unregister <- c
	}()

	c.conn.SetReadLimit(cfgWSServer.GetInt64("websocket.max_read_limit", 10485760))
	c.conn.SetReadDeadline(time.Now().Add(time.Duration(cfgWSServer.GetInt("websocket.read_deadline_seconds", 60)) * time.Second))
	c.conn.SetPongHandler(func(string) error {

		logging.Debug("%s, receive pong", c.Str())

		c.lastPongTime = time.Now()
		c.conn.SetReadDeadline(time.Now().Add(time.Duration(cfgWSServer.GetInt("websocket.read_deadline_seconds", 60)) * time.Second))
		return nil
	})

	for {
		messageType, message, err := c.conn.ReadMessage()
		if err != nil {
			if websocket.IsUnexpectedCloseError(err, websocket.CloseGoingAway, websocket.CloseAbnormalClosure) {
				logging.Warn("%s, unexpected close error: %v", c.Str(), err)
			}
			break
		}

		if messageType == websocket.BinaryMessage {
			go decodePackageWS(c, message)
		}
	}
}

func (c *WSServerClient) writePump() {
	defer func() {
		c.conn.Close()
		c.ticker.Stop()
	}()

	lastSendPing := time.Now()

	for {
		select {
		case message, ok := <-c.send:

			c.conn.SetWriteDeadline(time.Now().Add(time.Duration(cfgWSServer.GetInt("websocket.write_deadline_seconds", 60)) * time.Second))
			if !ok {
				logging.Debug("%s, close write deadline connection", c.Str())
				c.conn.WriteMessage(websocket.CloseMessage, []byte{})
				return
			}

			w, err := c.conn.NextWriter(websocket.BinaryMessage)
			if err != nil {
				return
			}
			w.Write(message)

			if err := w.Close(); err != nil {
				return
			}

		case <-c.ticker.C:

			if time.Since(lastSendPing) > time.Duration(cfgWSServer.GetInt("websocket.ping_interval_seconds", 10))*time.Second {
				lastSendPing = time.Now()

				c.conn.SetWriteDeadline(time.Now().Add(time.Duration(cfgWSServer.GetInt("websocket.write_deadline_seconds", 60)) * time.Second))

				if err := c.conn.WriteMessage(websocket.PingMessage, nil); err != nil {
					logging.Warn("%s, ping error: %v", c.Str(), err)
					return
				}

				logging.Debug("%s, send ping", c.Str())
			}

			go c.cleanTimeoutedCallback()
			go c.checkPingPong()
		}
	}
}
