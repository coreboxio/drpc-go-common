package drpc

import (
	"errors"
	"runtime"
	"sync"
	"time"
)

const (
	SDKVersion = "1.0.0"
)

type AnswerCallback interface {
	OnAnswer(answer *Answer)
	OnException(answer *Answer)
}

type QuestProcessor interface {
	Process(method string) func(*Quest) (*Answer, error)
}

type KeepAliveParams struct {
	pingTimeout       time.Duration
	pingInterval      time.Duration
	maxPingRetryCount int
}

type tcpClientConnectedCallback func(connId uint64, endpoint string, connected bool)
type tcpClientCloseCallback func(connId uint64, endpoint string)

type TCPClient struct {
	mutex           sync.Mutex
	autoReconnect   bool
	endpoint        string
	timeout         time.Duration
	connectTimeout  time.Duration
	conn            *tcpConnection
	questProcessor  QuestProcessor
	onConnected     tcpClientConnectedCallback
	onClosed        tcpClientCloseCallback
	logger          Logger
	keepAliveParams *KeepAliveParams
}

func NewTCPClient(endpoint string) *TCPClient {

	client := &TCPClient{}

	client.autoReconnect = true
	client.endpoint = endpoint
	client.timeout = Config.questTimeout
	client.connectTimeout = Config.connectTimeout
	runtime.SetFinalizer(client, closeTCPClient)
	return client
}

func closeTCPClient(client *TCPClient) {
	go client.Close()
}

func (client *TCPClient) SetAutoReconnect(autoReconnect bool) {
	client.autoReconnect = autoReconnect
}

func (client *TCPClient) SetKeepAlive(keepAlive bool) {
	if keepAlive {
		client.mutex.Lock()
		if client.keepAliveParams == nil {
			param := new(KeepAliveParams)
			client.keepAliveParams = param
			client.keepAliveParams.pingInterval = Config.pingInterval
			client.keepAliveParams.maxPingRetryCount = Config.maxPingRetryCount
			client.keepAliveParams.pingTimeout = Config.questTimeout
		}
		client.mutex.Unlock()
	}
}

func (client *TCPClient) SetKeepAliveTimeoutSecond(second time.Duration) {
	client.SetKeepAlive(true)
	client.keepAliveParams.pingTimeout = second
}

func (client *TCPClient) SetKeepAliveIntervalSecond(second time.Duration) {
	client.SetKeepAlive(true)
	client.keepAliveParams.pingInterval = second
}

func (client *TCPClient) SetKeepAliveMaxPingRetryCount(count int) {
	client.SetKeepAlive(true)
	client.keepAliveParams.maxPingRetryCount = count
}

func (client *TCPClient) GetAutoReconnect() bool {
	return client.autoReconnect
}

func (client *TCPClient) SetConnectTimeOut(timeout time.Duration) {
	client.connectTimeout = timeout
}

func (client *TCPClient) SetQuestTimeOut(timeout time.Duration) {
	client.timeout = timeout
}

func (client *TCPClient) SetQuestProcessor(questProcessor QuestProcessor) {
	client.questProcessor = questProcessor
}

func (client *TCPClient) SetOnConnectedCallback(onConnected tcpClientConnectedCallback) {
	client.onConnected = onConnected
}

func (client *TCPClient) SetOnClosedCallback(onClosed tcpClientCloseCallback) {
	client.onClosed = onClosed
}

func (client *TCPClient) SetLogger(logger Logger) {
	client.logger = logger
}

func (client *TCPClient) IsConnected() bool {
	client.mutex.Lock()
	conn := client.conn
	client.mutex.Unlock()

	if conn == nil {
		return false
	} else {
		return conn.isConnected()
	}
}

func (client *TCPClient) Endpoint() string {
	return client.endpoint
}

func (client *TCPClient) Connect() bool {

	conn := newTCPConnection(client.logger, client.onConnected, client.onClosed, client.questProcessor, client.keepAliveParams)

	client.mutex.Lock()
	defer client.mutex.Unlock()

	if client.conn != nil && client.conn.isConnected() {
		return true
	}

	client.conn = conn
	ok := conn.connect(client.endpoint, client.connectTimeout)

	return ok
}

func (client *TCPClient) Dial() bool {
	return client.Connect()
}

func (client *TCPClient) checkConnection() *tcpConnection {

	ok := client.IsConnected()
	if !ok {
		if client.autoReconnect {
			_ = client.Connect()
		} else {
			return nil
		}
	}

	client.mutex.Lock()
	defer client.mutex.Unlock()

	if client.conn != nil && client.conn.isConnected() {
		return client.conn
	}
	return nil
}

func (client *TCPClient) realSendQuest(quest *Quest, cb *connCallback) error {
	conn := client.checkConnection()
	if conn == nil {
		return errors.New("Connection is invalid.")
	}
	return conn.sendQuest(quest, cb)
}

func (client *TCPClient) SendQuest(quest *Quest, timeout ...time.Duration) (*Answer, error) {

	if !quest.isTwoWay {
		err := client.realSendQuest(quest, nil)
		return nil, err
	}

	//------------ send two way quest ---------------//
	realTimeout := client.timeout
	if len(timeout) == 1 && timeout[0] != 0 {
		realTimeout = timeout[0]
	} else if len(timeout) > 1 {
		panic("Invalid params when call DRPC.TCPCLient.SendQuest() function.")
	}

	answerChan := make(chan *Answer)

	cb := &connCallback{}
	cb.timeout = time.Now().Unix() + int64(realTimeout/time.Second)
	cb.callbackFunc = func(answer *Answer) {
		if answer == nil {
			answer = newErrorAnswerWithAnswer(quest.seqNum, answer)
		}

		answerChan <- answer
	}

	err := client.realSendQuest(quest, cb)
	if err != nil {
		return nil, err
	}

	answer := <-answerChan

	return answer, nil
}

func (client *TCPClient) SendQuestWithCallback(quest *Quest, callback AnswerCallback, timeout ...time.Duration) error {

	realTimeout := client.timeout
	if len(timeout) == 1 && timeout[0] != 0 {
		realTimeout = timeout[0]
	} else if len(timeout) > 1 {
		panic("Invalid params when call DRPC.TCPCLient.SendQuest() function.")
	}

	var cb *connCallback

	if quest.isTwoWay {
		cb = &connCallback{}

		cb.timeout = time.Now().Unix() + int64(realTimeout/time.Second)
		cb.callback = callback
	}

	return client.realSendQuest(quest, cb)
}

func (client *TCPClient) SendQuestWithLambda(quest *Quest, callback func(answer *Answer), timeout ...time.Duration) error {

	realTimeout := client.timeout
	if len(timeout) == 1 && timeout[0] != 0 {
		realTimeout = timeout[0]
	} else if len(timeout) > 1 {
		panic("Invalid params when call DRPC.TCPCLient.SendQuest() function.")
	}

	var cb *connCallback

	if quest.isTwoWay {
		cb = &connCallback{}

		cb.timeout = time.Now().Unix() + int64(realTimeout/time.Second)
		cb.callbackFunc = callback
	}

	return client.realSendQuest(quest, cb)
}

func (client *TCPClient) Close() {
	client.mutex.Lock()

	conn := client.conn
	client.conn = nil
	client.mutex.Unlock()

	if conn != nil {
		conn.close()
	}
}
