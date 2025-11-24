package drpc

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"time"

	"github.com/ugorji/go/codec"
)

type Quest struct {
	seqNum     uint32
	method     string
	isTwoWay   bool
	isMsgPack  bool
	createTime int64
	Payload
}

type Answer struct {
	seqNum    uint32
	status    uint8
	isMsgPack bool
	questTime int64
	Payload
}

//---------------------[ Quest Methods ]----------------------------//

func NewQuest(method string) *Quest {
	quest := &Quest{
		createTime: time.Now().UnixMilli(),
	}
	quest.seqNum = 0
	quest.method = method
	quest.isTwoWay = true
	quest.isMsgPack = true
	quest.Payload = Payload{make(map[interface{}]interface{})}
	return quest
}

func NewQuestWithPayload(method string, payload *Payload) *Quest {
	quest := NewQuest(method)
	quest.Payload = *payload
	return quest
}

func NewOneWayQuest(method string) *Quest {
	quest := NewQuest(method)
	quest.isTwoWay = false
	return quest
}

func NewOneWayQuestWithPayload(method string, payload *Payload) *Quest {
	quest := NewQuestWithPayload(method, payload)
	quest.isTwoWay = false
	return quest
}

func NewQuestWithBuffer(header []byte, body []byte) (*Quest, error) {
	return NewQuestWithRawData(&rawData{header, body})
}

func NewQuestWithRawData(data *rawData) (*Quest, error) {
	quest := &Quest{
		createTime: time.Now().UnixMilli(),
	}

	var handle codec.Handle
	if (data.header[1] & FlagMsgpack) == FlagMsgpack {
		quest.isMsgPack = true

		var mh codec.MsgpackHandle
		mh.RawToString = true
		handle = &mh
	} else {
		return nil, errors.New("invalid DRPC package flag")
	}

	methodLen := uint8(data.header[3])
	var methodSlice, payloadSlice []byte

	switch data.header[2] {

	case MessageTypeOneWay:
		quest.seqNum = 0
		quest.isTwoWay = false
		methodSlice = data.body[:methodLen]
		payloadSlice = data.body[methodLen:]

	case MessageTypeTwoWay:
		quest.isTwoWay = true

		seqNumReader := bytes.NewReader(data.body[:4])
		binary.Read(seqNumReader, binary.LittleEndian, &quest.seqNum)

		methodSlice = data.body[4 : 4+methodLen]
		payloadSlice = data.body[4+methodLen:]

	default:
		info := fmt.Sprintf("Receive invalid DRPC QType: %d", data.header[6])
		return nil, errors.New(info)
	}

	quest.method = string(methodSlice)
	quest.Payload = Payload{}

	decoder := codec.NewDecoderBytes(payloadSlice, handle)
	if err := decoder.Decode(&quest.Payload.data); err != nil {
		return nil, err
	}

	return quest, nil
}

func (quest *Quest) IsOneWay() bool {
	return !(quest.isTwoWay)
}

func (quest *Quest) IsTwoWay() bool {
	return quest.isTwoWay
}

func (quest *Quest) IsMsgPack() bool {
	return quest.isMsgPack
}

func (quest *Quest) IsJson() bool {
	return !(quest.isMsgPack)
}

func (quest *Quest) SeqNum() uint32 {
	return quest.seqNum
}

func (quest *Quest) Method() string {
	return quest.method
}

func (quest *Quest) Raw() ([]byte, error) {
	var handle codec.Handle
	header := [4]byte{
		ProtoVersion,
	}

	header[1] = FlagMsgpack

	var mh codec.MsgpackHandle
	mh.WriteExt = true
	handle = &mh

	if quest.isTwoWay {
		header[2] = MessageTypeTwoWay

	} else {
		header[2] = MessageTypeOneWay
	}

	header[3] = byte(uint8(len(quest.method)))

	payloadBuf := new(bytes.Buffer)
	encoder := codec.NewEncoder(payloadBuf, handle)
	if err := encoder.Encode(quest.data); err != nil {
		return nil, err
	}

	payload := payloadBuf.Bytes()
	payloadSize := uint32(len(payload))

	result := new(bytes.Buffer)
	result.Write(header[:])
	binary.Write(result, binary.LittleEndian, payloadSize)
	if quest.isTwoWay {
		binary.Write(result, binary.LittleEndian, quest.seqNum)
	}
	result.WriteString(quest.method)
	result.Write(payload)

	res := make([]byte, result.Len())
	copy(res, result.Bytes())
	return res, nil
}

//---------------------[ Answer Methods ]----------------------------//

func NewAnswer(quest *Quest) *Answer {
	answer := &Answer{}
	answer.seqNum = quest.seqNum
	answer.status = 0
	answer.isMsgPack = quest.isMsgPack
	answer.questTime = quest.createTime
	answer.Payload = Payload{make(map[interface{}]interface{})}
	return answer
}

func NewErrorAnswer(quest *Quest, code int, ex string) *Answer {
	answer := NewAnswer(quest)
	answer.status = 1
	answer.questTime = quest.createTime
	answer.Param("code", code)
	answer.Param("ex", ex)
	return answer
}

func newErrorAnswerWithSeqNum(seqNum uint32, code int, ex string) *Answer {
	answer := &Answer{}

	answer.seqNum = seqNum
	answer.status = 1
	answer.isMsgPack = true
	answer.Payload = Payload{make(map[interface{}]interface{})}

	answer.Param("code", code)
	answer.Param("ex", ex)

	return answer
}

func newErrorAnswerWithAnswer(seqNum uint32, errorAnswer *Answer) *Answer {
	answer := &Answer{}

	answer.seqNum = seqNum
	answer.status = 1
	answer.isMsgPack = true
	answer.Payload = Payload{make(map[interface{}]interface{})}

	ex, okEx := errorAnswer.GetString("ex")
	code, okCode := errorAnswer.GetString("code")

	if okEx {
		answer.Param("ex", ex)
	} else {
		answer.Param("ex", "")
	}

	if okCode {
		answer.Param("code", code)
	} else {
		answer.Param("code", DRPC_EC_PROTO_UNKNOWN_ERROR)
	}

	return answer
}

func NewAnswerWithBuffer(header []byte, body []byte) (*Answer, error) {
	return NewAnswerWithRawData(&rawData{header, body})
}

func NewAnswerWithRawData(data *rawData) (*Answer, error) {
	answer := &Answer{}

	var handle codec.Handle
	if (data.header[1] & FlagMsgpack) == FlagMsgpack {
		answer.isMsgPack = true

		var mh codec.MsgpackHandle
		mh.RawToString = true
		handle = &mh
	} else {
		return nil, errors.New("invalid DRPC package flag")
	}

	answer.status = uint8(data.header[3])

	seqNumReader := bytes.NewReader(data.body[:4])
	binary.Read(seqNumReader, binary.LittleEndian, &answer.seqNum)

	answer.Payload = Payload{}

	decoder := codec.NewDecoderBytes(data.body[4:], handle)
	if err := decoder.Decode(&answer.Payload.data); err != nil {
		return nil, err
	}

	return answer, nil
}

func (answer *Answer) SeqNum() uint32 {
	return answer.seqNum
}

func (answer *Answer) Status() uint8 {
	return answer.status
}

func (answer *Answer) IsException() bool {
	return answer.status != 0
}

func (answer *Answer) IsMsgPack() bool {
	return answer.isMsgPack
}

func (answer *Answer) Raw() ([]byte, error) {
	var handle codec.Handle
	header := [4]byte{
		ProtoVersion,
	}

	header[1] = FlagMsgpack

	var mh codec.MsgpackHandle
	mh.WriteExt = true
	handle = &mh

	header[2] = MessageTypeAnswer
	header[3] = byte(answer.status)

	payloadBuf := new(bytes.Buffer)
	encoder := codec.NewEncoder(payloadBuf, handle)
	if err := encoder.Encode(answer.data); err != nil {
		return nil, err
	}

	payload := payloadBuf.Bytes()
	payloadSize := uint32(len(payload))

	result := new(bytes.Buffer)
	result.Write(header[:])
	binary.Write(result, binary.LittleEndian, payloadSize)
	binary.Write(result, binary.LittleEndian, answer.seqNum)
	result.Write(payload)

	res := make([]byte, result.Len())
	copy(res, result.Bytes())
	return res, nil
}
