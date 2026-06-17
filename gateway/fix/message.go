package fix

import (
	"bytes"
	"fmt"
	"strconv"
	"strings"
	"time"
)

const (
	SOH = '\x01'

	MsgTypeLogon          = "A"
	MsgTypeLogout         = "5"
	MsgTypeHeartbeat      = "0"
	MsgTypeTestRequest    = "1"
	MsgTypeResendRequest  = "2"
	MsgTypeReject         = "3"
	MsgTypeSequenceReset  = "4"
	MsgTypeNewOrderSingle = "D"
	MsgTypeOrderCancel    = "F"
	MsgTypeExecutionReport = "8"
	MsgTypeOrderCancelReject = "9"

	BeginString = "FIX.4.4"
)

type FIXMessage struct {
	Fields map[int]string
}

func NewFIXMessage() *FIXMessage {
	return &FIXMessage{Fields: make(map[int]string)}
}

func (m *FIXMessage) Set(tag int, value string) {
	m.Fields[tag] = value
}

func (m *FIXMessage) Get(tag int) (string, bool) {
	v, ok := m.Fields[tag]
	return v, ok
}

func (m *FIXMessage) GetInt(tag int) (int, error) {
	v, ok := m.Fields[tag]
	if !ok {
		return 0, fmt.Errorf("tag %d not found", tag)
	}
	return strconv.Atoi(v)
}

func (m *FIXMessage) GetInt64(tag int) (int64, error) {
	v, ok := m.Fields[tag]
	if !ok {
		return 0, fmt.Errorf("tag %d not found", tag)
	}
	return strconv.ParseInt(v, 10, 64)
}

func (m *FIXMessage) MsgType() string {
	v, _ := m.Fields[35]
	return v
}

func (m *FIXMessage) SenderCompID() string {
	v, _ := m.Fields[49]
	return v
}

func (m *FIXMessage) TargetCompID() string {
	v, _ := m.Fields[56]
	return v
}

func (m *FIXMessage) Serialize() []byte {
	var buf bytes.Buffer

	keys := sortedKeys(m.Fields)

	for _, tag := range keys {
		buf.WriteString(strconv.Itoa(tag))
		buf.WriteByte('=')
		buf.WriteString(m.Fields[tag])
		buf.WriteByte(SOH)
	}

	return buf.Bytes()
}

func (m *FIXMessage) Build(targetCompID string) []byte {
	m.Set(8, BeginString)
	m.Set(49, targetCompID)
	m.Set(52, time.Now().UTC().Format("20060102-15:04:05.000"))

	body := m.Serialize()

	checksum := calcChecksum(body)

	var msg bytes.Buffer
	msg.Write(body)
	msg.WriteString("10=")
	msg.WriteString(fmt.Sprintf("%03d", checksum))
	msg.WriteByte(SOH)

	return msg.Bytes()
}

func ParseFIXMessage(raw []byte) (*FIXMessage, error) {
	msg := NewFIXMessage()

	parts := bytes.Split(raw, []byte{SOH})
	for _, part := range parts {
		if len(part) == 0 {
			continue
		}
		idx := bytes.IndexByte(part, '=')
		if idx < 0 {
			continue
		}
		tag, err := strconv.Atoi(string(part[:idx]))
		if err != nil {
			continue
		}
		value := string(part[idx+1:])
		msg.Fields[tag] = value
	}

	return msg, nil
}

func ReadFIXMessage(buf *bytes.Buffer) (*FIXMessage, int, error) {
	data := buf.Bytes()

	idx := bytes.LastIndex(data, []byte("10="))
	if idx < 0 {
		return nil, 0, nil
	}

	endIdx := bytes.IndexByte(data[idx:], SOH)
	if endIdx < 0 {
		return nil, 0, nil
	}

	msgLen := idx + endIdx + 1
	raw := data[:msgLen]

	beginIdx := bytes.Index(raw, []byte("8="))
	if beginIdx < 0 {
		return nil, 0, fmt.Errorf("invalid FIX message: no BeginString")
	}

	msg, err := ParseFIXMessage(raw[beginIdx:])
	if err != nil {
		return nil, 0, err
	}

	return msg, msgLen, nil
}

func calcChecksum(data []byte) int {
	sum := 0
	for _, b := range data {
		sum += int(b)
	}
	return sum % 256
}

func sortedKeys(m map[int]string) []int {
	keys := make([]int, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}

	tagOrder := map[int]int{
		8:  0,
		9:  1,
		35: 2,
		49: 3,
		56: 4,
		34: 5,
		52: 6,
	}

	for i := 0; i < len(keys); i++ {
		for j := i + 1; j < len(keys); j++ {
			oi, hasOi := tagOrder[keys[i]]
			oj, hasOj := tagOrder[keys[j]]

			if hasOi && hasOj {
				if oi > oj {
					keys[i], keys[j] = keys[j], keys[i]
				}
			} else if hasOi {
				// keep i before j
			} else if hasOj {
				keys[i], keys[j] = keys[j], keys[i]
			} else {
				if keys[i] > keys[j] {
					keys[i], keys[j] = keys[j], keys[i]
				}
			}
		}
	}

	return keys
}

func MakeLogonResponse(senderCompID, targetCompID string, seqNum int, heartBtInt int) []byte {
	msg := NewFIXMessage()
	msg.Set(35, MsgTypeLogon)
	msg.Set(98, "0")
	msg.Set(108, strconv.Itoa(heartBtInt))
	msg.Set(34, strconv.Itoa(seqNum))
	return msg.Build(targetCompID)
}

func MakeLogout(senderCompID, targetCompID, text string, seqNum int) []byte {
	msg := NewFIXMessage()
	msg.Set(35, MsgTypeLogout)
	msg.Set(58, text)
	msg.Set(34, strconv.Itoa(seqNum))
	return msg.Build(targetCompID)
}

func MakeHeartbeat(senderCompID, targetCompID string, seqNum int, testReqID string) []byte {
	msg := NewFIXMessage()
	msg.Set(35, MsgTypeHeartbeat)
	if testReqID != "" {
		msg.Set(112, testReqID)
	}
	msg.Set(34, strconv.Itoa(seqNum))
	return msg.Build(targetCompID)
}

func MakeExecutionReport(senderCompID, targetCompID string, seqNum int, clOrdID, symbol, side, ordType string,
	price, qty, cumQty, avgPx int64, orderID uint64, ordStatus, text string) []byte {

	msg := NewFIXMessage()
	msg.Set(35, MsgTypeExecutionReport)
	msg.Set(37, strconv.FormatUint(orderID, 10))
	msg.Set(11, clOrdID)
	msg.Set(55, symbol)
	msg.Set(54, side)
	msg.Set(40, ordType)
	msg.Set(44, strconv.FormatInt(price, 10))
	msg.Set(38, strconv.FormatInt(qty, 10))
	msg.Set(14, strconv.FormatInt(cumQty, 10))
	msg.Set(6, strconv.FormatInt(avgPx, 10))
	msg.Set(150, ordStatus)
	msg.Set(39, ordStatus)
	if text != "" {
		msg.Set(58, text)
	}
	msg.Set(34, strconv.Itoa(seqNum))
	return msg.Build(targetCompID)
}

func MakeOrderCancelReject(senderCompID, targetCompID string, seqNum int, clOrdID string, orderID uint64, reason string) []byte {
	msg := NewFIXMessage()
	msg.Set(35, MsgTypeOrderCancelReject)
	msg.Set(11, clOrdID)
	msg.Set(37, strconv.FormatUint(orderID, 10))
	msg.Set(58, reason)
	msg.Set(34, strconv.Itoa(seqNum))
	return msg.Build(targetCompID)
}

func SideToString(s interface{}) string {
	switch v := s.(type) {
	case string:
		if v == "1" {
			return "1"
		}
		return "2"
	}
	return "1"
}

func ParseSide(s string) (int, error) {
	switch strings.TrimSpace(s) {
	case "1":
		return 1, nil
	case "2":
		return 2, nil
	default:
		return 0, fmt.Errorf("invalid side: %s", s)
	}
}
