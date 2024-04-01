package eventlogger

import (
	"net"
	. "otpgo/util"

	"github.com/apex/log"
)

var senderSocket *net.UDPConn
var senderLog    *log.Entry


type LoggedEvent struct {
	eventType string
	roleName string
	channel string
	description string
}

func NewLoggedEvent(eventType string, roleName string, channel string, description string) LoggedEvent {
	le := &LoggedEvent{
		eventType: eventType,
		roleName: roleName,
		channel: channel,
		description: description,
	}
	return *le
}

func (l LoggedEvent) Send() {
	// processLoggedEvent(l)
	if senderSocket == nil {
		senderLog.Debug("Not active, not sending.")
		return
	}

	var serverType uint16
	switch l.roleName {
	case "MessageDirector":
		serverType = 1
	case "StateServer":
		serverType = 2
	case "ClientAgent":
		serverType = 3
	case "Client":
		serverType = 4
	case "DatabaseServer":
		serverType = 5
	case "AIEvent":
		serverType = 6
	default:
		// Other
		serverType = 7
	}

	eventDg := NewDatagram()
	eventDg.AddUint16(1) // message type
	eventDg.AddUint16(serverType) // serverType
	eventDg.AddUint32(0) // fromChannel
	if serverType == 7 {
		eventDg.AddString(l.roleName) // roleName
	}
	eventDg.AddString(l.eventType) // eventType
	eventDg.AddString(l.channel) // who
	eventDg.AddString(l.description) // description

	dg := NewDatagram()
	dg.AddBlob(&eventDg)

	_, err := senderSocket.Write(dg.Bytes())
	if err != nil {
		senderLog.Errorf("Error when attempting to write to UDP socket: %s", err.Error())
	}

}

func StartEventSender(address string) {
	if senderSocket != nil {
		return
	}

	if address == "" {
		senderLog.Debug("Not enabled.")
	}

	addr, err := net.ResolveUDPAddr("udp", address)
	if err != nil {
		senderLog.Fatalf("Unable to resolve UDP address \"%s\": %s", address, err.Error())
	}

	senderSocket, err = net.DialUDP("udp", nil, addr)
	if err != nil {
		senderLog.Fatalf("Unable to dial to UDP address \"%s\": %s", address, err.Error())
	}

	senderLog.Debug("Started.")

}
