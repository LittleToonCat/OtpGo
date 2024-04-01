package eventlogger

import (
	"fmt"
	"net"
	"os"
	"os/signal"
	"otpgo/core"
	. "otpgo/util"
	"syscall"
	"time"

	"github.com/apex/log"
	"github.com/jehiah/go-strftime"
)

var EventLoggerLog *log.Entry

var logfile *os.File
var server *net.UDPConn

func StartEventLogger(config core.Role) {
	if config.Output == "" {
		config.Output = "events-%Y%m%d-%H%M%S.log"
	}

	createLog(config)

	EventLoggerLog.Info("Opening UDP socket...")
	addr, err := net.ResolveUDPAddr("udp", config.Bind)
	if err != nil {
		EventLoggerLog.Fatalf("Unable to resolve UDP address \"%s\": %s", config.Bind, err)
	}

	server, err = net.ListenUDP("udp", addr)
	if err != nil {
		EventLoggerLog.Fatalf("Unable to open socket: %s", err)
	}
	EventLoggerLog.Infof("Opened UDP socket at %s", config.Bind)

	event := NewLoggedEvent("logOpened", "EventLogger", config.Bind, "Log opened upon Event Logger startup.")
	processLoggedEvent(event)

	handleInterrupts()
	go listen()
}

func createLog(config core.Role) {
	var err error
	if logfile != nil {
		logfile.Close()
	}

	t := time.Now()
	logfile, err = os.OpenFile(strftime.Format(config.Output, t), os.O_RDWR|os.O_CREATE, 0600)
	if err != nil {
		EventLoggerLog.Fatalf("failed to open logfile: %s", err)
		return
	}

	logfile.Truncate(0)
	logfile.Seek(0, 0)
}

func handleInterrupts() {
	c := make(chan os.Signal)
	signal.Notify(c, os.Interrupt, syscall.SIGTERM)
	go func() {
		<-c
		server.Close()
		logfile.Sync()
		logfile.Close()
	}()
}

func processLoggedEvent(le LoggedEvent) {
	timeStr := strftime.Format("%Y-%m-%d %H:%M:%S%z", time.Now())
	log := fmt.Sprintf("%s|%s|%s|%s|%s\n", timeStr, le.roleName, le.channel, le.eventType, le.description)

	_, err := logfile.WriteString(log)
	if err != nil {
		EventLoggerLog.Fatalf("failed to write to logfile: %s", err)
	}
	logfile.Sync()
}

func processPacket(dg Datagram, addr *net.UDPAddr) {
	dgi := NewDatagramIterator(&dg)

	defer func() {
		if r := recover(); r != nil {
			if _, ok := r.(DatagramIteratorEOF); ok {
				EventLoggerLog.Error("Reached end of datagram")
			}
		}
	}()

	// Skip length
	dgi.Skip(2)
	messageType := dgi.ReadUint16()
	serverType := dgi.ReadUint16()
	fromChannel := dgi.ReadUint32()

	var serverTypeString string
	switch serverType {
	case 1:
		serverTypeString = "MessageDirector"
	case 2:
		serverTypeString = "StateServer"
	case 3:
		serverTypeString = "ClientAgent"
	case 4:
		serverTypeString = "Client"
	case 5:
		serverTypeString = "DatabaseServer"
	case 6:
		serverTypeString = fmt.Sprintf("AIEvent:%d", fromChannel)
	case 7:
		// Other
		serverTypeString = dgi.ReadString()
	default:
		serverTypeString = fmt.Sprintf("UnknownEvent:%d", serverType)
	}

	var who string
	var eventType string
	var description string
	switch messageType {
	case 1:
		eventType = dgi.ReadString()
		who = dgi.ReadString()
		description = dgi.ReadString()
	case 2:
		eventType = "ServerStatus"
		who = dgi.ReadString()
		avatarCount := dgi.ReadUint32()
		objectCount := dgi.ReadUint32()
		description = fmt.Sprintf("Avatars:%d|TotalObjects:%d", avatarCount, objectCount)
	default:
		EventLoggerLog.Errorf("Received unknown message type: %d", messageType)
		return
	}

	le := NewLoggedEvent(eventType, serverTypeString, who, description)
	processLoggedEvent(le)
}

func listen() {
	buff := make([]byte, 1024)
	for {
		n, addr, err := server.ReadFromUDP(buff)
		if err != nil {
			// If the socket is unreadable the daemon is probably closed
			break
		}

		dg := NewDatagram()
		dg.Write(buff[0:n])

		processPacket(dg, addr)
	}
}

func init() {
	EventLoggerLog = log.WithFields(log.Fields{
		"name": "EventLogger",
		"modName": "EventLogger",
	})

	senderLog = log.WithFields(log.Fields{
		"name": "EventSender",
		"modName": "EventSender",
	})
}
