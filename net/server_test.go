package net

import (
	"fmt"
	"github.com/stretchr/testify/require"
	"io/ioutil"
	"net"
	"testing"
)

var serv NetworkServer
var msgChan chan string

type FakeServer struct {
	Server
}

func (s FakeServer) HandleConnect(conn net.Conn) {
	buf, err := ioutil.ReadAll(conn)
	if err != nil {
		msgChan <- ""
	} else {
		msgChan <- string(buf)
	}
}

func TestNetworkServer_Start(t *testing.T) {
	defer serv.Shutdown()

	errChan := make(chan error)
	go serv.Start("0:0", errChan)
	require.NotNil(t, <-errChan)
	serv.Shutdown()

	go serv.Start("127.0.0.1:7198", errChan)
	require.Nil(t, <-errChan)
}

func TestNetworkServer_Listen(t *testing.T) {
	msgChan = make(chan string)
	errChan := make(chan error)
	go serv.Start(":7198", errChan)
	<-errChan

	go func() {
		conn, err := net.Dial("tcp", ":7198")
		if err != nil {
			t.Fatal(err)
		}
		defer conn.Close()

		if _, err := fmt.Fprintf(conn, "test123"); err != nil {
			t.Fatal(err)
		}
	}()

	require.EqualValues(t, <-msgChan, "test123")
}

func init() {
	serv.Handler = FakeServer{}
}
