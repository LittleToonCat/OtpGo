package database

import (
	"fmt"
	"os"
	"os/exec"
	"otpgo/core"
	"otpgo/messagedirector"
	. "otpgo/test"
	. "otpgo/util"
	"testing"
	"time"

	"github.com/apex/log"
)

type Generate struct {
	Min int
	Max int
}
type Backend struct {
	Type      string

	// MONGO BACKEND
	Server    string
	Database  string
}

func connect(ch Channel_t) *TestChannelConnection {
	conn := (&TestChannelConnection{}).Create("127.0.0.1:57123", fmt.Sprintf("Channel (%d)", ch), ch)
	conn.Timeout = 100
	return conn
}

func TestMain(m *testing.M) {
	log.SetLevel(log.DebugLevel)

	// SETUP

	// Create a temporary directory for Mongo
	dir, err := os.MkdirTemp("", "otpgo-mongodb")
	if err != nil {
		panic(err)
	}

	// Start new Mongo instance.
	mongoCmd := exec.Command("mongod",
						  "--noauth", "--quiet",
						  "--nojournal", "--noprealloc",
						  "--bind_ip", "127.0.0.1",
						  "--port", "57023",
						  "--dbpath", dir)
	if err := mongoCmd.Start(); err != nil {
		os.RemoveAll(dir)
		panic(err)
	}

	StartDaemon(
		core.ServerConfig{MessageDirector: struct {
			Bind    string
			Connect string
		}{Bind: "127.0.0.1:57123"},
			General: struct {
				Eventlogger string
				DC_Files    []string
			}{Eventlogger: "", DC_Files: []string{"../test/test.dc"}}})
	if err := core.LoadDC(); err != nil {
		os.Exit(1)
	}
	messagedirector.Start()
	time.Sleep(100 * time.Millisecond)

	db := NewDatabaseServer(core.Role{
		Control: 75757,
		Generate: Generate{1000000, 1000010},
		Backend: Backend{Type: "mongodb", Server: "mongodb://127.0.0.1:57023", Database: "test"}})

	db.objectTypes[1] = core.DC.Get_class_by_name("DistributedTestObject3")
	db.objectTypes[2] = core.DC.Get_class_by_name("DistributedTestObject5")
	db.objectTypes[3] = core.DC.Get_class_by_name("DistributedDBTypeTestObject")

	code := m.Run()

	// TEARDOWN

	// End Mongo process
	if err := mongoCmd.Process.Kill(); err != nil {
		log.Errorf("failed to kill process: %s", err.Error())
	}

	// Remove temporary directory
	os.RemoveAll(dir)

	os.Exit(code)
}

func TestMongo_CreateGetAll(t *testing.T) {
	conn := connect(20)

	dg := NewDatagram()
	dg.AddServerHeader(75757, 20, DBSERVER_CREATE_STORED_OBJECT)
	dg.AddUint32(1) // Context
	dg.AddString("") // unknown
	dg.AddUint16(1) // Object type 1 = DistributedTestObject3

	dg.AddUint16(1) // Count
	dg.AddString("setRDB3")
	valueDg := NewDatagram()
	valueDg.AddUint32(143)
	dg.AddBlob(&valueDg)

	conn.SendDatagram(dg)

	// We should receive a response with the object's doId:
	dg = NewDatagram()
	dg.AddServerHeader(20, 75757, DBSERVER_CREATE_STORED_OBJECT_RESP)
	dg.AddUint32(1)
	dg.AddUint8(0) // Return code
	dg.AddDoid(1000000)
	conn.Expect(t, dg, false)

	// Now we should get the values back from the database:
	dg = NewDatagram()
	dg.AddServerHeader(75757, 20, DBSERVER_GET_STORED_VALUES)
	dg.AddUint32(1) // Context
	dg.AddDoid(1000000)
	dg.AddUint16(1) // Count
	dg.AddString("setRDB3")
	conn.SendDatagram(dg)

	dg = NewDatagram()
	dg.AddServerHeader(20, 75757, DBSERVER_GET_STORED_VALUES_RESP)
	dg.AddUint32(1)
	dg.AddDoid(1000000)
	dg.AddUint16(1) // Count
	dg.AddString("setRDB3")
	dg.AddUint8(0) // Return code
	dg.AddBlob(&valueDg)
	dg.AddBool(true) // Found
	conn.Expect(t, dg, false)

	// Send an update
	dg = NewDatagram()
	dg.AddServerHeader(75757, 20, DBSERVER_SET_STORED_VALUES)
	dg.AddDoid(1000000)
	dg.AddUint16(1)
	dg.AddString("setRDB3")
	valueDg = NewDatagram()
	valueDg.AddUint32(341)
	dg.AddBlob(&valueDg)

	conn.SendDatagram(dg)

	// Wait a bit for the update to take place.
	time.Sleep(100 * time.Millisecond)

	// Now we should get an updated field.
	dg = NewDatagram()
	dg.AddServerHeader(75757, 20, DBSERVER_GET_STORED_VALUES)
	dg.AddUint32(1) // Context
	dg.AddDoid(1000000)
	dg.AddUint16(1) // Count
	dg.AddString("setRDB3")
	conn.SendDatagram(dg)

	dg = NewDatagram()
	dg.AddServerHeader(20, 75757, DBSERVER_GET_STORED_VALUES_RESP)
	dg.AddUint32(1)
	dg.AddDoid(1000000)
	dg.AddUint16(1) // Count
	dg.AddString("setRDB3")
	dg.AddUint8(0) // Return code
	dg.AddBlob(&valueDg)
	dg.AddBool(true) // Found
	conn.Expect(t, dg, false)

}
