package stateserver

import (
	"encoding/hex"
	"fmt"
	"os"
	"otpgo/core"
	"otpgo/messagedirector"
	. "otpgo/test"
	. "otpgo/util"
	"testing"
	"time"

	"github.com/apex/log"
	"github.com/tj/assert"
)

func connect(ch Channel_t) *TestChannelConnection {
	conn := (&TestChannelConnection{}).Create("127.0.0.1:57123", fmt.Sprintf("Channel (%d)", ch), ch)
	conn.Timeout = 100
	return conn
}

func appendMeta(dg *Datagram, doid Doid_t, parent Doid_t, zone Zone_t, dclass uint16) {
	if doid != 6969 {
		dg.AddDoid(doid)
	}
	if parent != 6969 {
		dg.AddDoid(parent)
	}
	if zone != 6969 {
		dg.AddZone(zone)
	}
	if dclass != 6969 {
		dg.AddUint16(dclass)
	}
}

func appendMetaDoidLast(dg *Datagram, doid Doid_t, parent Doid_t, zone Zone_t, dclass uint16) {
	if parent != 6969 {
		dg.AddDoid(parent)
	}
	if zone != 6969 {
		dg.AddZone(zone)
	}
	if dclass != 6969 {
		dg.AddUint16(dclass)
	}
	if doid != 6969 {
		dg.AddDoid(doid)
	}
}

func instantiateObject(conn *TestChannelConnection, sender Channel_t, doid Doid_t, parent Doid_t, zone Zone_t, required uint16) {
	dg := (&TestDatagram{}).Create([]Channel_t{100100}, sender, STATESERVER_OBJECT_GENERATE_WITH_REQUIRED)
	appendMetaDoidLast(dg, doid, parent, zone, DistributedTestObject1)
	dg.AddUint32(uint32(required))
	conn.SendDatagram(*dg)
	time.Sleep(10 * time.Millisecond)
}

func deleteObject(conn *TestChannelConnection, sender Channel_t, doid Doid_t) {
	dg := (&TestDatagram{}).Create([]Channel_t{Channel_t(doid)}, sender, STATESERVER_OBJECT_DELETE_RAM)
	dg.AddDoid(doid)
	conn.SendDatagram(*dg)
}

func TestMain(m *testing.M) {
	// SETUP
	log.SetLevel(log.DebugLevel)

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

	NewStateServer(core.Role{Control: 100100})

	NewDatabaseStateServer(core.Role{Database: 1200, Ranges: struct {
		Min Channel_t
		Max Channel_t
	}{9000, 9999}})

	code := m.Run()

	// TEARDOWN
	os.Exit(code)
}

func TestStateServer_CreateDelete(t *testing.T) {
	ai, parent, children :=
		connect(LocationAsChannel(5000, 1500)),
		connect(5000),
		connect(ParentToChildren(101000000))
	children.Timeout = 400

	test := func() {
		instantiateObject(ai, 5, 101000000, 5000, 1500, 1337)

		var received bool
		parent.Timeout = 300
		for n := 0; n < 2; n++ {
			dg := parent.ReceiveMaybe()
			assert.True(t, dg != nil, "Parent did not receive ChangingLocation and/or GetAI")
			dgi := (&TestDatagram{}).Set(dg)
			msgType := dgi.MessageType()
			// Object should tell the parent that it's arriving
			if ok, why := dgi.MatchesHeader([]Channel_t{5000}, 5, STATESERVER_OBJECT_CHANGE_ZONE, -1); ok {
				received = true
				dgi.SeekPayload()
				dgi.ReadChannel()                                  // Sender
				dgi.ReadUint16()                                   // Message type
				assert.Equal(t, dgi.ReadDoid(), Doid_t(101000000)) // ID
				assert.Equal(t, dgi.ReadDoid(), Doid_t(5000))      // New Parent
				assert.Equal(t, dgi.ReadZone(), Zone_t(1500))      // New Zone
				assert.Equal(t, dgi.ReadDoid(), INVALID_DOID)      // Old Parent
				assert.Equal(t, dgi.ReadZone(), INVALID_ZONE)      // Old Zone
				// Object should also ask for its AI, which we are not testing here
			} else if ok, why = dgi.MatchesHeader([]Channel_t{5000}, 101000000, STATESERVER_OBJECT_GET_AI, -1); ok {
				continue
			} else {
				t.Errorf("Failed object creation test! (msgtype=%d): %s\nDatagram dump:\n%s", msgType, why, hex.Dump(dgi.Dg.Bytes()))
			}
		}

		assert.True(t, received)

		// Object should announce its entry to the zone
		dg := (&TestDatagram{}).Create([]Channel_t{LocationAsChannel(5000, 1500)},
			101000000, STATESERVER_OBJECT_ENTER_LOCATION_WITH_REQUIRED)
		appendMeta(dg, 101000000, 5000, 1500, DistributedTestObject1)
		dg.AddUint32(1337)
		ai.Expect(t, *dg, false)

		dg = (&TestDatagram{}).Create([]Channel_t{ParentToChildren(101000000)},
			101000000, STATESERVER_OBJECT_LOCATE)
		dg.AddUint32(STATESERVER_CONTEXT_WAKE_CHILDREN)
		children.Expect(t, *dg, false)

		// Test for DeleteRam
		deleteObject(ai, 5, 101000000)

		// Object should inform the parent that it is leaving
		dg = (&TestDatagram{}).Create([]Channel_t{5000}, 5, STATESERVER_OBJECT_CHANGE_ZONE)
		dg.AddDoid(101000000)
		appendMeta(dg, 6969, INVALID_DOID, INVALID_ZONE, 6969) // New Location
		appendMeta(dg, 6969, 5000, 1500, 6969)                 // Old Location
		parent.Expect(t, *dg, false)

		// Object should announce it's disappearance
		dg = (&TestDatagram{}).Create([]Channel_t{LocationAsChannel(5000, 1500)},
			5, STATESERVER_OBJECT_DELETE_RAM)
		dg.AddDoid(101000000)
		ai.Expect(t, *dg, false)
	}

	// Run tests twice to verify that ids can be reused
	test()
	test()

	// Cleanup
	parent.Close()
	ai.Close()
	children.Close()
}

func TestStateServer_Broadcast(t *testing.T) {
	ai := connect(LocationAsChannel(5000, 1500))

	// Broadcast for location test
	// Start with the creation of a DTO2
	dg := (&TestDatagram{}).Create([]Channel_t{100100}, 5, STATESERVER_OBJECT_GENERATE_WITH_REQUIRED)
	appendMetaDoidLast(dg, 101000001, 5000, 1500, DistributedTestObject2)
	ai.SendDatagram(*dg)

	// Ignore the entry message
	time.Sleep(50 * time.Millisecond)
	ai.Flush()

	// Send an update to setB2
	dg = (&TestDatagram{}).Create([]Channel_t{101000001}, 5, STATESERVER_OBJECT_UPDATE_FIELD)
	dg.AddDoid(101000001)
	dg.AddUint16(SetB2)
	dg.AddUint32(0xDEADBEEF)
	ai.SendDatagram(*dg)

	// Object should broadcast the update
	dg = (&TestDatagram{}).Create([]Channel_t{LocationAsChannel(5000, 1500)}, 5, STATESERVER_OBJECT_UPDATE_FIELD)
	dg.AddDoid(101000001)
	dg.AddUint16(SetB2)
	dg.AddUint32(0xDEADBEEF)
	ai.Expect(t, *dg, false)

	// Cleanup
	deleteObject(ai, 5, 101000001)
	ai.Close()
}

func TestStateServer_Airecv(t *testing.T) {
	conn := connect(5)
	conn.AddChannel(1300)

	instantiateObject(conn, 5, 101000002, 5000, 1500, 1337)
	time.Sleep(100 * time.Millisecond)

	dg := (&TestDatagram{}).Create([]Channel_t{101000002}, 5, STATESERVER_ADD_AI_RECV)
	dg.AddChannel(1300)
	conn.SendDatagram(*dg)
	time.Sleep(100 * time.Millisecond)
	conn.Flush()

	dg = (&TestDatagram{}).Create([]Channel_t{101000002}, 5, STATESERVER_OBJECT_UPDATE_FIELD)
	dg.AddDoid(101000002)
	dg.AddUint16(SetBA1)
	dg.AddUint16(0xF00D)
	conn.SendDatagram(*dg)

	// AI should receive the message
	dg = (&TestDatagram{}).Create([]Channel_t{LocationAsChannel(5000, 1500), 1300}, 5, STATESERVER_OBJECT_UPDATE_FIELD)
	dg.AddDoid(101000002)
	dg.AddUint16(SetBA1)
	dg.AddUint16(0xF00D)
	conn.Expect(t, *dg, false)

	// AI should not get its own reflected messages back
	dg = (&TestDatagram{}).Create([]Channel_t{101000002}, 1300, STATESERVER_OBJECT_UPDATE_FIELD)
	dg.AddDoid(101000002)
	dg.AddUint16(SetBA1)
	dg.AddUint16(0xF00D)
	conn.SendDatagram(*dg)

	conn.ExpectNone(t)

	// Test for AI notification of object deletion
	deleteObject(conn, 5, 101000002)

	// AI should receive the delete
	dg = (&TestDatagram{}).Create([]Channel_t{LocationAsChannel(5000, 1500), 1300}, 5, STATESERVER_OBJECT_DELETE_RAM)
	dg.AddDoid(101000002)
	conn.Expect(t, *dg, false)

	// OtpGo specific change:
	// Objects ending in "District" will get their AI channel set based on sender.
	dg = (&TestDatagram{}).Create([]Channel_t{100100}, 1300, STATESERVER_OBJECT_GENERATE_WITH_REQUIRED)
	appendMetaDoidLast(dg, 101000003, 5000, 1500, District)
	dg.AddUint32(143)
	conn.SendDatagram(*dg)
	// If the AI channel matches the sender, it should not send a entry message.
	conn.ExpectNone(t)

	dg = (&TestDatagram{}).Create([]Channel_t{101000003}, 5, STATESERVER_OBJECT_UPDATE_FIELD)
	dg.AddDoid(101000003)
	dg.AddUint16(SetBA1)
	dg.AddUint16(0xF00D)
	conn.SendDatagram(*dg)

	// AI should receive the message
	dg = (&TestDatagram{}).Create([]Channel_t{LocationAsChannel(5000, 1500), 1300}, 5, STATESERVER_OBJECT_UPDATE_FIELD)
	dg.AddDoid(101000003)
	dg.AddUint16(SetBA1)
	dg.AddUint16(0xF00D)
	conn.Expect(t, *dg, false)

	// AI should not get its own reflected messages back
	dg = (&TestDatagram{}).Create([]Channel_t{101000003}, 1300, STATESERVER_OBJECT_UPDATE_FIELD)
	dg.AddDoid(101000003)
	dg.AddUint16(SetBA1)
	dg.AddUint16(0xF00D)
	conn.SendDatagram(*dg)

	conn.ExpectNone(t)

	// Test for AI notification of object deletion
	deleteObject(conn, 5, 101000003)

	// AI should receive the delete
	dg = (&TestDatagram{}).Create([]Channel_t{LocationAsChannel(5000, 1500), 1300}, 5, STATESERVER_OBJECT_DELETE_RAM)
	dg.AddDoid(101000003)
	conn.Expect(t, *dg, false)

	conn.Close()
}

func TestStateServer_SetAI(t *testing.T) {
	conn := connect(5)
	conn.AddChannel(0)

	ai1Chan, ai2Chan := Channel_t(1000), Channel_t(2000)
	ai1, ai2 := connect(ai1Chan), connect(ai2Chan)
	do1, do2 := Channel_t(133337), Channel_t(133338)
	obj1, obj2 := connect(do1), connect(do2)
	children1, children2 := connect(ParentToChildren(Doid_t(do1))), connect(ParentToChildren(Doid_t(do2)))

	// Test for an object without children, AI, or optional fields
	instantiateObject(conn, 5, Doid_t(do1), 0, 0, 1337)
	conn.ExpectNone(t)
	time.Sleep(50 * time.Millisecond)
	children1.Flush()

	// Give DO #1 to AI #1
	dg := (&TestDatagram{}).Create([]Channel_t{do1}, 5, STATESERVER_ADD_AI_RECV)
	dg.AddChannel(ai1Chan)
	conn.SendDatagram(*dg)
	time.Sleep(50 * time.Millisecond)
	obj1.Flush()

	// DO #1 should announce its presence to AI #1
	dg = (&TestDatagram{}).Create([]Channel_t{ai1Chan}, do1, STATESERVER_OBJECT_ENTER_AI_RECV)
	dg.AddUint32(0)
	appendMetaDoidLast(dg, Doid_t(do1), 0, 0, DistributedTestObject1)
	dg.AddUint32(1337)
	dg.AddUint16(0)
	ai1.Expect(t, *dg, false)
	children1.ExpectNone(t)

	// Test for an object with an existing AI
	// Give DO #1 to AI #2
	dg = (&TestDatagram{}).Create([]Channel_t{do1}, 5, STATESERVER_ADD_AI_RECV)
	dg.AddChannel(ai2Chan)
	conn.SendDatagram(*dg)
	time.Sleep(50 * time.Millisecond)
	obj1.Flush()

	// DO #1 should tell AI #1 that it is changing AI
	dg = (&TestDatagram{}).Create([]Channel_t{ai1Chan}, 5, STATESERVER_OBJECT_LEAVING_AI_INTEREST)
	dg.AddDoid(Doid_t(do1)) // ID
	dg.AddChannel(ai2Chan)  // New AI
	dg.AddChannel(ai1Chan)  // Old AI
	ai1.Expect(t, *dg, false)

	// It should also inform AI #2 that it is entering
	dg = (&TestDatagram{}).Create([]Channel_t{ai2Chan}, do1, STATESERVER_OBJECT_ENTER_AI_RECV)
	dg.AddUint32(0)
	appendMetaDoidLast(dg, Doid_t(do1), 0, 0, DistributedTestObject1)
	dg.AddUint32(1337)
	dg.AddUint16(0)
	ai2.Expect(t, *dg, false)

	// Test for child AI handling on creation
	// Instantiate a new object beneath DO #1
	instantiateObject(conn, 5, Doid_t(do2), Doid_t(do1), 1500, 1337)

	// DO #1 should receive two messages from the child
	var context uint32
	for n := 0; n < 2; n++ {
		dg = obj1.ReceiveMaybe()
		assert.True(t, dg != nil, "Parent did not receive ChangingLocation and/or GetAI")
		dgi := (&TestDatagram{}).Set(dg)
		if ok, _ := dgi.MatchesHeader([]Channel_t{do1}, do2, STATESERVER_OBJECT_GET_AI, -1); ok {
			context = dgi.ReadUint32()
		} else if ok, _ := dgi.MatchesHeader([]Channel_t{do1}, 5, STATESERVER_OBJECT_CHANGE_ZONE, -1); ok {
			continue
		} else {
			t.Error("Received unexpected or non-matching header")
		}
	}

	// The parent should reply with AI #2 and a location acknowledgement
	dg = (&TestDatagram{}).Create([]Channel_t{do2}, do1, STATESERVER_OBJECT_GET_AI_RESP)
	dg.AddUint32(context)
	dg.AddDoid(Doid_t(do1))
	dg.AddChannel(ai2Chan)
	ack := (&TestDatagram{}).Create([]Channel_t{do2}, do1, STATESERVER_OBJECT_LOCATION_ACK)
	ack.AddDoid(Doid_t(do1))
	ack.AddZone(1500)
	obj2.ExpectMany(t, []Datagram{*dg, *ack}, false, true)

	// We should also receive a wake children message
	dg = (&TestDatagram{}).Create([]Channel_t{ParentToChildren(Doid_t(do2))}, do2, STATESERVER_OBJECT_LOCATE)
	dg.AddUint32(STATESERVER_CONTEXT_WAKE_CHILDREN)
	children2.Expect(t, *dg, false)

	// DO #2 should the announce its presence to AI #2
	dg = (&TestDatagram{}).Create([]Channel_t{ai2Chan}, do2, STATESERVER_OBJECT_ENTER_AI_RECV)
	dg.AddUint32(0)
	appendMetaDoidLast(dg, Doid_t(do2), Doid_t(do1), 1500, DistributedTestObject1)
	dg.AddUint32(1337)
	dg.AddUint16(0)
	ai2.Expect(t, *dg, false)
	children2.ExpectNone(t)

	// Test for child AI handling on reparent
	// Delete DO #2 and close our channel to it to let it generate for the next test
	deleteObject(conn, 5, Doid_t(do2))
	obj2.Close()
	time.Sleep(50 * time.Millisecond)
	children2.Flush()
	obj1.Flush()
	obj2.Flush()
	ai2.Flush()

	// Recreate DO #2 w/o a parent
	instantiateObject(conn, 5, Doid_t(do2), 0, 0, 1337)

	// Set the location of DO #2 to a zone of the first object
	dg = (&TestDatagram{}).Create([]Channel_t{do2}, 5, STATESERVER_OBJECT_SET_ZONE)
	appendMeta(dg, 6969, Doid_t(do1), 1500, 6969)
	conn.SendDatagram(*dg)

	// Ignore location change messages
	time.Sleep(10 * time.Millisecond)
	obj2.Flush()
	conn.Flush()
	children2.Flush()

	// DO #1 is expecting two messages from the child
	for n := 0; n < 2; n++ {
		obj1.Timeout = 2000
		dg = obj1.ReceiveMaybe()
		assert.True(t, dg != nil, "Parent did not receive ChangingLocation and/or GetAI")
		dgi := (&TestDatagram{}).Set(dg)
		if ok, _ := dgi.MatchesHeader([]Channel_t{do1}, do2, STATESERVER_OBJECT_GET_AI, -1); ok {
			context = dgi.ReadUint32()
		} else if ok, _ := dgi.MatchesHeader([]Channel_t{do1}, 5, STATESERVER_OBJECT_CHANGE_ZONE, -1); ok {
			continue
		} else {
			t.Error("Received unexpected or non-matching header")
		}
	}

	// DO #2 should also announce its presence to AI #2
	dg = (&TestDatagram{}).Create([]Channel_t{ai2Chan}, do2, STATESERVER_OBJECT_ENTER_AI_RECV)
	dg.AddUint32(0)
	appendMetaDoidLast(dg, Doid_t(do2), Doid_t(do1), 1500, DistributedTestObject1)
	dg.AddUint32(1337)
	dg.AddUint16(0)
	ai2.Expect(t, *dg, false)
	children2.ExpectNone(t)

	// Test for an object w/ children
	dg = (&TestDatagram{}).Create([]Channel_t{do1}, 5, STATESERVER_ADD_AI_RECV)
	dg.AddChannel(ai1Chan)
	conn.SendDatagram(*dg)
	time.Sleep(20 * time.Millisecond)
	obj1.Flush()

	var ai1Expected, ai2Expected []Datagram
	// DO #1 should tell AI #2 that it is changing to AI #1
	dg = (&TestDatagram{}).Create([]Channel_t{ai2Chan, ParentToChildren(Doid_t(do1))}, 5, STATESERVER_OBJECT_LEAVING_AI_INTEREST)
	dg.AddDoid(Doid_t(do1)) // ID
	dg.AddChannel(ai1Chan)  // New AI
	dg.AddChannel(ai2Chan)  // Old AI
	children1.Expect(t, *dg, false)
	ai2Expected = append(ai2Expected, *dg)

	// It should also tell AI #2 that it is entering
	dg = (&TestDatagram{}).Create([]Channel_t{ai1Chan}, do1, STATESERVER_OBJECT_ENTER_AI_RECV)
	dg.AddUint32(0)
	appendMetaDoidLast(dg, Doid_t(do1), 0, 0, DistributedTestObject1)
	dg.AddUint32(1337)
	dg.AddUint16(0)
	ai1Expected = append(ai1Expected, *dg)

	// DO #2 will also tell AI #2 that it is changing AI
	dg = (&TestDatagram{}).Create([]Channel_t{ai2Chan}, 5, STATESERVER_OBJECT_LEAVING_AI_INTEREST)
	dg.AddDoid(Doid_t(do2)) // ID
	dg.AddChannel(ai1Chan)  // New AI
	dg.AddChannel(ai2Chan)  // Old AI
	children2.ExpectNone(t)
	ai2Expected = append(ai2Expected, *dg)

	// It should also tell AI #2 that it is entering
	dg = (&TestDatagram{}).Create([]Channel_t{ai1Chan}, do2, STATESERVER_OBJECT_ENTER_AI_RECV)
	dg.AddUint32(0)
	appendMetaDoidLast(dg, Doid_t(do2), Doid_t(do1), 1500, DistributedTestObject1)
	dg.AddUint32(1337)
	dg.AddUint16(0)
	ai1Expected = append(ai1Expected, *dg)

	ai1.ExpectMany(t, ai1Expected, false, true)
	ai2.ExpectMany(t, ai2Expected, false, true)

	// Tests for various corner cases
	// A notification with an incorrect parent should do nothing
	obj2 = connect(do2)
	time.Sleep(10 * time.Millisecond)

	dg = (&TestDatagram{}).Create([]Channel_t{do2}, 0xDEADBEEF, STATESERVER_OBJECT_LEAVING_AI_INTEREST)
	dg.AddDoid(0xDEADBEEF)
	dg.AddChannel(0xBEEFDEAD) // New AI
	dg.AddChannel(ai1Chan)    // Old AI
	conn.SendDatagram(*dg)
	obj2.Expect(t, *dg, false)
	obj2.ExpectNone(t)
	ai1.ExpectNone(t)
	obj1.ExpectNone(t)
	children2.ExpectNone(t)

	// A notification with the same AI channel should do nothing
	dg = (&TestDatagram{}).Create([]Channel_t{do2}, do1, STATESERVER_OBJECT_LEAVING_AI_INTEREST)
	dg.AddDoid(Doid_t(do1))
	dg.AddChannel(ai1Chan)
	dg.AddChannel(ai1Chan)
	obj1.SendDatagram(*dg)
	obj2.Expect(t, *dg, false)
	obj2.ExpectNone(t)
	ai1.ExpectNone(t)
	obj1.ExpectNone(t)
	children2.ExpectNone(t)

	// Test for AI enter with other fields
	obj2.Close()
	deleteObject(conn, 5, Doid_t(do2))
	time.Sleep(10 * time.Millisecond)
	children2.Flush()
	obj1.Flush()
	obj2.Flush()
	ai2.Flush()
	ai1.Flush()

	// Recreate the second object with an optional field
	dg = (&TestDatagram{}).Create([]Channel_t{100100}, 5, STATESERVER_OBJECT_GENERATE_WITH_REQUIRED_OTHER)
	appendMetaDoidLast(dg, Doid_t(do2), 0, 0, DistributedTestObject1)
	dg.AddUint32(1337) // setRequired1
	dg.AddUint16(1)    // Optional fields
	dg.AddUint16(SetBR1)
	dg.AddString("Listen to Todd Edwards if you want some bops <3")
	conn.SendDatagram(*dg)

	// Set the AI of DO #2
	dg = (&TestDatagram{}).Create([]Channel_t{do2}, 5, STATESERVER_OBJECT_)
	dg.AddChannel(ai1Chan)
	conn.SendDatagram(*dg)

	// Expect an _ENTER_AI_RECV with other fields
	dg = (&TestDatagram{}).Create([]Channel_t{ai1Chan}, do2, STATESERVER_OBJECT_ENTER_AI_RECV)
	dg.AddUint32(0)
	appendMetaDoidLast(dg, Doid_t(do2), 0, 0, DistributedTestObject1)
	dg.AddUint32(1337)
	dg.AddUint16(1)
	dg.AddUint16(SetBR1)
	dg.AddString("Listen to Todd Edwards if you want some bops <3")
	ai1.Expect(t, *dg, false)

	// Cleanup :D
	deleteObject(conn, 5, Doid_t(do1))
	deleteObject(conn, 5, Doid_t(do2))
	time.Sleep(10 * time.Millisecond)
	for _, conn := range []*TestChannelConnection{conn, ai1, ai2, obj1, obj2, children1, children2} {
		conn.Close()
	}
}

func TestStateServer_Ram(t *testing.T) {
	do := Channel_t(102000000)
	ai := connect(LocationAsChannel(15000, 7000))
	ai.AddChannel(LocationAsChannel(15000, 4000))

	// Test that RAM fields are remembered
	// Create the DO
	instantiateObject(ai, 5, Doid_t(do), 15000, 7000, 0xBEEF)

	// See if it shows up
	dg := (&TestDatagram{}).Create([]Channel_t{LocationAsChannel(15000, 7000)},
		do, STATESERVER_OBJECT_ENTER_LOCATION_WITH_REQUIRED)
	appendMeta(dg, Doid_t(do), 15000, 7000, DistributedTestObject1)
	dg.AddUint32(0xBEEF)
	ai.Expect(t, *dg, false)

	// We don't need this location anymore
	ai.RemoveChannel(LocationAsChannel(15000, 7000))

	// Send a RAM update
	dg = (&TestDatagram{}).Create([]Channel_t{do}, 5, STATESERVER_OBJECT_UPDATE_FIELD)
	dg.AddDoid(Doid_t(do))
	dg.AddUint16(SetBR1)
	dg.AddString("Stussy S")
	ai.SendDatagram(*dg)

	// Ignore the broadcasted update
	time.Sleep(10 * time.Millisecond)
	ai.Flush()

	// Move the DO to zone 4000
	dg = (&TestDatagram{}).Create([]Channel_t{do}, 5, STATESERVER_OBJECT_SET_ZONE)
	appendMeta(dg, 6969, 15000, 4000, 6969)
	ai.SendDatagram(*dg)

	// It should announce its entry with the RAM field included
	entryDg := (&TestDatagram{}).Create([]Channel_t{LocationAsChannel(15000, 4000)},
		do, STATESERVER_OBJECT_ENTER_LOCATION_WITH_REQUIRED_OTHER)
	appendMeta(entryDg, Doid_t(do), 15000, 4000, DistributedTestObject1)
	entryDg.AddUint32(0xBEEF)
	entryDg.AddUint16(1)
	entryDg.AddUint16(SetBR1)
	entryDg.AddString("Stussy S")
	// It will also send a CHANGING_LOCATION message to the parent
	locDg := (&TestDatagram{}).Create([]Channel_t{15000, LocationAsChannel(15000, 4000)},
		5, STATESERVER_OBJECT_CHANGE_ZONE)
	locDg.AddDoid(Doid_t(do))
	locDg.AddLocation(15000, 4000)
	locDg.AddLocation(15000, 7000)
	ai.ExpectMany(t, []Datagram{*entryDg, *locDg}, false, false)

	// Cleanup
	deleteObject(ai, 5, Doid_t(do))
	time.Sleep(10 * time.Millisecond)
	ai.Close()
}

func TestStateServer_SetLocation(t *testing.T) {
	conn := connect(5)

	do1, do2, do3 := Channel_t(0xDEAD), Channel_t(0xBEEF), Channel_t(0xABBA)
	obj1, obj2, obj3, location1, location3 :=
		connect(do1), connect(do2), connect(do3),
		connect(LocationAsChannel(Doid_t(do1), 10000)), connect(LocationAsChannel(Doid_t(do3), 20000))

	obj1.Flush()
	obj2.Flush()
	obj3.Flush()
	location1.Flush()
	location3.Flush()

	// Test location on an object without a previous location
	// Instantiate DO #2
	instantiateObject(conn, 5, Doid_t(do2), 0, 0, 0xD00D)
	conn.ExpectNone(t)

	// Set the object's location
	dg := (&TestDatagram{}).Create([]Channel_t{do2}, 5, STATESERVER_OBJECT_SET_ZONE)
	appendMeta(dg, 6969, Doid_t(do1), 10000, 6969)
	conn.SendDatagram(*dg)
	time.Sleep(10 * time.Millisecond)
	obj2.Flush()
	obj1.Flush()

	dg = (&TestDatagram{}).Create([]Channel_t{LocationAsChannel(Doid_t(do1), 10000)},
		do2, STATESERVER_OBJECT_ENTER_LOCATION_WITH_REQUIRED)
	appendMeta(dg, Doid_t(do2), Doid_t(do1), 10000, DistributedTestObject1)
	dg.AddUint32(0xD00D)
	location1.Expect(t, *dg, false)

	// Test location on an object with an existing location
	// Instantiate DO #3
	instantiateObject(conn, 5, Doid_t(do3), 0, 0, 0xD00F)
	conn.ExpectNone(t)

	// Move DO #2 into a zone of DO #3
	dg = (&TestDatagram{}).Create([]Channel_t{do2}, 5, STATESERVER_OBJECT_SET_ZONE)
	appendMeta(dg, 6969, Doid_t(do3), 20000, 6969)
	conn.SendDatagram(*dg)
	time.Sleep(10 * time.Millisecond)
	obj2.Flush()

	// DO #2 should announce its departure from its location
	dg = (&TestDatagram{}).Create([]Channel_t{do1, LocationAsChannel(Doid_t(do1), 10000), do3},
		5, STATESERVER_OBJECT_CHANGE_ZONE)
	dg.AddDoid(Doid_t(do2))
	appendMeta(dg, 6969, Doid_t(do3), 20000, 6969) // New location
	appendMeta(dg, 6969, Doid_t(do1), 10000, 6969) // Old location
	location1.Expect(t, *dg, false)
	obj1.Expect(t, *dg, false)
	obj3.ExpectMany(t, []Datagram{*dg}, false, true)
	time.Sleep(10 * time.Millisecond)
	obj3.Flush()

	// DO #2 should also announce its entry to the new location
	dg = (&TestDatagram{}).Create([]Channel_t{LocationAsChannel(Doid_t(do3), 20000)},
		do2, STATESERVER_OBJECT_ENTER_LOCATION_WITH_REQUIRED)
	appendMeta(dg, Doid_t(do2), Doid_t(do3), 20000, DistributedTestObject1)
	dg.AddUint32(0xD00D)
	location3.Expect(t, *dg, false)

	// Test for non-propagation of SetLocation
	// Move DO #3 to a new zone
	conn.AddChannel(ParentToChildren(Doid_t(do3)))
	dg = (&TestDatagram{}).Create([]Channel_t{do3}, 5, STATESERVER_OBJECT_SET_ZONE)
	appendMeta(dg, 6969, Doid_t(do1), 10000, 6969)
	conn.SendDatagram(*dg)
	time.Sleep(10 * time.Millisecond)
	obj3.Flush()

	// Children should receive no messages
	conn.ExpectNone(t)
	conn.RemoveChannel(ParentToChildren(Doid_t(do3)))

	// Only DO #3 should change zones
	dg = (&TestDatagram{}).Create([]Channel_t{LocationAsChannel(Doid_t(do1), 10000)},
		do3, STATESERVER_OBJECT_ENTER_LOCATION_WITH_REQUIRED)
	appendMeta(dg, Doid_t(do3), Doid_t(do1), 10000, DistributedTestObject1)
	dg.AddUint32(0xD00F)
	location1.Expect(t, *dg, false)
	location1.ExpectNone(t)
	location3.ExpectNone(t)

	// Test for SetLocation w/ an AI
	// Give DO #2 an AI
	aiChan := Channel_t(0xFF)
	conn.AddChannel(aiChan)
	dg = (&TestDatagram{}).Create([]Channel_t{do2}, 5, STATESERVER_ADD_AI_RECV)
	dg.AddChannel(aiChan)
	conn.SendDatagram(*dg)
	time.Sleep(10 * time.Millisecond)
	obj2.Flush()
	conn.Flush()

	// Change DO #2's location
	dg = (&TestDatagram{}).Create([]Channel_t{do2}, 5, STATESERVER_OBJECT_SET_ZONE)
	appendMeta(dg, 6969, INVALID_DOID, INVALID_ZONE, 6969)
	conn.SendDatagram(*dg)
	obj2.Flush()

	// Expect a CHANGING_LOCATION message on the AI channel
	dg = (&TestDatagram{}).Create([]Channel_t{aiChan, do3, LocationAsChannel(Doid_t(do3), 20000)},
		5, STATESERVER_OBJECT_CHANGE_ZONE)
	dg.AddDoid(Doid_t(do2))
	appendMeta(dg, 6969, INVALID_DOID, INVALID_ZONE, 6969)
	appendMeta(dg, 6969, Doid_t(do3), 20000, 6969)
	conn.Expect(t, *dg, false)

	conn.RemoveChannel(aiChan)
	time.Sleep(10 * time.Millisecond)
	location1.Flush()
	obj1.Flush()

	// Remove AI
	dg = (&TestDatagram{}).Create([]Channel_t{do2}, 5, STATESERVER_ADD_AI_RECV)
	dg.AddChannel(INVALID_CHANNEL)
	conn.SendDatagram(*dg)
	time.Sleep(10 * time.Millisecond)
	conn.Flush()
	obj2.Flush()
	location3.Flush()

	// Test for SetLocation w/ an owner
	// Give DO #2 an owner
	ownerChan := Channel_t(0xFC)
	conn.AddChannel(ownerChan)
	dg = (&TestDatagram{}).Create([]Channel_t{do2}, 5, STATESERVER_OBJECT_SET_OWNER_RECV)
	dg.AddChannel(ownerChan)
	conn.SendDatagram(*dg)
	time.Sleep(10 * time.Millisecond)
	obj2.Flush()
	conn.Flush()

	// Change DO #2's location
	dg = (&TestDatagram{}).Create([]Channel_t{do2}, 5, STATESERVER_OBJECT_SET_ZONE)
	appendMeta(dg, 6969, Doid_t(do1), 10000, 6969)
	conn.SendDatagram(*dg)
	obj2.Flush()

	// Expect CHANGING_LOCATION on the owner channel
	dg = (&TestDatagram{}).Create([]Channel_t{ownerChan, do1}, 5, STATESERVER_OBJECT_CHANGE_ZONE)
	dg.AddDoid(Doid_t(do2))
	appendMeta(dg, 6969, Doid_t(do1), 10000, 6969)
	appendMeta(dg, 6969, INVALID_DOID, INVALID_ZONE, 6969)
	conn.Expect(t, *dg, false)

	conn.RemoveChannel(ownerChan)
	time.Sleep(10 * time.Millisecond)
	location1.Flush()
	obj1.Flush()

	// Remove owner
	dg = (&TestDatagram{}).Create([]Channel_t{do2}, 5, STATESERVER_OBJECT_SET_OWNER_RECV)
	dg.AddChannel(0)
	conn.SendDatagram(*dg)
	time.Sleep(10 * time.Millisecond)
	conn.Flush()
	obj2.Flush()

	// Test for SetLocation w/ a ram+broadcast field
	// Set a ram+broadcast field of DO #2
	dg = (&TestDatagram{}).Create([]Channel_t{do2}, 5, STATESERVER_OBJECT_UPDATE_FIELD)
	dg.AddDoid(Doid_t(do2))
	dg.AddUint16(SetBR1)
	dg.AddString("I really do hate writing unit tests!")
	conn.SendDatagram(*dg)
	time.Sleep(10 * time.Millisecond)
	obj1.Flush()
	location1.Flush()

	// Change DO #2's location
	dg = (&TestDatagram{}).Create([]Channel_t{do2}, 5, STATESERVER_OBJECT_SET_ZONE)
	appendMeta(dg, 6969, Doid_t(do3), 20000, 6969)
	conn.SendDatagram(*dg)

	// Ignore changing location messages
	time.Sleep(10 * time.Millisecond)
	obj1.Flush()
	obj2.Flush()
	obj3.Flush()
	location1.Flush()

	// DO #2 should send an ENTER_LOCATION_WITH_REQUIRED_OTHER
	dg = (&TestDatagram{}).Create([]Channel_t{LocationAsChannel(Doid_t(do3), 20000)},
		do2, STATESERVER_OBJECT_ENTER_LOCATION_WITH_REQUIRED_OTHER)
	appendMeta(dg, Doid_t(do2), Doid_t(do3), 20000, DistributedTestObject1)
	dg.AddUint32(0xD00D)
	dg.AddUint16(1)
	dg.AddUint16(SetBR1)
	dg.AddString("I really do hate writing unit tests!")
	location3.Expect(t, *dg, false)

	// Test for SetLocation w/ a ram, non-broadcast field
	// Delete DO #2 for reuse
	deleteObject(conn, 5, Doid_t(do2))
	obj2.Close()
	time.Sleep(20 * time.Millisecond)
	obj2.Flush()
	obj3.Flush()
	location3.Flush()

	// Recreate DO #2 with a ram, non-broadcast field
	dg = (&TestDatagram{}).Create([]Channel_t{100100}, 5, STATESERVER_OBJECT_GENERATE_WITH_REQUIRED_OTHER)
	appendMetaDoidLast(dg, Doid_t(do2), 0, 0, DistributedTestObject3)
	dg.AddUint32(0xF0000D) // setRequired1
	dg.AddUint32(0xB0000B) // setRDB3
	dg.AddUint16(1)        // Optional fields
	dg.AddUint16(SetDb3)
	dg.AddString("Unit tests take sooooooo long to write!")
	conn.SendDatagram(*dg)

	// Change DO #2's location
	dg = (&TestDatagram{}).Create([]Channel_t{do2}, 5, STATESERVER_OBJECT_SET_ZONE)
	appendMeta(dg, 6969, Doid_t(do1), 10000, 6969)
	conn.SendDatagram(*dg)

	// Ignore changing location messages
	time.Sleep(10 * time.Millisecond)
	obj2.Flush()
	obj1.Flush()

	// DO #2 should send an ENTER_LOCATION_WITH_REQUIRED_OTHER
	dg = (&TestDatagram{}).Create([]Channel_t{LocationAsChannel(Doid_t(do1), 10000)},
		do2, STATESERVER_OBJECT_ENTER_LOCATION_WITH_REQUIRED_OTHER)
	appendMeta(dg, Doid_t(do2), Doid_t(do1), 10000, DistributedTestObject3)
	dg.AddUint32(0xF0000D)
	dg.AddUint32(0xB0000B)
	dg.AddUint16(0)
	location1.Expect(t, *dg, false)

	// Clean up :D
	deleteObject(conn, 5, Doid_t(do2))
	deleteObject(conn, 5, Doid_t(do3))
	time.Sleep(10 * time.Millisecond)
	for _, conn := range []*TestChannelConnection{conn, obj1, obj2, obj3, location1, location3} {
		conn.Close()
	}
}

func TestStateServer_Inheritance(t *testing.T) {
	do := Channel_t(0xF00000D)
	conn := connect(LocationAsChannel(10000, 5000))

	// Test for the creation of subclass objects
	dg := (&TestDatagram{}).Create([]Channel_t{100100}, 5, STATESERVER_OBJECT_GENERATE_WITH_REQUIRED)
	appendMetaDoidLast(dg, Doid_t(do), 10000, 5000, DistributedTestObject3)
	dg.AddUint32(0xF0000D) // setRequired1
	dg.AddUint32(0xB0000B) // setRDB3
	conn.SendDatagram(*dg)

	dg = (&TestDatagram{}).Create([]Channel_t{LocationAsChannel(10000, 5000)},
		do, STATESERVER_OBJECT_ENTER_LOCATION_WITH_REQUIRED)
	appendMeta(dg, Doid_t(do), 10000, 5000, DistributedTestObject3)
	dg.AddUint32(0xF0000D)
	dg.AddUint32(0xB0000B)
	conn.Expect(t, *dg, false)

	// Test the broadcast messages
	for _, field := range []uint16{SetRDB3, SetRequired1} {
		dg = (&TestDatagram{}).Create([]Channel_t{do}, 5, STATESERVER_OBJECT_UPDATE_FIELD)
		dg.AddDoid(Doid_t(do))
		dg.AddUint16(field)
		dg.AddUint32(0xB000000B)
		conn.SendDatagram(*dg)
		dg = (&TestDatagram{}).Create([]Channel_t{LocationAsChannel(10000, 5000)},
			5, STATESERVER_OBJECT_UPDATE_FIELD)
		dg.AddDoid(Doid_t(do))
		dg.AddUint16(field)
		dg.AddUint32(0xB000000B)
		conn.Expect(t, *dg, false)
	}

	// This field should fail to update
	dg = (&TestDatagram{}).Create([]Channel_t{do}, 5, STATESERVER_OBJECT_UPDATE_FIELD)
	dg.AddDoid(Doid_t(do))
	dg.AddUint16(SetB2)
	dg.AddUint32(0xB000B)
	conn.SendDatagram(*dg)
	conn.ExpectNone(t)

	// Cleanup :D
	deleteObject(conn, 5, Doid_t(do))
	time.Sleep(10 * time.Millisecond)
	conn.Close()
}

func TestStateServer_Error(t *testing.T) {
	do, do2 := Channel_t(0x8080808), Channel_t(0x9090909)
	conn := connect(5)
	conn.AddChannel(LocationAsChannel(8000, 4000))

	// Test for updates on an invalid field
	// Instantiate the object
	instantiateObject(conn, 5, Doid_t(do), 8000, 4000, 1337)

	// Object should announce its entry
	dg := (&TestDatagram{}).Create([]Channel_t{LocationAsChannel(8000, 4000)},
		do, STATESERVER_OBJECT_ENTER_LOCATION_WITH_REQUIRED)
	appendMeta(dg, Doid_t(do), 8000, 4000, DistributedTestObject1)
	dg.AddUint32(1337)
	conn.Expect(t, *dg, false)

	// Send an update on an invalid field
	dg = (&TestDatagram{}).Create([]Channel_t{do}, 5, STATESERVER_OBJECT_UPDATE_FIELD)
	dg.AddDoid(Doid_t(do))
	dg.AddUint16(0x1337)
	dg.AddUint32(0)
	conn.SendDatagram(*dg)

	// Nothing should happen; 0x1337 is an invalid field
	conn.ExpectNone(t)

	// Test for truncated updates
	dg = (&TestDatagram{}).Create([]Channel_t{do}, 5, STATESERVER_OBJECT_UPDATE_FIELD)
	dg.AddDoid(Doid_t(do))
	dg.AddUint16(SetRequired1)
	dg.AddUint16(0) // Should be 32-bit
	conn.SendDatagram(*dg)

	// Nothing should happen & object should log an error
	conn.ExpectNone(t)

	// Test creating an object w/ an in-use doid
	instantiateObject(conn, 5, Doid_t(do), 8000, 4000, 1337)

	// Nothing should happen and the SS should log an error
	conn.ExpectNone(t)

	// Sanity check the values of the object to make sure they're unchanged
	dg = (&TestDatagram{}).Create([]Channel_t{do}, 5, STATESERVER_QUERY_OBJECT_ALL)
	dg.AddUint32(69)
	conn.SendDatagram(*dg)

	dg = (&TestDatagram{}).Create([]Channel_t{5}, do, STATESERVER_QUERY_OBJECT_ALL_RESP)
	dg.AddUint32(69)
	appendMetaDoidLast(dg, Doid_t(do), 8000, 4000, DistributedTestObject1)
	dg.AddUint32(1337)
	dg.AddUint16(0)
	conn.Expect(t, *dg, false)

	// Test creating an object w/o a required field
	dg = (&TestDatagram{}).Create([]Channel_t{100100}, 5, STATESERVER_OBJECT_GENERATE_WITH_REQUIRED)
	appendMetaDoidLast(dg, Doid_t(do2), 8000, 4000, DistributedTestObject1)
	conn.SendDatagram(*dg)

	// Nothing should happen and SS should error
	conn.ExpectNone(t)

	// Verify that the object doesn't exist
	dg = (&TestDatagram{}).Create([]Channel_t{do2}, 5, STATESERVER_QUERY_OBJECT_ALL)
	dg.AddUint32(69)
	conn.SendDatagram(*dg)
	conn.ExpectNone(t)

	// Test creating an object w/ an unrecognized class
	dg = (&TestDatagram{}).Create([]Channel_t{100100}, 5, STATESERVER_OBJECT_GENERATE_WITH_REQUIRED)
	appendMetaDoidLast(dg, Doid_t(do2), 8000, 4000, 0x1337)
	conn.SendDatagram(*dg)

	conn.ExpectNone(t)

	dg = (&TestDatagram{}).Create([]Channel_t{do2}, 5, STATESERVER_QUERY_OBJECT_ALL)
	dg.AddUint32(69)
	conn.SendDatagram(*dg)
	conn.ExpectNone(t)

	// Cleanup
	deleteObject(conn, 5, Doid_t(do))
	time.Sleep(10 * time.Millisecond)
	conn.Close()
}

func TestStateServer_SingleObjectAccessors(t *testing.T) {
	do, context := Channel_t(0x5050505), uint32(0x1111)
	conn := connect(5)
	instantiateObject(conn, 5, Doid_t(do), 0, 0, 0x1337)

	// Test GetField with an invalid DO
	dg := (&TestDatagram{}).Create([]Channel_t{do}, 5, STATESERVER_OBJECT_QUERY_FIELD)
	dg.AddUint32(context)
	dg.AddDoid(0xF00D)
	dg.AddUint16(SetRequired1)
	conn.SendDatagram(*dg)

	conn.ExpectNone(t)
	context++

	// Test GetFields with an invalid DO
	dg = (&TestDatagram{}).Create([]Channel_t{do}, 5, STATESERVER_OBJECT_QUERY_FIELDS)
	dg.AddUint32(context)
	dg.AddDoid(0xF00D)
	dg.AddUint16(2)
	dg.AddUint16(SetRequired1)
	dg.AddUint16(SetBR1)
	conn.SendDatagram(*dg)

	conn.ExpectNone(t)
	context++

	// Test SetField update with an invalid DO
	dg = (&TestDatagram{}).Create([]Channel_t{do}, 5, STATESERVER_OBJECT_UPDATE_FIELD)
	dg.AddDoid(0xF00D)
	dg.AddUint16(SetRequired1)
	dg.AddUint32(0xB00B)
	conn.SendDatagram(*dg)

	// Verify the field is unchanged
	dg = (&TestDatagram{}).Create([]Channel_t{do}, 5, STATESERVER_OBJECT_QUERY_FIELD)
	dg.AddUint32(0xB0000B)
	dg.AddDoid(Doid_t(do))
	dg.AddUint16(SetRequired1)
	conn.SendDatagram(*dg)

	dg = (&TestDatagram{}).Create([]Channel_t{5}, do, STATESERVER_OBJECT_QUERY_FIELD_RESP)
	dg.AddUint32(0xB0000B)
	dg.AddBool(true)
	dg.AddUint16(SetRequired1)
	dg.AddUint32(0x1337)
	conn.Expect(t, *dg, false)

	// Test SetFields with an invalid DO
	dg = (&TestDatagram{}).Create([]Channel_t{do}, 5, STATESERVER_OBJECT_UPDATE_FIELD_MULTIPLE)
	dg.AddDoid(0xF00D)
	dg.AddUint16(2)
	dg.AddUint16(SetRequired1)
	dg.AddUint32(0xB00B)
	dg.AddUint16(SetBR1)
	dg.AddString("Why is this unit test so long?!")
	conn.SendDatagram(*dg)

	// Verify the field is unchanged
	dg = (&TestDatagram{}).Create([]Channel_t{do}, 5, STATESERVER_OBJECT_QUERY_FIELD)
	dg.AddUint32(0xB0000B)
	dg.AddDoid(Doid_t(do))
	dg.AddUint16(SetRequired1)
	conn.SendDatagram(*dg)

	dg = (&TestDatagram{}).Create([]Channel_t{5}, do, STATESERVER_OBJECT_QUERY_FIELD_RESP)
	dg.AddUint32(0xB0000B)
	dg.AddBool(true)
	dg.AddUint16(SetRequired1)
	dg.AddUint32(0x1337)
	conn.Expect(t, *dg, false)

	// Cleanup
	deleteObject(conn, 5, Doid_t(do))
	time.Sleep(10 * time.Millisecond)
	conn.Close()
}

func TestStateServer_CreateWithOther(t *testing.T) {
	do1, do2 := Channel_t(0x13337), Channel_t(0x13338)
	conn := connect(LocationAsChannel(6000, 3000))

	// Instantiate a new DO with other fields
	dg := (&TestDatagram{}).Create([]Channel_t{100100}, 5, STATESERVER_OBJECT_GENERATE_WITH_REQUIRED_OTHER)
	appendMetaDoidLast(dg, Doid_t(do1), 6000, 3000, DistributedTestObject1)
	dg.AddUint32(0x1337)
	dg.AddUint16(1)
	dg.AddUint16(SetBR1)
	dg.AddString("liam is epic")
	conn.SendDatagram(*dg)

	// It should announce its presence
	dg = (&TestDatagram{}).Create([]Channel_t{LocationAsChannel(6000, 3000)},
		do1, STATESERVER_OBJECT_ENTER_LOCATION_WITH_REQUIRED_OTHER)
	appendMeta(dg, Doid_t(do1), 6000, 3000, DistributedTestObject1)
	dg.AddUint32(0x1337)
	dg.AddUint16(1)
	dg.AddUint16(SetBR1)
	dg.AddString("liam is epic")
	conn.Expect(t, *dg, false)

	// Create another object with a non-ram field as OTHER
	dg = (&TestDatagram{}).Create([]Channel_t{100100}, 5, STATESERVER_OBJECT_GENERATE_WITH_REQUIRED_OTHER)
	appendMetaDoidLast(dg, Doid_t(do2), 6000, 3000, DistributedTestObject1)
	dg.AddUint32(0x1337)
	dg.AddUint16(1)
	dg.AddUint16(SetB1)
	dg.AddUint8(42)
	conn.SendDatagram(*dg)

	// The object should show up without the non-RAM field and an error should be logged
	dg = (&TestDatagram{}).Create([]Channel_t{LocationAsChannel(6000, 3000)},
		do2, STATESERVER_OBJECT_ENTER_LOCATION_WITH_REQUIRED)
	appendMeta(dg, Doid_t(do2), 6000, 3000, DistributedTestObject1)
	dg.AddUint32(0x1337)
	conn.Expect(t, *dg, false)

	// Cleanup
	deleteObject(conn, 5, Doid_t(do1))
	deleteObject(conn, 5, Doid_t(do2))
	conn.Close()
}

func TestStateServer_GetLocation(t *testing.T) {
	conn := connect(0x699)
	do1, do2, do3 := Channel_t(100), Channel_t(200), Channel_t(300)

	instantiateObject(conn, 5, Doid_t(do1), 2000, 3000, 9)
	instantiateObject(conn, 5, Doid_t(do2), 3000, 4000, 9)
	instantiateObject(conn, 5, Doid_t(do3), 4000, 5000, 9)

	for n := 1; n <= 3; n++ {
		dg := (&TestDatagram{}).Create([]Channel_t{Channel_t(n * 100)}, 0x699, STATESERVER_OBJECT_LOCATE)
		dg.AddUint32(uint32(n))
		conn.SendDatagram(*dg)

		dg = (&TestDatagram{}).Create([]Channel_t{0x699}, Channel_t(Doid_t(n*100)), STATESERVER_OBJECT_LOCATE_RESP)
		dg.AddUint32(uint32(n))
		appendMeta(dg, Doid_t(n*100), Doid_t((n*1000)+1000), Zone_t((n*1000)+2000), 6969)
		conn.Expect(t, *dg, false)

		deleteObject(conn, 5, Doid_t(Channel_t(n*100)))
	}

	time.Sleep(10 * time.Millisecond)
	conn.Close()
}

func TestStateServer_DeleteAiObjects(t *testing.T) {
	conn := connect(LocationAsChannel(20000, 10000))
	do := Channel_t(0x1337)

	// Instantiate an object
	instantiateObject(conn, 5, Doid_t(do), 20000, 10000, 0xF00D)

	// Give the object an AI channel
	dg := (&TestDatagram{}).Create([]Channel_t{do}, 5, STATESERVER_ADD_AI_RECV)
	dg.AddChannel(100)
	conn.SendDatagram(*dg)

	// Ignore noise
	time.Sleep(10 * time.Millisecond)
	conn.Flush()

	// Test reset with an invalid AI
	dg = (&TestDatagram{}).Create([]Channel_t{100100}, 5, STATESERVER_SHARD_REST)
	dg.AddChannel(200)
	conn.SendDatagram(*dg)
	conn.ExpectNone(t)

	// Test our message with the correct AI
	dg = (&TestDatagram{}).Create([]Channel_t{100100}, 5, STATESERVER_SHARD_REST)
	dg.AddChannel(100)
	conn.SendDatagram(*dg)

	// The object should die
	dg = (&TestDatagram{}).Create([]Channel_t{
		LocationAsChannel(20000, 10000), 100}, 5, STATESERVER_OBJECT_DELETE_RAM)
	dg.AddDoid(Doid_t(do))
	conn.Expect(t, *dg, false)

	// Cleanup
	conn.Close()
}

func TestStateServer_Get(t *testing.T) {
	conn := connect(0x69)
	do := Channel_t(0x500)

	// OtpGo specific change:
	// Test for GetAll w/ the State Server's
	// ObjectServer object.

	// Send a GET_ALL request !!To the State Server!!
	dg := (&TestDatagram{}).Create([]Channel_t{100100}, 0x69, STATESERVER_QUERY_OBJECT_ALL)
	dg.AddUint32(1)
	conn.SendDatagram(*dg)

	// Expect all data from ObjectServer (setName and setDcHash) in response.
	dg = (&TestDatagram{}).Create([]Channel_t{0x69}, Channel_t(100100), STATESERVER_QUERY_OBJECT_ALL_RESP)
	dg.AddUint32(1)
	appendMetaDoidLast(dg, Doid_t(100100), 0, 0, ObjectServer)
	dg.AddString("ObjectServer")
	dg.AddUint32(uint32(core.DC.Get_hash()))
	dg.AddUint16(0)
	conn.Expect(t, *dg, false)

	// Test for GetAll w/ only required fields
	// Create the object
	instantiateObject(conn, 5, Doid_t(do), 0, 0, 0)

	// Send a GET_ALL request
	dg = (&TestDatagram{}).Create([]Channel_t{do}, 0x69, STATESERVER_QUERY_OBJECT_ALL)
	dg.AddUint32(1)
	conn.SendDatagram(*dg)

	// Expect all data in the response
	dg = (&TestDatagram{}).Create([]Channel_t{0x69}, do, STATESERVER_QUERY_OBJECT_ALL_RESP)
	dg.AddUint32(1)
	appendMetaDoidLast(dg, Doid_t(do), 0, 0, DistributedTestObject1)
	dg.AddUint32(0)
	dg.AddUint16(0)
	conn.Expect(t, *dg, false)

	// Test for GetField on a valid field w/ a set value
	dg = (&TestDatagram{}).Create([]Channel_t{do}, 0x69, STATESERVER_OBJECT_QUERY_FIELD)
	dg.AddUint32(1)
	dg.AddDoid(Doid_t(do))
	dg.AddUint16(SetRequired1)
	conn.SendDatagram(*dg)

	// Expect response
	dg = (&TestDatagram{}).Create([]Channel_t{0x69}, do, STATESERVER_OBJECT_QUERY_FIELD_RESP)
	dg.AddUint32(1)
	dg.AddBool(true)
	dg.AddUint16(SetRequired1)
	dg.AddUint32(0)
	conn.Expect(t, *dg, false)

	// Test for GetField on a valid field w/ no set value
	dg = (&TestDatagram{}).Create([]Channel_t{do}, 0x69, STATESERVER_OBJECT_QUERY_FIELD)
	dg.AddUint32(1)
	dg.AddDoid(Doid_t(do))
	dg.AddUint16(SetBR1)
	conn.SendDatagram(*dg)

	// Expect failure
	dg = (&TestDatagram{}).Create([]Channel_t{0x69}, do, STATESERVER_OBJECT_QUERY_FIELD_RESP)
	dg.AddUint32(1)
	dg.AddBool(false)
	conn.Expect(t, *dg, false)

	// Test for GetField on an invalid field
	dg = (&TestDatagram{}).Create([]Channel_t{do}, 0x69, STATESERVER_OBJECT_QUERY_FIELD)
	dg.AddUint32(1)
	dg.AddDoid(Doid_t(do))
	dg.AddUint16(0x1337)
	conn.SendDatagram(*dg)

	// Expect failure
	dg = (&TestDatagram{}).Create([]Channel_t{0x69}, do, STATESERVER_OBJECT_QUERY_FIELD_RESP)
	dg.AddUint32(1)
	dg.AddBool(false)
	conn.Expect(t, *dg, false)

	// Test for GetFields with set values
	// Set a second field
	dg = (&TestDatagram{}).Create([]Channel_t{do}, 0x69, STATESERVER_OBJECT_UPDATE_FIELD)
	dg.AddDoid(Doid_t(do))
	dg.AddUint16(SetBR1)
	dg.AddString("coronavirus gonna get me o_o")
	conn.SendDatagram(*dg)

	// Let our SET_FIELD get sent first
	time.Sleep(10 * time.Millisecond)

	dg = (&TestDatagram{}).Create([]Channel_t{do}, 0x69, STATESERVER_OBJECT_QUERY_FIELDS)
	dg.AddUint32(1)
	dg.AddDoid(Doid_t(do))
	dg.AddUint16(2)
	dg.AddUint16(SetRequired1)
	dg.AddUint16(SetBR1)
	conn.SendDatagram(*dg)

	dg = (&TestDatagram{}).Create([]Channel_t{0x69}, do, STATESERVER_OBJECT_QUERY_FIELDS_RESP)
	dg.AddUint32(1)
	dg.AddBool(true)
	dg.AddUint16(2)
	dg.AddUint16(SetRequired1)
	dg.AddUint32(0)
	dg.AddUint16(SetBR1)
	dg.AddString("coronavirus gonna get me o_o")
	conn.Expect(t, *dg, false)

	// Test for GetFields with set and unset fields
	dg = (&TestDatagram{}).Create([]Channel_t{do}, 0x69, STATESERVER_OBJECT_QUERY_FIELDS)
	dg.AddUint32(1)
	dg.AddDoid(Doid_t(do))
	dg.AddUint16(2)
	dg.AddUint16(SetBR1)
	dg.AddUint16(SetBRA1)
	conn.SendDatagram(*dg)

	dg = (&TestDatagram{}).Create([]Channel_t{0x69}, do, STATESERVER_OBJECT_QUERY_FIELDS_RESP)
	dg.AddUint32(1)
	dg.AddBool(true)
	dg.AddUint16(1)
	dg.AddUint16(SetBR1)
	dg.AddString("coronavirus gonna get me o_o")
	conn.Expect(t, *dg, false)

	// Test for GetFields with only unset fields
	dg = (&TestDatagram{}).Create([]Channel_t{do}, 0x69, STATESERVER_OBJECT_QUERY_FIELDS)
	dg.AddUint32(1)
	dg.AddDoid(Doid_t(do))
	dg.AddUint16(2)
	dg.AddUint16(SetBRA1)
	dg.AddUint16(SetBRO1)
	conn.SendDatagram(*dg)

	dg = (&TestDatagram{}).Create([]Channel_t{0x69}, do, STATESERVER_OBJECT_QUERY_FIELDS_RESP)
	dg.AddUint32(1)
	dg.AddBool(true)
	dg.AddUint16(0)
	conn.Expect(t, *dg, false)

	// Test for GetFields with only invalid fields
	dg = (&TestDatagram{}).Create([]Channel_t{do}, 0x69, STATESERVER_OBJECT_QUERY_FIELDS)
	dg.AddUint32(1)
	dg.AddDoid(Doid_t(do))
	dg.AddUint16(2)
	dg.AddUint16(SetRDB3)
	dg.AddUint16(SetDb3)
	conn.SendDatagram(*dg)

	dg = (&TestDatagram{}).Create([]Channel_t{0x69}, do, STATESERVER_OBJECT_QUERY_FIELDS_RESP)
	dg.AddUint32(1)
	dg.AddBool(false)
	conn.Expect(t, *dg, false)

	// Test for GetFields with mixed valid/invalid fields
	dg = (&TestDatagram{}).Create([]Channel_t{do}, 0x69, STATESERVER_OBJECT_QUERY_FIELDS)
	dg.AddUint32(1)
	dg.AddDoid(Doid_t(do))
	dg.AddUint16(2)
	dg.AddUint16(SetRDB3)
	dg.AddUint16(SetRequired1)
	conn.SendDatagram(*dg)

	dg = (&TestDatagram{}).Create([]Channel_t{0x69}, do, STATESERVER_OBJECT_QUERY_FIELDS_RESP)
	dg.AddUint32(1)
	dg.AddBool(false)
	conn.Expect(t, *dg, false)

	// Cleanup
	deleteObject(conn, 5, Doid_t(do))
	time.Sleep(10 * time.Millisecond)
	conn.Close()
}

func TestStateServer_SetField(t *testing.T) {
	conn, location := connect(120), connect(LocationAsChannel(6000, 3000))
	do := Channel_t(0x888)

	dg := (&TestDatagram{}).Create([]Channel_t{100100}, 5, STATESERVER_OBJECT_GENERATE_WITH_REQUIRED)
	appendMetaDoidLast(dg, Doid_t(do), 6000, 3000, DistributedTestObject3)
	dg.AddUint32(0) // setRequired1
	dg.AddUint32(0) // setRDB3
	conn.SendDatagram(*dg)

	time.Sleep(10 * time.Millisecond)
	location.Flush()

	// Send a multi-field update two broadcast and one ram field
	dg = (&TestDatagram{}).Create([]Channel_t{do}, 5, STATESERVER_OBJECT_UPDATE_FIELD_MULTIPLE)
	dg.AddDoid(Doid_t(do))
	dg.AddUint16(3)
	dg.AddUint16(SetDb3)
	dg.AddString("this a string. circumvention couldn't think of anything better lmao")
	dg.AddUint16(SetRequired1)
	dg.AddUint32(0xDEADBEEF)
	dg.AddUint16(SetB1)
	dg.AddUint8(10)
	conn.SendDatagram(*dg)

	time.Sleep(10 * time.Millisecond)

	// Verify that the ram fields are set
	dg = (&TestDatagram{}).Create([]Channel_t{do}, 120, STATESERVER_QUERY_OBJECT_ALL)
	dg.AddUint32(1)
	conn.SendDatagram(*dg)

	dg = (&TestDatagram{}).Create([]Channel_t{120}, do, STATESERVER_QUERY_OBJECT_ALL_RESP)
	dg.AddUint32(1)
	appendMetaDoidLast(dg, Doid_t(do), 6000, 3000, DistributedTestObject3)
	dg.AddUint32(0xDEADBEEF)
	dg.AddUint32(0)
	dg.AddUint16(1)
	dg.AddUint16(SetDb3)
	dg.AddString("this a string. circumvention couldn't think of anything better lmao")
	conn.Expect(t, *dg, false)

	// Cleanup
	deleteObject(conn, 5, Doid_t(do))
	time.Sleep(10 * time.Millisecond)
	location.Close()
	conn.Close()
}

func TestStateServer_Ownrecv(t *testing.T) {
	conn := connect(500)
	do, owner, location := Channel_t(50), Channel_t(500), LocationAsChannel(2000, 1000)

	// Test for broadcast of a field
	// Create a new object
	instantiateObject(conn, 5, Doid_t(do), 2000, 1000, 0)

	// Set the object's owner
	dg := (&TestDatagram{}).Create([]Channel_t{do}, 5, STATESERVER_OBJECT_SET_OWNER_RECV)
	dg.AddChannel(owner)
	conn.SendDatagram(*dg)

	// Ignore EnterOwner message
	time.Sleep(10 * time.Millisecond)
	conn.Flush()

	// Set an ownrecv field
	dg = (&TestDatagram{}).Create([]Channel_t{do}, 5, STATESERVER_OBJECT_UPDATE_FIELD)
	dg.AddDoid(Doid_t(do))
	dg.AddUint16(SetBRO1)
	dg.AddUint32(0xF005BA11) // FOOTBALL o_o
	conn.SendDatagram(*dg)

	// See if owner channel & location receives it
	dg = (&TestDatagram{}).Create([]Channel_t{location, owner}, 5, STATESERVER_OBJECT_UPDATE_FIELD)
	dg.AddDoid(Doid_t(do))
	dg.AddUint16(SetBRO1)
	dg.AddUint32(0xF005BA11)
	conn.Expect(t, *dg, false)

	// Test that client should not get its messages reflected back
	dg = (&TestDatagram{}).Create([]Channel_t{do}, owner, STATESERVER_OBJECT_UPDATE_FIELD)
	dg.AddDoid(Doid_t(do))
	dg.AddUint16(SetBRO1)
	dg.AddUint32(0xF005BA11)
	conn.SendDatagram(*dg)

	// Owner should get nothing
	conn.ExpectNone(t)

	// Delete the object
	deleteObject(conn, 5, Doid_t(do))

	// The owner should be notified of deletion
	dg = (&TestDatagram{}).Create([]Channel_t{location, owner}, 5, STATESERVER_OBJECT_DELETE_RAM)
	dg.AddDoid(Doid_t(do))
	conn.Expect(t, *dg, false)

	// Cleanup
	time.Sleep(10 * time.Millisecond)
	conn.Close()
}

func TestStateServer_SetOwner(t *testing.T) {
	own1Chan, own2Chan, do1, do2 := Channel_t(1234), Channel_t(5678), Channel_t(0xB1), Channel_t(0xB2)
	conn, own1, own2 := connect(5), connect(own1Chan), connect(own2Chan)

	// Test for SetOwner w/ no owner
	instantiateObject(conn, 5, Doid_t(do1), 2, 1, 0)

	// Set the owner
	dg := (&TestDatagram{}).Create([]Channel_t{do1}, 5, STATESERVER_OBJECT_SET_OWNER_RECV)
	dg.AddChannel(own1Chan)
	conn.SendDatagram(*dg)

	// The object should send an ENTER_OWNER
	dg = (&TestDatagram{}).Create([]Channel_t{own1Chan}, do1, STATESERVER_OBJECT_ENTER_OWNER_RECV)
	appendMeta(dg, Doid_t(do1), 2, 1, DistributedTestObject1)
	dg.AddUint32(0)
	dg.AddUint16(0) // 0 optional fields
	own1.Expect(t, *dg, false)

	// Test for SetOwner w/ an owner
	dg = (&TestDatagram{}).Create([]Channel_t{do1}, 5, STATESERVER_OBJECT_SET_OWNER_RECV)
	dg.AddChannel(own2Chan)
	conn.SendDatagram(*dg)

	// Expect a CHANGING_OWNER message
	dg = (&TestDatagram{}).Create([]Channel_t{own1Chan}, 5, STATESERVER_OBJECT_CHANGE_OWNER_RECV)
	dg.AddDoid(Doid_t(do1))
	dg.AddChannel(own2Chan) // New owner
	dg.AddChannel(own1Chan) // Old owner
	own1.Expect(t, *dg, false)
	// It should enter the new owner
	dg = (&TestDatagram{}).Create([]Channel_t{own2Chan}, do1, STATESERVER_OBJECT_ENTER_OWNER_RECV)
	appendMeta(dg, Doid_t(do1), 2, 1, DistributedTestObject1)
	dg.AddUint32(0)
	dg.AddUint16(0) // 0 optional fields
	own2.Expect(t, *dg, false)

	// Test for SetOwner w/ owner fields and non-owner, non-broadcast fields
	dg = (&TestDatagram{}).Create([]Channel_t{100100}, 5, STATESERVER_OBJECT_GENERATE_WITH_REQUIRED_OTHER)
	appendMetaDoidLast(dg, Doid_t(do2), 2, 1, DistributedTestObject3)
	dg.AddUint32(0) // setRequired1
	dg.AddUint32(0) // setRDB3
	dg.AddUint16(3) // 3 optional fields
	dg.AddUint16(SetDb3)
	dg.AddString("You spilled sausage on your trousers?")
	dg.AddUint16(SetBRO1)
	dg.AddUint32(0x69)
	dg.AddUint16(SetBR1)
	dg.AddString("Were you able to get the stain out?")
	conn.SendDatagram(*dg)

	time.Sleep(10 * time.Millisecond)

	dg = (&TestDatagram{}).Create([]Channel_t{do2}, 5, STATESERVER_OBJECT_SET_OWNER_RECV)
	dg.AddChannel(own1Chan)
	conn.SendDatagram(*dg)

	// It should enter the new owner with only broadcast and/or ownrecv fields
	dg = (&TestDatagram{}).Create([]Channel_t{own1Chan}, do2, STATESERVER_OBJECT_ENTER_OWNER_RECV)
	dg.AddDoid(Doid_t(do2))
	dg.AddLocation(2, 1)
	dg.AddUint16(DistributedTestObject3)
	dg.AddUint32(0) // setRequired1
	dg.AddUint32(0) // setRDB3
	dg.AddUint16(2) // 2 optional fields; setDb3 is not broadcast or ownrecv
	dg.AddUint16(SetBR1)
	dg.AddString("Were you able to get the stain out?")
	dg.AddUint16(SetBRO1)
	dg.AddUint32(0x69)
	own1.Expect(t, *dg, false)

	// Cleanup
	deleteObject(conn, 5, Doid_t(do1))
	deleteObject(conn, 5, Doid_t(do2))
	time.Sleep(10 * time.Millisecond)
	own1.Close()
	own2.Close()
	conn.Close()
}

func TestStateServer_Molecular(t *testing.T) {
	do, locationChan := Channel_t(0xB00B), LocationAsChannel(2500, 5000)
	conn, location := connect(1337), connect(locationChan)
	location.Timeout = 200

	// Test for broadcast of a molecular field
	// Create an object
	dg := (&TestDatagram{}).Create([]Channel_t{100100}, 5, STATESERVER_OBJECT_GENERATE_WITH_REQUIRED)
	appendMetaDoidLast(dg, Doid_t(do), 2500, 5000, DistributedTestObject4)
	dg.AddUint32(4)     // setX
	dg.AddUint32(8)     // setY
	dg.AddUint32(0x100) // setUnrelated
	dg.AddUint32(12)    // setZ
	conn.SendDatagram(*dg)

	// It should send an entry message to its location
	dg = (&TestDatagram{}).Create([]Channel_t{locationChan}, do, STATESERVER_OBJECT_ENTER_LOCATION_WITH_REQUIRED)
	appendMeta(dg, Doid_t(do), 2500, 5000, DistributedTestObject4)
	dg.AddUint32(4)     // setX
	dg.AddUint32(8)     // setY
	dg.AddUint32(0x100) // setUnrelated
	dg.AddUint32(12)    // setZ
	location.Expect(t, *dg, false)

	// Send a molecular update
	dg = (&TestDatagram{}).Create([]Channel_t{do}, 5, STATESERVER_OBJECT_UPDATE_FIELD)
	dg.AddDoid(Doid_t(do))
	dg.AddUint16(SetXyz)
	dg.AddUint32(50)
	dg.AddUint32(60)
	dg.AddUint32(70)
	conn.SendDatagram(*dg)

	// The molecular field should be broadcast
	dg = (&TestDatagram{}).Create([]Channel_t{locationChan}, 5, STATESERVER_OBJECT_UPDATE_FIELD)
	dg.AddDoid(Doid_t(do))
	dg.AddUint16(SetXyz)
	dg.AddUint32(50)
	dg.AddUint32(60)
	dg.AddUint32(70)
	location.Expect(t, *dg, false)

	// Test for molecular SetField updating individual values
	// Inspect the object to see if its fields are updated
	dg = (&TestDatagram{}).Create([]Channel_t{do}, 1337, STATESERVER_QUERY_OBJECT_ALL)
	dg.AddUint32(1)
	conn.SendDatagram(*dg)

	dg = (&TestDatagram{}).Create([]Channel_t{1337}, do, STATESERVER_QUERY_OBJECT_ALL_RESP)
	dg.AddUint32(1)
	appendMetaDoidLast(dg, Doid_t(do), 2500, 5000, DistributedTestObject4)
	dg.AddUint32(50)
	dg.AddUint32(60)
	dg.AddUint32(0x100)
	dg.AddUint32(70)
	dg.AddUint16(0)
	conn.Expect(t, *dg, false)

	// Test for molecular SetField with ram, non-required fields
	dg = (&TestDatagram{}).Create([]Channel_t{do}, 5, STATESERVER_OBJECT_UPDATE_FIELD)
	dg.AddDoid(Doid_t(do))
	dg.AddUint16(Set123)
	dg.AddUint8(1) // setOne
	dg.AddUint8(2) // setTwo
	dg.AddUint8(3) // setThree
	conn.SendDatagram(*dg)

	// The molecular should be broacast
	dg = (&TestDatagram{}).Create([]Channel_t{locationChan}, 5, STATESERVER_OBJECT_UPDATE_FIELD)
	dg.AddDoid(Doid_t(do))
	dg.AddUint16(Set123)
	dg.AddUint8(1) // setOne
	dg.AddUint8(2) // setTwo
	dg.AddUint8(3) // setThree
	location.Expect(t, *dg, false)

	// GET_ALL should return all of the individual fields
	dg = (&TestDatagram{}).Create([]Channel_t{do}, 1337, STATESERVER_QUERY_OBJECT_ALL)
	dg.AddUint32(1)
	conn.SendDatagram(*dg)

	dg = (&TestDatagram{}).Create([]Channel_t{1337}, do, STATESERVER_QUERY_OBJECT_ALL_RESP)
	dg.AddUint32(1)
	appendMetaDoidLast(dg, Doid_t(do), 2500, 5000, DistributedTestObject4)
	dg.AddUint32(50)
	dg.AddUint32(60)
	dg.AddUint32(0x100)
	dg.AddUint32(70)
	dg.AddUint16(3)
	dg.AddUint16(SetOne)
	dg.AddUint8(1)
	dg.AddUint16(SetTwo)
	dg.AddUint8(2)
	dg.AddUint16(SetThree)
	dg.AddUint8(3)
	conn.Expect(t, *dg, false)

	// Test for molecular & atomic mixed GetFields
	dg = (&TestDatagram{}).Create([]Channel_t{do}, 1337, STATESERVER_OBJECT_QUERY_FIELDS)
	dg.AddUint32(1)
	dg.AddDoid(Doid_t(do))
	dg.AddUint16(5) // Field count
	dg.AddUint16(SetXyz)
	dg.AddUint16(SetOne)
	dg.AddUint16(SetUnrelated)
	dg.AddUint16(Set123)
	dg.AddUint16(SetX)
	conn.SendDatagram(*dg)

	dg = (&TestDatagram{}).Create([]Channel_t{1337}, do, STATESERVER_OBJECT_QUERY_FIELDS_RESP)
	dg.AddUint32(1)
	dg.AddBool(true)
	dg.AddUint16(5)
	dg.AddUint16(SetX)
	dg.AddUint32(50)
	dg.AddUint16(SetUnrelated)
	dg.AddUint32(0x100)
	dg.AddUint16(SetXyz)
	dg.AddUint32(50)
	dg.AddUint32(60)
	dg.AddUint32(70)
	dg.AddUint16(SetOne)
	dg.AddUint8(1)
	dg.AddUint16(Set123)
	dg.AddUint8(1)
	dg.AddUint8(2)
	dg.AddUint8(3)
	conn.Expect(t, *dg, false)

	// Cleanup :D
	deleteObject(conn, 5, Doid_t(do))
	time.Sleep(10 * time.Millisecond)
	location.Close()
	conn.Close()
}

func TestStateServer_GetZonesObjects(t *testing.T) {
	conn := connect(5)

	do0, do1, do2, do3, do4, do5, do6 :=
		Channel_t(1000), // Root object
		Channel_t(2001),
		Channel_t(2002),
		Channel_t(2003),
		Channel_t(2004),
		Channel_t(2005),
		Channel_t(3006) // Child of child

	type oz struct { // object-zone
		object Channel_t
		zone   Zone_t
	}
	checkObjects := func(objects []oz, zones []Zone_t) {
		var expected []Datagram
		time.Sleep(100 * time.Millisecond)

		// Send query
		dg := (&TestDatagram{}).Create([]Channel_t{do0}, 5, STATESERVER_OBJECT_GET_ZONES_OBJECTS)
		dg.AddUint32(0xF337)
		dg.AddDoid(Doid_t(do0))          // Parent ID
		dg.AddUint16(uint16(len(zones))) // Zone Count
		for _, zone := range zones {
			dg.AddZone(zone)
		}
		conn.SendDatagram(*dg)

		// Expect object count
		dg = (&TestDatagram{}).Create([]Channel_t{5}, do0, STATESERVER_OBJECT_GET_ZONES_COUNT_RESP)
		dg.AddUint32(0xF337)
		dg.AddDoid(Doid_t(len(objects)))
		expected = append(expected, *dg)

		for _, obj := range objects {
			dg = (&TestDatagram{}).Create([]Channel_t{5}, obj.object, STATESERVER_OBJECT_ENTER_INTEREST_WITH_REQUIRED)
			dg.AddUint32(0xF337)
			appendMeta(dg, Doid_t(obj.object), Doid_t(do0), obj.zone, DistributedTestObject1)
			dg.AddUint32(0)
			expected = append(expected, *dg)
		}

		conn.ExpectMany(t, expected, false, false)
	}

	// Create some objects
	instantiateObject(conn, 5, Doid_t(do0), 0, 0, 0)
	instantiateObject(conn, 5, Doid_t(do1), Doid_t(do0), 912, 0)
	instantiateObject(conn, 5, Doid_t(do2), Doid_t(do0), 912, 0)
	instantiateObject(conn, 5, Doid_t(do3), Doid_t(do0), 930, 0)
	instantiateObject(conn, 5, Doid_t(do4), Doid_t(do0), 940, 0)
	instantiateObject(conn, 5, Doid_t(do5), Doid_t(do0), 950, 0)
	instantiateObject(conn, 5, Doid_t(do6), Doid_t(do1), 860, 0)

	checkObjects([]oz{oz{do1, 912}, oz{do2, 912}, oz{do3, 930}}, []Zone_t{912, 930})

	// Verify GET_OBJECTS with updates
	// Move DO #4
	dg := (&TestDatagram{}).Create([]Channel_t{do4}, 5, STATESERVER_OBJECT_SET_ZONE)
	appendMeta(dg, 6969, Doid_t(do0), 930, 6969)
	conn.SendDatagram(*dg)

	checkObjects([]oz{oz{do1, 912}, oz{do2, 912}, oz{do3, 930}, oz{do4, 930}}, []Zone_t{912, 930})

	// Delete DO #2
	deleteObject(conn, 5, Doid_t(do2))
	checkObjects([]oz{oz{do1, 912}, oz{do3, 930}, oz{do4, 930}}, []Zone_t{912, 930})

	// Move DO #3 away
	dg = (&TestDatagram{}).Create([]Channel_t{do3}, 5, STATESERVER_OBJECT_SET_ZONE)
	appendMeta(dg, 6969, Doid_t(do1), 930, 6969)
	conn.SendDatagram(*dg)
	checkObjects([]oz{oz{do1, 912}, oz{do4, 930}}, []Zone_t{912, 930})

	time.Sleep(100 * time.Millisecond)
	// Test OBJECT_LOCATION_ACK behavior

	parent := Doid_t(4321)

	// Move DO #4 and DO #5 into a zone under our parent
	dg = (&TestDatagram{}).Create([]Channel_t{do4, do5}, 5, STATESERVER_OBJECT_SET_ZONE)
	appendMeta(dg, 6969, parent, 1000, 6969)
	conn.SendDatagram(*dg)

	time.Sleep(10 * time.Millisecond)

	// Acknowledge DO #5 as the parent
	dg = (&TestDatagram{}).Create([]Channel_t{do5}, Channel_t(parent), STATESERVER_OBJECT_LOCATION_ACK)
	dg.AddDoid(parent)
	dg.AddZone(1000)
	conn.SendDatagram(*dg)

	// Acknowledge DO #4, but with the old zone
	dg = (&TestDatagram{}).Create([]Channel_t{do4}, Channel_t(parent), STATESERVER_OBJECT_LOCATION_ACK)
	dg.AddDoid(parent)
	dg.AddZone(930)
	conn.SendDatagram(*dg)

	time.Sleep(10 * time.Millisecond)

	// Fake a relayed parent->child query
	dg = (&TestDatagram{}).Create([]Channel_t{ParentToChildren(parent)}, 5, STATESERVER_OBJECT_GET_ZONE_OBJECTS)
	dg.AddUint32(0xDEAD)
	dg.AddDoid(parent)
	dg.AddZone(1000)
	conn.SendDatagram(*dg)

	var expected []Datagram
	// DO #5 should respond with a contextual query
	dg = (&TestDatagram{}).Create([]Channel_t{5}, do5, STATESERVER_OBJECT_ENTER_INTEREST_WITH_REQUIRED)
	dg.AddUint32(0xDEAD)
	appendMeta(dg, Doid_t(do5), parent, 1000, DistributedTestObject1)
	dg.AddUint32(0)
	expected = append(expected, *dg)

	// DO #4 should respond with an ENTER_LOCATION
	dg = (&TestDatagram{}).Create([]Channel_t{5}, do4, STATESERVER_OBJECT_ENTER_LOCATION_WITH_REQUIRED)
	appendMeta(dg, Doid_t(do4), parent, 1000, DistributedTestObject1)
	dg.AddUint32(0)
	expected = append(expected, *dg)

	conn.ExpectMany(t, expected, false, true)

	// Cleanup :D
	for _, do := range []Channel_t{do0, do1, do2, do3, do4, do5, do6} {
		deleteObject(conn, 5, Doid_t(do))
	}
	time.Sleep(10 * time.Millisecond)
	conn.Close()
}

func TestStateServer_DeleteChildren(t *testing.T) {
	do0, do1, do2 := Channel_t(0xA2), Channel_t(0xB2), Channel_t(0xC2)
	conn, children, loc1, loc2, loc3 := connect(5),
		connect(ParentToChildren(Doid_t(do0))),
		connect(LocationAsChannel(Doid_t(do0), 710)),
		connect(LocationAsChannel(Doid_t(do0), 720)),
		connect(LocationAsChannel(Doid_t(do1), 720))

	// Test for DELETE_CHILDREN
	// Create an object tree
	instantiateObject(conn, 5, Doid_t(do0), 0, 0, 0)
	instantiateObject(conn, 5, Doid_t(do1), Doid_t(do0), 710, 0)
	instantiateObject(conn, 5, Doid_t(do2), Doid_t(do0), 720, 0)

	// Ignore entry broadcasts
	time.Sleep(10 * time.Millisecond)
	loc1.Flush()
	loc2.Flush()
	children.Flush()

	// Send DELETE_CHILDREN
	dg := (&TestDatagram{}).Create([]Channel_t{do0}, 5, STATESERVER_OBJECT_DELETE_CHILDREN)
	dg.AddDoid(Doid_t(do0))
	conn.SendDatagram(*dg)

	// Children should receive the message
	dg = (&TestDatagram{}).Create([]Channel_t{ParentToChildren(Doid_t(do0))}, 5, STATESERVER_OBJECT_DELETE_CHILDREN)
	dg.AddDoid(Doid_t(do0))
	children.Expect(t, *dg, false)
	// Children should broadcast their own deletion
	dg = (&TestDatagram{}).Create([]Channel_t{LocationAsChannel(Doid_t(do0), 710)}, 5, STATESERVER_OBJECT_DELETE_RAM)
	dg.AddDoid(Doid_t(do1))
	loc1.Expect(t, *dg, false)
	dg = (&TestDatagram{}).Create([]Channel_t{LocationAsChannel(Doid_t(do0), 720)}, 5, STATESERVER_OBJECT_DELETE_RAM)
	dg.AddDoid(Doid_t(do2))
	loc2.Expect(t, *dg, false)

	// Test DELETE_RAM propagation to children
	// Create a multi-tier object tree
	instantiateObject(conn, 5, Doid_t(do1), Doid_t(do0), 710, 0)
	instantiateObject(conn, 5, Doid_t(do2), Doid_t(do1), 720, 0)

	// Ignore entry broadcasts
	time.Sleep(10 * time.Millisecond)
	loc1.Flush()
	loc3.Flush()

	// Delete the root object
	dg = (&TestDatagram{}).Create([]Channel_t{do0}, 5, STATESERVER_OBJECT_DELETE_RAM)
	dg.AddDoid(Doid_t(do0))
	conn.SendDatagram(*dg)

	// First object should receive DELETE_CHILDREN from it's parent
	dg = (&TestDatagram{}).Create([]Channel_t{ParentToChildren(Doid_t(do0))}, 5, STATESERVER_OBJECT_DELETE_CHILDREN)
	dg.AddDoid(Doid_t(do0))
	children.Expect(t, *dg, false)
	// It should then delete itself
	dg = (&TestDatagram{}).Create([]Channel_t{LocationAsChannel(Doid_t(do0), 710)}, 5, STATESERVER_OBJECT_DELETE_RAM)
	dg.AddDoid(Doid_t(do1))
	loc1.Expect(t, *dg, false)
	// It should also delete it's child
	dg = (&TestDatagram{}).Create([]Channel_t{LocationAsChannel(Doid_t(do1), 720)}, 5, STATESERVER_OBJECT_DELETE_RAM)
	dg.AddDoid(Doid_t(do2))
	loc3.Expect(t, *dg, false)

	// Cleanup
	time.Sleep(10 * time.Millisecond)
	for _, conn := range []*TestChannelConnection{conn, children, loc1, loc2, loc3} {
		conn.Close()
	}
}

func TestStateServer_ActiveZones(t *testing.T) {
	do0, do1, do2 := Channel_t(11), Channel_t(22), Channel_t(33)
	conn := connect(5)

	instantiateObject(conn, 5, Doid_t(do0), 0, 0, 0)
	instantiateObject(conn, 5, Doid_t(do1), Doid_t(do0), 1234, 0)
	instantiateObject(conn, 5, Doid_t(do2), Doid_t(do0), 1337, 0)

	time.Sleep(100 * time.Millisecond)

	dg := (&TestDatagram{}).Create([]Channel_t{do0}, 5, STATESERVER_GET_ACTIVE_ZONES)
	dg.AddUint32(0)
	conn.SendDatagram(*dg)

	dg = (&TestDatagram{}).Create([]Channel_t{5}, do0, STATESERVER_GET_ACTIVE_ZONES_RESP)
	dg.AddUint32(0)
	dg.AddUint16(2)
	dg.AddZone(1234)
	dg.AddZone(1337)
	conn.Expect(t, *dg, false)

	// Cleanup
	deleteObject(conn, 5, Doid_t(do0))
	deleteObject(conn, 5, Doid_t(do1))
	deleteObject(conn, 5, Doid_t(do2))
	time.Sleep(10 * time.Millisecond)
	conn.Close()
}

func TestStateServer_Clrecv(t *testing.T) {
	do := Channel_t(0xF00)
	conn := connect(LocationAsChannel(0xB00B, 0xF00D))

	// Create a DistributedChunk
	dg := (&TestDatagram{}).Create([]Channel_t{100100}, 5, STATESERVER_OBJECT_GENERATE_WITH_REQUIRED_OTHER)
	appendMetaDoidLast(dg, Doid_t(do), 0xB00B, 0xF00D, DistributedChunk)
	dg.AddUint16(12) // blockList size
	dg.AddUint32(10) // blockList[0].x
	dg.AddUint32(20) // blockList[0].y
	dg.AddUint32(30) // blockList[0].z
	dg.AddUint16(1)  // 1 Optional Field
	dg.AddUint16(LastBlock)
	dg.AddUint32(0) // lastBlock.x
	dg.AddUint32(0) // lastBlock.y
	dg.AddUint32(0) // lastBlock.z
	conn.SendDatagram(*dg)

	time.Sleep(10 * time.Millisecond)

	// Expect entry message
	dg = (&TestDatagram{}).Create([]Channel_t{LocationAsChannel(0xB00B, 0xF00D)},
		do, STATESERVER_OBJECT_ENTER_LOCATION_WITH_REQUIRED_OTHER)
	appendMeta(dg, Doid_t(do), 0xB00B, 0xF00D, DistributedChunk)
	dg.AddUint16(12) // blockList size
	dg.AddUint32(10) // blockList[0].x
	dg.AddUint32(20) // blockList[0].y
	dg.AddUint32(30) // blockList[0].z
	dg.AddUint16(1)  // 1 Optional Field
	dg.AddUint16(LastBlock)
	dg.AddUint32(0) // lastBlock.x
	dg.AddUint32(0) // lastBlock.y
	dg.AddUint32(0) // lastBlock.z
	conn.Expect(t, *dg, false)

	// Update the object with new values
	dg = (&TestDatagram{}).Create([]Channel_t{do}, 5, STATESERVER_OBJECT_UPDATE_FIELD_MULTIPLE)
	dg.AddDoid(Doid_t(do))
	dg.AddUint16(3)
	dg.AddUint16(BlockList)
	dg.AddUint16(24) // blockList size
	dg.AddUint32(20) // blockList[0].x
	dg.AddUint32(30) // blockList[0].y
	dg.AddUint32(40) // blockList[0].z
	dg.AddUint32(50) // blockList[1].x
	dg.AddUint32(60) // blockList[2].y
	dg.AddUint32(70) // blockList[3].z
	dg.AddUint16(LastBlock)
	dg.AddUint32(100) // lastBlock.x
	dg.AddUint32(200) // lastBLock.y
	dg.AddUint32(300) // lastBlock.z
	dg.AddUint16(NewBlock)
	dg.AddUint32(1000) // lastBlock.x
	dg.AddUint32(2000) // lastBlock.y
	dg.AddUint32(3000) // lastBlock.z
	conn.SendDatagram(*dg)

	// Only the broadcast field should be sent to the location
	dg = (&TestDatagram{}).Create([]Channel_t{LocationAsChannel(0xB00B, 0xF00D)}, 5, STATESERVER_OBJECT_UPDATE_FIELD)
	dg.AddDoid(Doid_t(do))
	dg.AddUint16(NewBlock)
	dg.AddUint32(1000) // lastBlock.x
	dg.AddUint32(2000) // lastBlock.y
	dg.AddUint32(3000) // lastBlock.z
	conn.Expect(t, *dg, false)

	// And now we have a fully functional stateserver :D
	deleteObject(conn, 5, Doid_t(do))
	time.Sleep(10 * time.Millisecond)
	conn.Close()
}

func TestDatabaseStateServer_Activate(t *testing.T) {
	shard := connect(5)
	database := connect(1200)

	do1 := Channel_t(9001)
	do2 := Channel_t(9002)

	shard.AddChannel(LocationAsChannel(80000, 100))

	dg := (&TestDatagram{}).Create([]Channel_t{do1}, 5, DBSS_OBJECT_ACTIVATE_WITH_DEFAULTS)
	appendMeta(dg, 9001, 80000, 100, DistributedTestObject5)
	shard.SendDatagram(*dg)

	// Expect values to be retrieved from the database
	dg = (&TestDatagram{}).Create([]Channel_t{1200}, do1, DBSERVER_GET_STORED_VALUES)
	dg.AddUint32(0) // Context
	dg.AddDoid(9001)
	dg.AddUint16(2) // 2 required db fields.
	dg.AddString("setRDB3")
	dg.AddString("setRDbD5")

	database.Expect(t, *dg, false)

	// Send back to the DBSS with some required values
	dg = (&TestDatagram{}).Create([]Channel_t{do1}, 1200, DBSERVER_GET_STORED_VALUES_RESP)
	dg.AddUint32(0) // Context
	dg.AddDoid(9001)
	dg.AddUint16(2) // 2 required db fields.
	dg.AddString("setRDB3")
	dg.AddString("setRDbD5")
	dg.AddUint8(0) // Return code

	// setRDB3
	dg.AddUint16(4) // uint32 size
	dg.AddUint32(3117)
	dg.AddBool(true)

	// setRDbD5
	dg.AddUint16(1) // uint8 size
	dg.AddUint8(97)
	dg.AddBool(true)

	database.SendDatagram(*dg)

	// See if it announces its entry into 100.
	dg = (&TestDatagram{}).Create([]Channel_t{LocationAsChannel(80000, 100)}, do1, STATESERVER_OBJECT_ENTER_LOCATION_WITH_REQUIRED)
	appendMeta(dg, 9001, 80000, 100, DistributedTestObject5)
	dg.AddUint32(78)   // setRequired1
	dg.AddUint32(3117) // setRDB3
	shard.Expect(t, *dg, false)

	// Try to activate an already active object.
	dg = (&TestDatagram{}).Create([]Channel_t{do1}, 5, DBSS_OBJECT_ACTIVATE_WITH_DEFAULTS)
	appendMeta(dg, 9001, 80000, 100, DistributedTestObject5)
	shard.SendDatagram(*dg)

	// This object is already active, so this should be ignored.
	shard.ExpectNone(t)
	database.ExpectNone(t)

	// Remove object from ram
	deleteObject(shard, 5, 9001)
	time.Sleep(100 * time.Millisecond)
	shard.Flush()

	// Test for Activate on Database object with other fields
	dg = (&TestDatagram{}).Create([]Channel_t{do1}, 5, DBSS_OBJECT_ACTIVATE_WITH_DEFAULTS_OTHER)
	appendMeta(dg, 9001, 80000, 100, DistributedTestObject5)
	dg.AddUint16(2) // Two other fields:
	dg.AddUint16(SetRequired1)
	dg.AddUint32(0x00a49de2)
	dg.AddUint16(SetBR1)
	dg.AddString("V ybir Whar")
	shard.SendDatagram(*dg)

	// Expect values to be retrieved from the database
	dg = (&TestDatagram{}).Create([]Channel_t{1200}, do1, DBSERVER_GET_STORED_VALUES)
	dg.AddUint32(1) // Context (increased by one)
	dg.AddDoid(9001)
	dg.AddUint16(2) // 2 required db fields.
	dg.AddString("setRDB3")
	dg.AddString("setRDbD5")

	database.Expect(t, *dg, false)

	// Send back to the DBSS with some required values
	dg = (&TestDatagram{}).Create([]Channel_t{do1}, 1200, DBSERVER_GET_STORED_VALUES_RESP)
	dg.AddUint32(1) // Context
	dg.AddDoid(9001)
	dg.AddUint16(2) // 2 fields.
	dg.AddString("setRequired1")
	dg.AddString("setRDB3")
	dg.AddUint8(0) // Return code.

	// setRequired1
	dg.AddUint16(4) // uint32 size
	dg.AddUint32(0x12345678)
	dg.AddBool(true)

	// setRDB3
	dg.AddUint16(4) // uint32 size
	dg.AddUint32(3117)
	dg.AddBool(true)

	database.SendDatagram(*dg)

	// See if it announces its entry into 100.
	dg = (&TestDatagram{}).Create([]Channel_t{LocationAsChannel(80000, 100)}, do1, STATESERVER_OBJECT_ENTER_LOCATION_WITH_REQUIRED_OTHER)
	appendMeta(dg, 9001, 80000, 100, DistributedTestObject5)
	dg.AddUint32(0x00a49de2) // setRequired1
	dg.AddUint32(3117)       // setRDB3
	dg.AddUint16(1)          // One other field:
	dg.AddUint16(SetBR1)
	dg.AddString("V ybir Whar")
	shard.Expect(t, *dg, false)

	// Remove object from ram
	deleteObject(shard, 5, 9001)
	time.Sleep(100 * time.Millisecond)
	shard.Flush()

	// Test for Activate on non-existent Database object
	dg = (&TestDatagram{}).Create([]Channel_t{do2}, 5, DBSS_OBJECT_ACTIVATE_WITH_DEFAULTS)
	appendMeta(dg, 9002, 80000, 100, DistributedTestObject5)
	shard.SendDatagram(*dg)

	// Expect values to be retrieved from the database
	dg = (&TestDatagram{}).Create([]Channel_t{1200}, do2, DBSERVER_GET_STORED_VALUES)
	dg.AddUint32(2) // Context (increased by one)
	dg.AddDoid(9002)
	dg.AddUint16(2) // 2 required db fields.
	dg.AddString("setRDB3")
	dg.AddString("setRDbD5")

	database.Expect(t, *dg, false)

	// Send back to the DBSS with a failure.
	dg = (&TestDatagram{}).Create([]Channel_t{do2}, 1200, DBSERVER_GET_STORED_VALUES_RESP)
	dg.AddUint32(2) // Context
	dg.AddDoid(9002)
	dg.AddUint16(2) // 2 fields.
	dg.AddString("setRDB3")
	dg.AddString("setRDbD5")
	dg.AddUint8(1) // Return code.

	database.SendDatagram(*dg)

	// Expect no entry messages
	shard.ExpectNone(t)
}
