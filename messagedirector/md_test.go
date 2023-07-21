// All of the tests below can be directly attributed Astron C++; major credit to them.

package messagedirector

import (
	"otpgo/core"
	. "otpgo/test"
	. "otpgo/util"
	"github.com/apex/log"
	"os"
	"testing"
	"time"
)

var mainClient, client1, client2 *TestMDConnection

func TestMain(m *testing.M) {
	// SETUP
	// Silence the (very annoying) logger while we're testing
	log.SetHandler(log.HandlerFunc(func(*log.Entry) error { return nil }))
	upstream := StartUpstream("127.0.0.1:57124")

	StartDaemon(
		core.ServerConfig{MessageDirector: struct {
			Bind    string
			Connect string
		}{Bind: "127.0.0.1:57123", Connect: "127.0.0.1:57124"}})
	Start()

	for {
		if upstream.Server != nil {
			break
		}
	}
	mainClient = (&TestMDConnection{}).Set(*upstream.Server, "main")

	client1 = (&TestMDConnection{}).Connect(":57123", "client #1")
	client2 = (&TestMDConnection{}).Connect(":57123", "client #2")

	code := m.Run()

	// TEARDOWN
	mainClient.Close()
	client1.Close()
	client2.Close()
	os.Exit(code)
}

func TestMD_Single(t *testing.T) {
	mainClient.Flush()

	// Send a datagram
	dg := (&TestDatagram{}).Create([]Channel_t{1234}, 4321, 1337)
	dg.AddString("HELLO #2!")
	client1.SendDatagram(*dg)

	// The MD should pass it to the main client
	mainClient.Expect(t, *dg, false)
}

func TestMD_Subscribe(t *testing.T) {
	mainClient.Flush()
	client1.Flush()
	client2.Flush()

	// Subscribe to a channel
	dg := (&TestDatagram{}).CreateAddChannel(123456789)
	client1.SendDatagram(*dg)
	client1.ExpectNone(t)
	mainClient.Expect(t, *dg, false)

	// Send a test datagram on the other connection
	dg = (&TestDatagram{}).Create([]Channel_t{123456789}, 0, 1234)
	dg.AddUint32(0xDEADBEEF)
	client2.SendDatagram(*dg)
	client1.Expect(t, *dg, false)
	// MD should relay the message upwards
	mainClient.Expect(t, *dg, false)

	// Subscribe on the other connection
	dg = (&TestDatagram{}).CreateAddChannel(123456789)
	client2.SendDatagram(*dg)
	client2.ExpectNone(t)
	// MD should not ask for the channel again
	mainClient.ExpectNone(t)

	// Send a test datagram on the first connection
	dg = (&TestDatagram{}).Create([]Channel_t{123456789}, 0, 1234)
	dg.AddUint32(0xDEADBEEF)
	client1.SendDatagram(*dg)
	client2.Expect(t, *dg, false)
	mainClient.Expect(t, *dg, false)

	// Unsubscribe on the first connection
	dg = (&TestDatagram{}).CreateRemoveChannel(123456789)
	client1.SendDatagram(*dg)
	client1.ExpectNone(t)
	client2.ExpectNone(t)
	// MD should not unsubscribe from parent
	mainClient.ExpectNone(t)

	// Send another datagram on the second connection
	dg = (&TestDatagram{}).Create([]Channel_t{123456789}, 0, 1234)
	dg.AddUint32(0xDEADBEEF)
	client2.SendDatagram(*dg)
	mainClient.Expect(t, *dg, false) // Should be sent upwards
	client2.ExpectNone(t)            // Should not be relayed
	client1.ExpectNone(t)            // Should not be echoed back

	// CLose the second connection, auto-unsubscribing it
	client2.Close()
	client2 = (&TestMDConnection{}).Connect(":57123", "client #2")
	client1.ExpectNone(t)
	// MD should unsubscribe from parent
	mainClient.Expect(t, *(&TestDatagram{}).CreateRemoveChannel(123456789), false)
}

func TestMD_Multi(t *testing.T) {
	mainClient.Flush()
	client1.Flush()
	client2.Flush()

	// Subscribe to a pair of channels on the first client
	for _, ch := range []Channel_t{1111, 2222} {
		dg := (&TestDatagram{}).CreateAddChannel(ch)
		client1.SendDatagram(*dg)
	}

	// Subscribe to a pair of channels on the second
	for _, ch := range []Channel_t{2222, 3333} {
		dg := (&TestDatagram{}).CreateAddChannel(ch)
		client2.SendDatagram(*dg)
	}

	// Because Astron is not an instantaneous system, the control messages need time to process
	time.Sleep(300 * time.Millisecond)
	mainClient.Flush()

	// A datagram on channel 2222 should be delivered on both clients
	dg := (&TestDatagram{}).Create([]Channel_t{2222}, 0, 1337)
	dg.AddUint32(0xDEADBEEF)
	mainClient.SendDatagram(*dg)
	client1.Expect(t, *dg, false)
	client2.Expect(t, *dg, false)

	// A datagram to channels 1111 and 3333 should be delivered to both as well
	dg = (&TestDatagram{}).Create([]Channel_t{1111, 3333}, 0, 1337)
	dg.AddUint32(0xDEADBEEF)
	mainClient.SendDatagram(*dg)
	client1.Expect(t, *dg, false)
	client2.Expect(t, *dg, false)

	// A datagram should only be delivered once if multiple channels match
	dg = (&TestDatagram{}).Create([]Channel_t{1111, 2222}, 0, 1337)
	dg.AddUint32(0xDEADBEEF)
	mainClient.SendDatagram(*dg)
	client1.Expect(t, *dg, false)
	client1.ExpectNone(t)
	client2.Expect(t, *dg, false)

	// Verify behavior for datagrams with duplicate recipients
	dg = (&TestDatagram{}).Create([]Channel_t{1111, 2222, 3333, 1111, 1111,
		2222, 3333, 3333, 2222}, 0, 1337)
	dg.AddUint32(0xDEADBEEF)
	mainClient.SendDatagram(*dg)
	client1.Expect(t, *dg, false)
	client1.ExpectNone(t)
	client2.Expect(t, *dg, false)
	client2.ExpectNone(t)

	// Send the same message through the first client; verify behavior
	client1.SendDatagram(*dg)
	mainClient.Expect(t, *dg, false)
	mainClient.ExpectNone(t)
	client2.Expect(t, *dg, false)
	client2.ExpectNone(t)
	client1.ExpectNone(t)

	// Unsubscribe from the channels
	for _, ch := range []Channel_t{1111, 2222, 3333} {
		dg := (&TestDatagram{}).CreateRemoveChannel(ch)
		client1.SendDatagram(*dg)
		client2.SendDatagram(*dg)
	}

	time.Sleep(300 * time.Millisecond)
}

func TestMD_PostRemove(t *testing.T) {
	mainClient.Flush()
	client1.Flush()
	client2.Flush()

	prDg := (&TestDatagram{}).Create([]Channel_t{6969}, 6969, 2000)
	prDg.AddString("nosyliam is epic")

	addPrDg := (&TestDatagram{}).CreateAddPostRemove(420, *prDg)
	client1.SendDatagram(*addPrDg)

	// Post remove should be pre-routed upstream
	mainClient.Expect(t, *addPrDg, false)

	// Nothing else should be happening
	mainClient.ExpectNone(t)
	client1.ExpectNone(t)
	client2.ExpectNone(t)

	// Reconnect and see if the PR gets sent
	client1.Close()
	client1 = (&TestMDConnection{}).Connect(":57123", "client #1")

	// Upstream should receive the PR and a clear_post_remove
	clearPrDg := (&TestDatagram{}).CreateClearPostRemove(420)
	mainClient.ExpectMany(t, []Datagram{*prDg, *clearPrDg}, false, true)

	// Reconnect; the PR shouldn't be sent again
	client1.Close()
	client1 = (&TestMDConnection{}).Connect(":57123", "client #1")
	mainClient.ExpectNone(t)

	// Add the previous PR to the other client
	addPrDg = (&TestDatagram{}).CreateAddPostRemove(69, *prDg)
	client2.SendDatagram(*addPrDg)
	mainClient.Expect(t, *addPrDg, false)

	// Cancel it!
	clearPrDg = (&TestDatagram{}).CreateClearPostRemove(69)
	client2.SendDatagram(*clearPrDg)
	mainClient.Expect(t, *clearPrDg, false)

	// Did it work?
	client2.Close()
	client2 = (&TestMDConnection{}).Connect(":57123", "client #2")
	mainClient.ExpectNone(t)

	// Let's add some more PRs to client 1
	prs := []Datagram{
		*(&TestDatagram{}).CreateAddPostRemove(69, *prDg),
		*(&TestDatagram{}).CreateAddPostRemove(69,
			*(&TestDatagram{}).Create([]Channel_t{0x1337}, 6969, 0xBEEF)),
		*(&TestDatagram{}).CreateAddPostRemove(420,
			*(&TestDatagram{}).Create([]Channel_t{0x13337}, 6969, 0xDEAD)),
	}
	client1.SendDatagram(prs[0])
	client1.SendDatagram(prs[1])
	client1.SendDatagram(prs[2])

	// They should be pre-routed upstream
	mainClient.ExpectMany(t, prs, false, true)

	// Now our clients should be empty
	mainClient.ExpectNone(t)
	client1.ExpectNone(t)
	client2.ExpectNone(t)

	// Reconnect and see if all of the datagrams get sent
	client1.Close()
	client1 = (&TestMDConnection{}).Connect(":57123", "client #1")

	expected := []Datagram{
		*prDg,
		*(&TestDatagram{}).Create([]Channel_t{0x1337}, 6969, 0xBEEF),
		*(&TestDatagram{}).Create([]Channel_t{0x13337}, 6969, 0xDEAD),
		*(&TestDatagram{}).CreateClearPostRemove(69),
		*(&TestDatagram{}).CreateClearPostRemove(420),
	}

	mainClient.ExpectMany(t, expected, false, true)
	mainClient.ExpectNone(t)
}

func TestMD_Ranges(t *testing.T) {
	mainClient.Flush()
	client1.Flush()
	client2.Flush()

	// If we don't lower the message receive timeouts this test will take forever
	mainClient.Timeout = 50
	client1.Timeout = 50
	client2.Timeout = 50

	// Subscribe to range 1000 - 1999
	dg := (&TestDatagram{}).CreateAddRange(1000, 1999)
	client1.SendDatagram(*dg)
	// Upstream should receive the range as well
	mainClient.Expect(t, *dg, false)

	checkChannels := func(channels map[Channel_t]bool) {
		for channel, shouldReceive := range channels {
			dg := (&TestDatagram{}).Create([]Channel_t{channel}, 1337, 6969)
			dg.AddUint16(uint16(channel))
			client2.SendDatagram(*dg)

			if shouldReceive {
				//fmt.Printf("Checking chan %d\n", channel)
				client1.Expect(t, *dg, false)
				client1.ExpectNone(t)
			} else {
				//fmt.Printf("Checking chan %d\n", channel)
				client1.ExpectNone(t)
			}

			mainClient.Expect(t, *dg, false)
		}
	}

	checkChannels(map[Channel_t]bool{
		500:  false,
		999:  false,
		1000: true,
		1500: true,
		1999: true,
		2000: false,
		2050: false,
	})

	// Range subscriptions should still only receive messages once
	dg = (&TestDatagram{}).Create([]Channel_t{500, 1001, 1500}, 1337, 6969)
	dg.AddString("nosyliam is epic #2")
	client2.SendDatagram(*dg)
	client1.Expect(t, *dg, false)
	client1.ExpectNone(t)
	mainClient.Expect(t, *dg, false)

	// Slice the range
	dg = (&TestDatagram{}).CreateRemoveRange(1300, 1700)
	client1.SendDatagram(*dg)
	mainClient.Expect(t, *dg, false)

	checkChannels(map[Channel_t]bool{
		500:  false,
		999:  false,
		1000: true,
		1299: true,
		1300: false,
		1500: false,
		1699: false,
		1700: false,
		1701: true,
		1800: true,
	})

	dg = (&TestDatagram{}).CreateAddRange(1900, 2100)
	client1.SendDatagram(*dg)
	mainClient.Expect(t, *dg, false)

	checkChannels(map[Channel_t]bool{
		500:  false,
		999:  false,
		1000: true,
		1300: false,
		1500: false,
		1699: false,
		1700: false,
		1701: true,
		1900: true,
		1999: true,
		2000: true,
		2050: true,
		2101: false,
	})

	// This behavior differs from Astron C++ as ranges in
	//  AstronGo are cleaned up after each removal operation
	dg = (&TestDatagram{}).CreateRemoveRange(1000, 1999)
	client1.SendDatagram(*dg)
	mainClient.ExpectMany(t, []Datagram{
		*(&TestDatagram{}).CreateRemoveRange(1000, 1299),
		*(&TestDatagram{}).CreateRemoveRange(1701, 1999),
	}, false, false)

	checkChannels(map[Channel_t]bool{
		500:  false,
		999:  false,
		1000: false,
		1001: false,
		1999: false,
		2000: true,
		2050: true,
		2500: false,
	})

	// When client one dies, it's last remaining range should die
	client1.Close()
	client1 = (&TestMDConnection{}).Connect(":57123", "client #1")
	mainClient.Expect(t, *(&TestDatagram{}).CreateRemoveRange(2000, 2100), false)
	mainClient.ExpectNone(t)

	// Set up a new range
	dg = (&TestDatagram{}).CreateAddRange(3000, 5000)
	client1.SendDatagram(*dg)
	// Upstream should receive the range as well
	mainClient.Expect(t, *dg, false)

	checkChannels(map[Channel_t]bool{
		2999: false,
		3000: true,
		4999: true,
		5000: true,
		5001: false,
	})

	// Remove a range that intersects with the front part
	dg = (&TestDatagram{}).CreateRemoveRange(2950, 3043)
	client1.SendDatagram(*dg)
	// Only the subscribed part should be removed upstream
	mainClient.Expect(t, *(&TestDatagram{}).CreateRemoveRange(3000, 3043), false)

	checkChannels(map[Channel_t]bool{
		2999: false,
		3000: false,
		3043: false,
		3044: true,
		4000: true,
		5000: true,
	})

	// Remove a range that intersects with the end part
	dg = (&TestDatagram{}).CreateRemoveRange(4763, 6000)
	client1.SendDatagram(*dg)
	mainClient.Expect(t, *(&TestDatagram{}).CreateRemoveRange(4763, 5000), false)

	checkChannels(map[Channel_t]bool{
		2999: false,
		3000: false,
		3043: false,
		3044: true,
		4000: true,
		4762: true,
		4763: false,
		5000: false,
	})

	// Remove a range in the middle
	dg = (&TestDatagram{}).CreateRemoveRange(3951, 4049)
	client1.SendDatagram(*dg)
	mainClient.Expect(t, *dg, false)

	checkChannels(map[Channel_t]bool{
		3043: false,
		3044: true,
		3802: true,
		3950: true,
		3951: false,
		4049: false,
		4050: true,
		4763: false,
	})

	// Remove an intersection from the lower half
	dg = (&TestDatagram{}).CreateRemoveRange(4030, 4070)
	client1.SendDatagram(*dg)
	mainClient.Expect(t, *(&TestDatagram{}).CreateRemoveRange(4050, 4070), false)

	checkChannels(map[Channel_t]bool{
		3044: true,
		3802: true,
		3950: true,
		3951: false,
		4070: false,
		4133: true,
		4762: true,
		4763: false,
	})

	// Remove an intersection from the upper half
	dg = (&TestDatagram{}).CreateRemoveRange(3891, 4040)
	client1.SendDatagram(*dg)
	mainClient.Expect(t, *(&TestDatagram{}).CreateRemoveRange(3891, 3950), false)

	checkChannels(map[Channel_t]bool{
		3043: false,
		3044: true,
		3890: true,
		3891: false,
		3893: false,
		4071: true,
		4762: true,
		4763: false,
	})

	// Remove an intersection of the upper and lower range
	dg = (&TestDatagram{}).CreateRemoveRange(3700, 4200)
	client1.SendDatagram(*dg)
	mainClient.ExpectMany(t, []Datagram{*(&TestDatagram{}).CreateRemoveRange(3700, 3890),
		*(&TestDatagram{}).CreateRemoveRange(4071, 4200)}, false, true)

	checkChannels(map[Channel_t]bool{
		3043: false,
		3044: true,
		3699: true,
		3700: false,
		4200: false,
		4201: true,
		4762: true,
		4763: false,
	})

	// Introduce the second client to an intersecting range
	dg = (&TestDatagram{}).CreateAddRange(3500, 4500)
	client2.SendDatagram(*dg)
	mainClient.Expect(t, *dg, false)

	// Remove an upper part of the lower range
	dg = (&TestDatagram{}).CreateRemoveRange(3650, 3800)
	client1.SendDatagram(*dg)
	mainClient.ExpectNone(t)

	checkChannels(map[Channel_t]bool{
		// Lower range
		3043: false,
		3044: true, // Lower bound
		3649: true, // Upper bound
		3650: false,

		3787: false,
		4000: false,

		// Upper range
		4200: false,
		4201: true, // Lower bound
	})

	// Do some even more complicated stuff (see Astron C++'s unit tests)
	dg = (&TestDatagram{}).CreateRemoveRange(3475, 3525)
	client1.SendDatagram(*dg)
	mainClient.Expect(t, *(&TestDatagram{}).CreateRemoveRange(3475, 3499), false)

	checkChannels(map[Channel_t]bool{
		// Lower range
		3043: false,
		3044: true, // Lower bound
		3474: true, // Upper bound
		3475: false,

		3483: false,
		3499: false,

		// Mid range
		3525: false,
		3526: true, // Lower bound
		3600: true,
		3649: true,
		3650: false,

		// Upper range
		4200: false,
		4201: true, // Lower bound
	})

	// Do the last, most complicated thing!
	dg = (&TestDatagram{}).CreateRemoveRange(3620, 4300)
	client2.SendDatagram(*dg)
	mainClient.Expect(t, *(&TestDatagram{}).CreateRemoveRange(3650, 4200), false)

	checkChannels(map[Channel_t]bool{
		// Lower range
		3474: true, // Upper bound
		3475: false,

		// Mid range
		3525: false,
		3526: true, // Lower bound
		3649: true, // Upper bound
		3650: false,

		// Upper range
		4200: false,
		4201: true, // Lower bound
		4762: true, // Upper bound
		4763: false,
	})

	client2.Close()
	client2 = (&TestMDConnection{}).Connect(":57123", "client #2")
	mainClient.Expect(t, *(&TestDatagram{}).CreateRemoveRange(3500, 3525), false)
	mainClient.ExpectNone(t)

	dg = (&TestDatagram{}).CreateAddRange(1000, 5000)
	client2.SendDatagram(*dg)
	mainClient.Expect(t, *dg, false)

	client2.SendDatagram(*(&TestDatagram{}).CreateRemoveRange(1000, 5000))
	mainClient.ExpectMany(t, []Datagram{
		*(&TestDatagram{}).CreateRemoveRange(1000, 3043),
		*(&TestDatagram{}).CreateRemoveRange(3475, 3525),
		*(&TestDatagram{}).CreateRemoveRange(3650, 4200),
		*(&TestDatagram{}).CreateRemoveRange(4763, 5000),
	}, false, false)

	// Now we have a fully functional MD!
	client1.Close()
	client2.Close()
	mainClient.Close()

	return
}
