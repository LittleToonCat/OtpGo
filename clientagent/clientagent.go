package clientagent

import (
	"fmt"
	gonet "net"
	"os"
	"os/signal"
	"otpgo/core"
	"otpgo/eventlogger"
	"otpgo/messagedirector"
	"otpgo/net"
	. "otpgo/util"
	"strconv"
	"sync"

	"net/http"

	"github.com/apex/log"
	gluahttp "github.com/cjoudrey/gluahttp"
	gluacrypto "github.com/tengattack/gluacrypto"
	libs "github.com/vadv/gopher-lua-libs"
	lua "github.com/yuin/gopher-lua"
)

type ChannelTracker struct {
	next   Channel_t
	max    Channel_t
	unused []Channel_t
	log    *log.Entry
}

type LuaQueueEntry struct {
	fn     lua.LValue
	client *Client
	args   []lua.LValue
}

type ClientAgent struct {
	net.NetworkServer
	sync.Mutex

	Tracker *ChannelTracker
	config  core.Role
	log     *log.Entry

	rng             messagedirector.Range
	interestTimeout int
	database        Channel_t

	L            *lua.LState
	LQueue       []LuaQueueEntry
	processQueue chan bool

	receiveDatagramFunc *lua.LFunction
}

func NewChannelTracker(min Channel_t, max Channel_t, log *log.Entry) *ChannelTracker {
	return &ChannelTracker{next: min, max: max}
}

func (c *ChannelTracker) alloc() Channel_t {
	var ch Channel_t
	if c.next <= c.max {
		c.next++
		return c.next
	} else if len(c.unused) != 0 {
		ch, c.unused = c.unused[0], c.unused[1:]
		return ch
	} else {
		c.log.Fatalf("CA has no more available channels.")
	}
	return 0
}

func (c *ChannelTracker) free(ch Channel_t) {
	c.unused = append(c.unused, ch)
}

func NewClientAgent(config core.Role) *ClientAgent {
	ca := &ClientAgent{
		config:   config,
		database: config.Database,
		log: log.WithFields(log.Fields{
			"name":    fmt.Sprintf("ClientAgent (%s)", config.Bind),
			"modName": "ClientAgent",
		}),
		LQueue:       []LuaQueueEntry{},
		processQueue: make(chan bool),
	}
	ca.Tracker = NewChannelTracker(Channel_t(config.Channels.Min), Channel_t(config.Channels.Max), ca.log)

	ca.rng = messagedirector.Range{Min: Channel_t(config.Channels.Min), Max: Channel_t(config.Channels.Max)}
	if ca.rng.Size() <= 0 {
		ca.log.Fatal("Failed to instantiate CA: invalid channel range")
		return nil
	}

	if ca.config.Tuning.Interest_Timeout == 0 {
		ca.config.Tuning.Interest_Timeout = 5
	}

	ca.interestTimeout = config.Tuning.Interest_Timeout

	// Init Lua state
	ca.L = lua.NewState()

	// Preload libaries
	libs.Preload(ca.L)
	// Replace gopher-lua-libs's crypto module with
	// gluacrypto since it has more methods.
	gluacrypto.Preload(ca.L)
	// Used for web requests within Lua
	ca.L.PreloadModule("http", gluahttp.NewHttpModule(&http.Client{}).Loader)

	RegisterLuaUtilTypes(ca.L)
	core.RegisterLuaDCTypes(ca.L)
	RegisterClientType(ca.L)

	// Set globals
	ca.L.SetGlobal("SERVER_VERSION", lua.LString(ca.config.Version))
	if ca.config.DC_Hash != 0 {
		ca.L.SetGlobal("DC_HASH", lua.LNumber(ca.config.DC_Hash))
	} else {
		ca.L.SetGlobal("DC_HASH", lua.LNumber(core.DC.Get_hash()))
	}

	ca.L.SetGlobal("dcFile", core.NewLuaDCFile(ca.L, core.DC))


	ca.log.Infof("Running Lua script: %s", ca.config.Lua_File)
	if err := ca.L.DoFile(ca.config.Lua_File); err != nil {
		ca.log.Fatal(err.Error())
		return nil
	}

	// Santity check to make sure certian global functions exists:
	if function, ok := ca.L.GetGlobal("receiveDatagram").(*lua.LFunction); ok {
		ca.receiveDatagramFunc = function
	} else {
		ca.log.Fatal("Missing \"receiveDatagram\" function in Lua script.")
		return nil
	}

	ca.Handler = ca
	errChan := make(chan error)
	go func() {
		err := <-errChan
		switch err {
		case nil:
			ca.log.Infof("Opened listening socket at %s", config.Bind)
		default:
			ca.log.Fatal(err.Error())
		}
	}()
	go ca.queueLoop()
	go ca.Start(config.Bind, errChan, config.Proxy)
	return ca
}

func (c *ClientAgent) getEntryFromQueue() LuaQueueEntry {
	c.Lock()
	defer c.Unlock()

	op := c.LQueue[0]
	c.LQueue[0] = LuaQueueEntry{}
	c.LQueue = c.LQueue[1:]
	if len(c.LQueue) == 0 {
		// Recreate the queue slice. This prevents the capacity from growing indefinitely and allows old entries to drop off as soon as possible from the backing array.
		c.LQueue = make([]LuaQueueEntry, 0)
	}
	return op
}

func (c *ClientAgent) queueLoop() {
	signalCh := make(chan os.Signal, 1)
	signal.Notify(signalCh, os.Interrupt)

	for {
		select {
		case <-c.processQueue:
			for len(c.LQueue) > 0 {
				entry := c.getEntryFromQueue()
				err := c.L.CallByParam(lua.P{
					Fn:      entry.fn,
					NRet:    0,
					Protect: true,
				}, entry.args...)
				if err != nil {
					var event eventlogger.LoggedEvent
					if entry.client != nil {
						entry.client.log.Errorf("Lua error:\n%s", err.Error())
						event = eventlogger.NewLoggedEvent("lua-error", "Client", strconv.FormatUint(uint64(entry.client.allocatedChannel), 10), err.Error())
						entry.client.sendDisconnect(CLIENT_DISCONNECT_GENERIC, "Lua error has occured.", true)
					} else {
						c.log.Errorf("Lua error:\n%s", err.Error())
						event = eventlogger.NewLoggedEvent("lua-error", "ClientAgent", "", err.Error())
					}
					event.Send()
				}
			}
		case <-signalCh:
			return
		case <-core.StopChan:
			return
		}
	}
}

func (c *ClientAgent) CallLuaFunction(fn lua.LValue, client *Client, args ...lua.LValue) {
	// Queue the call
	c.Lock()
	entry := LuaQueueEntry{fn, client, args}
	c.LQueue = append(c.LQueue, entry)
	c.Unlock()

	select {
	case c.processQueue <- true:
	default:
	}
}

func (c *ClientAgent) HandleConnect(conn gonet.Conn) {
	c.log.Debugf("Incoming connection from %s", conn.RemoteAddr())
	NewClient(c.config, c, conn)
}

func (c *ClientAgent) Allocate() Channel_t {
	return c.Tracker.alloc()
}
