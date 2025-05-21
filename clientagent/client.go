package clientagent

import (
	"otpgo/core"
	"slices"
	"sort"
	"strconv"
	"sync/atomic"

	"fmt"
	gonet "net"
	"otpgo/eventlogger"
	"otpgo/messagedirector"
	"otpgo/net"
	. "otpgo/util"
	"sync"
	"time"

	"otpgo/dc"

	"github.com/apex/log"
	lua "github.com/yuin/gopher-lua"
)

type ClientState int

const (
	CLIENT_STATE_NEW ClientState = iota
	CLIENT_STATE_ANONYMOUS
	CLIENT_STATE_ESTABLISHED
)

type DeclaredObject struct {
	do Doid_t
	dc dc.DCClass
}

type OwnedObject struct {
	DeclaredObject
	parent Doid_t
	zone   Zone_t
}

type VisibleObject struct {
	DeclaredObject
	parent Doid_t
	zone   Zone_t
}

type Interest struct {
	id     uint16
	parent Doid_t
	zones  []Zone_t
}

func (i *Interest) hasZone(zone Zone_t) bool {
	for _, z := range i.zones {
		if z == zone {
			return true
		}
	}
	return false
}

type Client struct {
	sync.Mutex
	messagedirector.MDParticipantBase

	// Client properties
	config core.Role
	ca     *ClientAgent
	log    *log.Entry

	userTable *lua.LTable

	allocatedChannel Channel_t
	channel          Channel_t
	state            ClientState
	authenticated    bool

	context               atomic.Uint32
	createContextMap      *MutexMap[uint32, func(doId Doid_t)]
	getContextMap         *MutexMap[uint32, func(doId Doid_t, dgi *DatagramIterator)]
	queryFieldsContextMap *MutexMap[uint32, func(dgi *DatagramIterator)]

	queue         []Datagram
	queueLock     sync.Mutex

	shouldProcess chan bool
	stopChan      chan bool

	seenObjects       []Doid_t
	sessionObjects    []Doid_t
	historicalObjects []Doid_t

	visibleObjects   *MutexMap[Doid_t, VisibleObject]
	declaredObjects  *MutexMap[Doid_t, DeclaredObject]
	ownedObjects     *MutexMap[Doid_t, OwnedObject]
	pendingObjects   *MutexMap[Doid_t, uint32]
	interests        *MutexMap[uint16, Interest]
	pendingInterests *MutexMap[uint32, *InterestOperation]
	sendableFields   *MutexMap[Doid_t, []uint16]

	conn   gonet.Conn
	client *net.Client
	lock   sync.Mutex

	cleanDisconnect  bool
	allowedInterests InterestPermission
	heartbeat        *time.Timer
	stopHeartbeat    chan bool
	terminationBegun atomic.Bool
	terminationLock  sync.Mutex
}

func NewClient(config core.Role, ca *ClientAgent, conn gonet.Conn) *Client {
	c := &Client{
		config:                config,
		ca:                    ca,
		conn:                  conn,
		queue:                 []Datagram{},
		shouldProcess:         make(chan bool),
		stopChan:              make(chan bool),
		createContextMap:      NewMutexMap[uint32, func(doId Doid_t)](),
		getContextMap:         NewMutexMap[uint32, func(doId Doid_t, dgi *DatagramIterator)](),
		queryFieldsContextMap: NewMutexMap[uint32, func(dgi *DatagramIterator)](),
		authenticated:         false,
		visibleObjects:        NewMutexMap[Doid_t, VisibleObject](),
		declaredObjects:       NewMutexMap[Doid_t, DeclaredObject](),
		ownedObjects:          NewMutexMap[Doid_t, OwnedObject](),
		pendingObjects:        NewMutexMap[Doid_t, uint32](),
		interests:             NewMutexMap[uint16, Interest](),
		pendingInterests:      NewMutexMap[uint32, *InterestOperation](),
		sendableFields:        NewMutexMap[Doid_t, []uint16](),
	}
	// This is to prevent termination calls before the client can be fully initialized.
	c.terminationLock.Lock()

	c.init(config, conn)
	c.Init(c)

	c.allocatedChannel = ca.Allocate()
	if c.allocatedChannel == 0 {
		c.terminationLock.Unlock()
		c.sendDisconnect(CLIENT_DISCONNECT_GENERIC, "Client capacity reached", false)
		return nil
	}
	c.channel = c.allocatedChannel
	c.SetName(fmt.Sprintf("Client (%d)", c.channel))

	c.log = log.WithFields(log.Fields{
		"name":    c.Name(),
		"modName": "Client",
		"id":      fmt.Sprintf("%d", c.channel),
	})

	c.SubscribeChannel(c.channel)
	c.SubscribeChannel(CHANNEL_CLIENT_BROADCAST)

	go c.queueLoop()

	c.terminationLock.Unlock()
	return c
}

func (c *Client) sendDisconnect(reason uint16, message string, security bool) {
	// TODO: Implement security loglevel
	var eventType string
	if security {
		c.log.Errorf("[SECURITY] Ejecting client (%s): %s", reason, message)
		eventType = "client-ejected-security"
	} else {
		c.log.Errorf("Ejecting client (%s): %s", reason, message)
		eventType = "client-ejected"
	}

	event := eventlogger.NewLoggedEvent(eventType, "Client", strconv.FormatUint(uint64(c.allocatedChannel), 10), fmt.Sprintf("%d|%s", reason, message))
	event.Send()

	if c.client.ConnectedAndIsNotDisconnecting() {
		resp := NewDatagram()
		resp.AddUint16(CLIENT_GO_GET_LOST)
		resp.AddUint16(reason)
		resp.AddString(message)
		c.client.SendDatagram(resp)

		c.cleanDisconnect = true
		c.Terminate(nil)
	}
}

func (c *Client) annihilate() {
	c.Lock()
	defer c.Unlock()

	if c.IsTerminated() {
		return
	}

	c.ca.Tracker.free(c.allocatedChannel)

	// Delete all session object
	for len(c.sessionObjects) > 0 {
		var do Doid_t
		do, c.sessionObjects = c.sessionObjects[0], c.sessionObjects[1:]
		c.log.Debugf("Client exited, deleting session object ID=%d", do)
		dg := NewDatagram()
		dg.AddServerHeader(Channel_t(do), c.channel, STATESERVER_OBJECT_DELETE_RAM)
		dg.AddDoid(do)
		c.RouteDatagram(dg)
	}

	iterator := c.pendingInterests.Iterator()
	for iterator.Next() {
		interestOperation := iterator.Value().Interface().(*InterestOperation)
		go interestOperation.finish()
	}
	c.pendingInterests.RUnlock()

	c.Cleanup()
}

func (c *Client) lookupInterests(parent Doid_t, zone Zone_t) []Interest {
	var interests []Interest

	iterator := c.interests.Iterator()
	for iterator.Next() {
		interest := iterator.Value().Interface().(Interest)
		if parent == interest.parent && interest.hasZone(zone) {
			interests = append(interests, interest)
		}
	}
	c.interests.RUnlock()
	return interests
}

func (c *Client) buildInterest(id uint16, parent Doid_t, zones []Zone_t) Interest {
	int := Interest{
		id:     id,
		parent: parent,
		zones:  zones,
	}
	return int
}

func (c *Client) addInterest(i Interest, context uint32, caller Channel_t) {
	c.log.Debugf("addInterest(%t)", i)
	var zones []Zone_t

	for _, zone := range i.zones {
		if len(c.lookupInterests(i.parent, zone)) == 0 {
			zones = append(zones, zone)
		}
	}

	if prevInt, ok := c.interests.Get(i.id); ok {
		// This interest already exists, so it is being altered
		var killedZones []Zone_t

		if prevInt.parent == i.parent {
			for _, zone := range prevInt.zones {
				if len(c.lookupInterests(i.parent, zone)) > 1 {
					// Another interest has this zone, so ignore it
					continue
				}
				if !i.hasZone(zone) {
					killedZones = append(killedZones, zone)
				}
			}
		} else {
			killedZones = prevInt.zones
		}

		c.closeZones(prevInt.parent, killedZones)
	}
	c.interests.Set(i.id, i, false)

	if len(zones) == 0 {
		// We aren't requesting any new zones, so let the client know we finished
		c.notifyInterestDone(i.id, []Channel_t{caller})
		c.handleInterestDone(i.id, context)
		return
	}

	// Build a new IOP otherwise
	iopContext := c.context.Add(1)
	iop := NewInterestOperation(c, c.config.Tuning.Interest_Timeout, i.id,
		context, iopContext, i.parent, zones, caller)
	c.pendingInterests.Set(iopContext, iop, false)

	resp := NewDatagram()
	resp.AddServerHeader(Channel_t(i.parent), c.channel, STATESERVER_OBJECT_GET_ZONES_OBJECTS)
	resp.AddUint32(iopContext)
	resp.AddDoid(i.parent)
	resp.AddUint16(uint16(len(zones)))
	for _, zone := range zones {
		resp.AddZone(zone)
		c.SubscribeChannel(LocationAsChannel(i.parent, zone))
	}
	c.RouteDatagram(resp)
}

func (c *Client) removeInterest(i Interest, context uint32) {
	var zones []Zone_t

	// Check if other interest exists with the same location,
	// we don't want to close them if there are.
	for _, zone := range i.zones {
		if len(c.lookupInterests(i.parent, zone)) == 1 {
			zones = append(zones, zone)
		}
	}

	c.closeZones(i.parent, zones)
	// c.notifyInterestDone(i.id, []Channel_t{caller})
	c.handleInterestDone(i.id, context)

	c.interests.Delete(i.id, false)
}

func (c *Client) closeZones(parent Doid_t, zones []Zone_t) {
	c.log.Debugf("Closing zones: %t", zones)
	var toRemove []Doid_t

	iterator := c.visibleObjects.Iterator()
	for iterator.Next() {
		obj := iterator.Value().Interface().(VisibleObject)
		if obj.parent != parent {
			// Object does not belong to the parent in question
			continue
		}

		for i := range zones {
			if zones[i] == obj.zone {
				for i := range c.sessionObjects {
					if c.sessionObjects[i] == obj.do {
						c.sendDisconnect(CLIENT_DISCONNECT_SESSION_OBJECT_DELETED,
							"A session object has unexpectedly left interest.", false)
						c.visibleObjects.RUnlock()
						return
					}
				}

				c.handleRemoveObject(obj.do, false)
				tempSeenObjectSlice := make([]Doid_t, 0)
				for _, o := range c.seenObjects {
					if o != obj.do {
						tempSeenObjectSlice = append(tempSeenObjectSlice, o)
					}
				}
				c.seenObjects = tempSeenObjectSlice
				toRemove = append(toRemove, obj.do)
			}
		}
	}
	c.visibleObjects.RUnlock()

	for _, do := range toRemove {
		c.addHistoricalObject(do)
		c.visibleObjects.Delete(do, false)
	}

	for _, zone := range zones {
		c.UnsubscribeChannel(LocationAsChannel(parent, zone))
	}
}

func (c *Client) historicalObject(do Doid_t) bool {
	for i := range c.historicalObjects {
		if c.historicalObjects[i] == do {
			return true
		}
	}
	return false
}

func (c *Client) addHistoricalObject(do Doid_t) {
	if c.historicalObject(do) {
		return
	}
	c.historicalObjects = append(c.historicalObjects, do)
}

func (c *Client) lookupObject(do Doid_t) dc.DCClass {
	// Search UberDOGs
	for i := range core.Uberdogs {
		if core.Uberdogs[i].Id == do {
			return core.Uberdogs[i].Class
		}
	}

	// Check the object cache
	if obj, ok := c.ownedObjects.Get(do); ok {
		return obj.dc
	}

	if slices.Contains(c.seenObjects, do) {
		if obj, ok := c.visibleObjects.Get(do); ok {
			return obj.dc
		}
	}

	// Check declared objects
	if obj, ok := c.declaredObjects.Get(do); ok {
		return obj.dc
	}

	// We don't know :(
	return nil
}

func (c *Client) tryQueuePending(do Doid_t, dg Datagram) bool {
	if context, ok := c.pendingObjects.Get(do); ok {
		if iop, ok := c.pendingInterests.Get(context); ok {
			iop.pendingQueue = append(iop.pendingQueue, dg)
			return true
		}
	}
	return false
}

func (c *Client) handleObjectEntrance(dgi *DatagramIterator, other bool) {
	do, parent, zone, dc := dgi.ReadDoid(), dgi.ReadDoid(), dgi.ReadZone(), dgi.ReadUint16()

	c.pendingObjects.Delete(do, false)

	for i := range c.seenObjects {
		if c.seenObjects[i] == do {
			return
		}
	}

	if _, ok := c.ownedObjects.Get(do); ok {
		for i := range c.sessionObjects {
			if c.sessionObjects[i] == do {
				return
			}
		}
	}

	if _, ok := c.visibleObjects.Get(do); !ok {
		cls := core.DC.GetClass(int(dc))
		c.visibleObjects.Set(do, VisibleObject{
			DeclaredObject: DeclaredObject{
				do: do,
				dc: cls,
			},
			parent: parent,
			zone:   zone,
		}, false)
	}
	c.seenObjects = append(c.seenObjects, do)

	c.handleAddObject(do, parent, zone, dc, dgi, other)
}

func (c *Client) notifyInterestDone(interestId uint16, callers []Channel_t) {
	if len(callers) == 0 {
		return
	}

	resp := NewDatagram()
	resp.AddMultipleServerHeader(callers, c.channel, CLIENTAGENT_DONE_INTEREST_RESP)
	resp.AddChannel(c.channel)
	resp.AddUint16(interestId)
	c.RouteDatagram(resp)
}

func (c *Client) HandleDatagram(dg Datagram, dgi *DatagramIterator) {
	c.Lock()
	defer c.Unlock()

	sender := dgi.ReadChannel()
	msgType := dgi.ReadUint16()
	if sender == c.channel {
		return
	}

	switch msgType {
	case CLIENTAGENT_EJECT:
		reason, error := dgi.ReadUint16(), dgi.ReadString()
		c.sendDisconnect(reason, error, false)
	case CLIENTAGENT_DROP:
		c.lock.Lock()
		c.Terminate(fmt.Errorf("dropped by outside sender: %d", sender))
		c.lock.Unlock()
	case CLIENT_AGENT_SET_INTEREST:
		// c.context++
		interestId, context, parentDoId := dgi.ReadUint16(), dgi.ReadUint32(), dgi.ReadDoid()
		dgi.ReadUint32() // context is sent twice for some reason?

		zones := []Zone_t{}

		for dgi.RemainingSize() > 0 {
			zone := dgi.ReadZone()
			if !slices.Contains(zones, zone) {
				zones = append(zones, zone)
			}
		}

		i := c.buildInterest(interestId, parentDoId, zones)

		c.handleAddInterest(i, context)
		c.addInterest(i, context, sender)
	case CLIENT_AGENT_REMOVE_INTEREST:
		interestId, context := dgi.ReadUint16(), dgi.ReadUint32()

		if i, ok := c.interests.Get(interestId); ok {
			c.handleRemoveInterest(i.id, context)
			c.removeInterest(i, context)
		} else {
			c.log.Debugf("Attempted to remove non-existant interest: %d", interestId)
		}
	case CLIENTAGENT_SET_CLIENT_ID:
		c.SetChannel(dgi.ReadChannel())
	case CLIENTAGENT_SEND_DATAGRAM:
		datagram := dgi.ReadDatagram()
		c.client.SendDatagram(*datagram)
	case CLIENTAGENT_OPEN_CHANNEL:
		c.SubscribeChannel(dgi.ReadChannel())
	case CLIENTAGENT_CLOSE_CHANNEL:
		c.UnsubscribeChannel(dgi.ReadChannel())
	case CLIENTAGENT_ADD_POST_REMOVE:
		c.AddPostRemove(*dgi.ReadDatagram())
	case CLIENTAGENT_CLEAR_POST_REMOVES:
		c.ClearPostRemoves()
	case CLIENTAGENT_DECLARE_OBJECT:
		do, dc := dgi.ReadDoid(), dgi.ReadUint16()

		if _, ok := c.declaredObjects.Get(do); ok {
			c.log.Warnf("Received object declaration for previously declared object %d", do)
			return
		}

		cls := core.DC.GetClass(int(dc))
		c.declaredObjects.Set(do, DeclaredObject{
			do: do,
			dc: cls,
		}, false)
	case CLIENTAGENT_UNDECLARE_OBJECT:
		do := dgi.ReadDoid()

		if _, ok := c.declaredObjects.Get(do); !ok {
			c.log.Warnf("Received object de-declaration for previously declared object %d", do)
			return
		}

		c.declaredObjects.Delete(do, false)
	case CLIENT_SET_FIELD_SENDABLE:
		do := dgi.ReadDoid()

		var fields []uint16
		for dgi.RemainingSize() >= Blobsize {
			fields = append(fields, dgi.ReadUint16())
		}
		c.sendableFields.Set(do, fields, false)
	case CLIENTAGENT_ADD_SESSION_OBJECT:
		do := dgi.ReadDoid()
		for _, d := range c.sessionObjects {
			if d == do {
				c.log.Warnf("Received add sesion object with existing ID=%d", do)
			}
		}

		c.log.Debugf("Added session object with ID %d", do)
		c.sessionObjects = append(c.sessionObjects, do)
	case CLIENTAGENT_REMOVE_SESSION_OBJECT:
		do := dgi.ReadDoid()
		for _, d := range c.sessionObjects {
			if d == do {
				break
			}
			c.log.Warnf("Received remove sesion object with non-existant ID=%d", do)
		}

		c.log.Debugf("Removed session object with ID %d", do)
		tempSessionObjectSlice := make([]Doid_t, 0)
		for _, o := range c.sessionObjects {
			if o != do {
				tempSessionObjectSlice = append(tempSessionObjectSlice, o)
			}
		}
		c.sessionObjects = tempSessionObjectSlice
	case CLIENTAGENT_GET_TLVS:
		resp := NewDatagram()
		resp.AddServerHeader(sender, c.channel, CLIENTAGENT_GET_TLVS_RESP)
		resp.AddUint32(dgi.ReadUint32())
		resp.AddDataBlob(c.client.Tlvs())
		c.RouteDatagram(resp)
	case CLIENTAGENT_GET_NETWORK_ADDRESS:
		resp := NewDatagram()
		resp.AddServerHeader(sender, c.channel, CLIENTAGENT_GET_NETWORK_ADDRESS_RESP)
		resp.AddUint32(dgi.ReadUint32())
		resp.AddString(c.client.RemoteIP())
		resp.AddUint16(c.client.RemotePort())
		resp.AddString(c.client.LocalIP())
		resp.AddUint16(c.client.LocalPort())
		c.RouteDatagram(resp)
	case STATESERVER_OBJECT_UPDATE_FIELD:
		do := dgi.ReadDoid()
		dclass := c.lookupObject(do)
		if dclass == nil {
			if c.tryQueuePending(do, dg) {
				return
			}
			c.log.Warnf("Received server-side field update for unknown object %d", do)
			return
		}

		if sender != c.channel {
			field := dgi.ReadUint16()
			dcField := dclass.GetFieldByIndex(int(field))
			if dcField == dc.SwigcptrDCField(0) {
				c.log.Warnf("Received server-side field update for object %s(%d) with unknown field %d", dclass.GetName(), do, field)
				return
			}
			c.handleUpdateField(do, dclass, dcField, dgi)
		}
	// case STATESERVER_OBJECT_UPDATE_FIELD_MULTIPLE:
	// 	do := dgi.ReadDoid()
	// 	if c.lookupObject(do) == nil {
	// 		if c.tryQueuePending(do, dg) {
	// 			return
	// 		}
	// 		c.log.Warnf("Received server-side multi-field update for unknown object %d", do)
	// 	}

	// 	if sender != c.channel {
	// 		fields := dgi.ReadUint16()
	// 		c.handleUpdateField(do, fields, dgi)
	// 	}
	case STATESERVER_OBJECT_DELETE_RAM:
		do := dgi.ReadDoid()
		if c.lookupObject(do) == nil {
			if c.tryQueuePending(do, dg) {
				return
			}
			c.log.Warnf("Received server-side object delete for unknown object %d", do)
			return
		}

		c.log.Debugf("Deleting server-side object %d", do)
		tempSessionObjectSlice := make([]Doid_t, 0)
		for _, so := range c.sessionObjects {
			if so != do {
				tempSessionObjectSlice = append(tempSessionObjectSlice, so)
			} else {
				c.sendDisconnect(CLIENT_DISCONNECT_SESSION_OBJECT_DELETED,
					fmt.Sprintf("The session object with id %d has been unexpectedly deleted", do), false)
			}
		}

		c.sessionObjects = tempSessionObjectSlice

		tempSeenObjectSlice := make([]Doid_t, 0)
		for _, so := range c.seenObjects {
			if so != do {
				tempSeenObjectSlice = append(tempSeenObjectSlice, so)
			} else {
				c.handleRemoveObject(do, false)
			}
		}
		c.seenObjects = tempSeenObjectSlice

		if _, ok := c.ownedObjects.Get(do); ok {
			c.handleRemoveOwnership(do)
			c.ownedObjects.Delete(do, false)
		}

		c.addHistoricalObject(do)
		c.visibleObjects.Delete(do, false)
	case STATESERVER_OBJECT_ENTER_OWNER_RECV:
		do, parent, zone, dc := dgi.ReadDoid(), dgi.ReadDoid(), dgi.ReadZone(), dgi.ReadUint16()

		if _, ok := c.ownedObjects.Get(do); !ok {
			cls := core.DC.GetClass(int(dc))
			c.ownedObjects.Set(do, OwnedObject{
				DeclaredObject: DeclaredObject{
					do: do,
					dc: cls,
				},
				parent: parent,
				zone:   zone,
			}, false)
		}

		c.handleAddOwnership(do, parent, zone, dc, dgi)
	case STATESERVER_OBJECT_ENTER_LOCATION_WITH_REQUIRED:
		fallthrough
	case STATESERVER_OBJECT_ENTER_LOCATION_WITH_REQUIRED_OTHER:
		offset := dgi.Tell()
		do, parent, zone := dgi.ReadDoid(), dgi.ReadDoid(), dgi.ReadZone()

		pendingInterestIterator := c.pendingInterests.Iterator()
		for pendingInterestIterator.Next() {
			id := pendingInterestIterator.Key().Interface().(uint32)
			iop := pendingInterestIterator.Value().Interface().(*InterestOperation)
			if iop.parent == parent && iop.hasZone(zone) {
				iop.pendingQueue = append(iop.pendingQueue, dg)
				c.pendingObjects.Set(do, id, false)
				c.pendingInterests.RUnlock()
				return
			}
		}
		c.pendingInterests.RUnlock()

		interestIterator := c.interests.Iterator()
		for interestIterator.Next() {
			iop := interestIterator.Value().Interface().(Interest)
			if iop.parent == parent && iop.hasZone(zone) {
				dgi.Seek(offset)
				c.handleObjectEntrance(dgi, msgType == STATESERVER_OBJECT_ENTER_LOCATION_WITH_REQUIRED_OTHER)
			}
		}
		c.interests.RUnlock()
	case STATESERVER_OBJECT_GET_ZONE_COUNT_RESP:
		fallthrough
	case STATESERVER_OBJECT_GET_ZONES_COUNT_RESP:
		context := dgi.ReadUint32()
		if iop, ok := c.pendingInterests.Get(context); ok {
			total := dgi.ReadUint32()
			iop.setExpected(int(total))
		} else {
			c.log.Warnf("Got zone count for unknown interest: %d", context)
		}
	case STATESERVER_OBJECT_ENTER_INTEREST_WITH_REQUIRED:
		fallthrough
	case STATESERVER_OBJECT_ENTER_INTEREST_WITH_REQUIRED_OTHER:
		context := dgi.ReadUint32()
		// Skip do, parent and zone
		dgi.Skip(Dgsize * 3)
		dc := dgi.ReadUint16()
		if iop, ok := c.pendingInterests.Get(context); ok {
			if !iop.finished {
				iop.generateQueue[dc] = append(iop.generateQueue[dc], dg)
				iop.totalCount++
				if iop.ready() {
					go iop.finish()
				}
			} else {
				// Message arrived late, announce generate.
				c.handleObjectEntrance(dgi, msgType == STATESERVER_OBJECT_ENTER_INTEREST_WITH_REQUIRED_OTHER)
			}
		}
	case STATESERVER_OBJECT_CHANGE_ZONE:
		do := dgi.ReadDoid()
		if c.lookupObject(do) == nil {
			if c.tryQueuePending(do, dg) {
				return
			}
			c.log.Warnf("Got ChangeZone from unknown object:%d", do)
			return
		}

		if slices.Contains(c.sessionObjects, do) {
			return
		}

		parent := dgi.ReadDoid()
		zone := dgi.ReadZone()

		if obj, ok := c.visibleObjects.Get(do); ok {
			if len(c.lookupInterests(parent, zone)) > 0 {
				// We have interest in new location; update.
				obj.parent = parent
				obj.zone = zone

				c.visibleObjects.Set(do, obj, false)

				// Tell the client to update.
				c.handleObjectLocation(do, parent, zone)
			} else {
				// Not interested, delete.
				tempSeenObjectSlice := make([]Doid_t, 0)
				for _, so := range c.seenObjects {
					if so != do {
						tempSeenObjectSlice = append(tempSeenObjectSlice, so)
					} else {
						c.handleRemoveObject(do, false)
					}
				}
				c.seenObjects = tempSeenObjectSlice

				if _, ok := c.ownedObjects.Get(do); ok {
					c.handleRemoveOwnership(do)
					c.ownedObjects.Delete(do, false)
				}

				c.addHistoricalObject(do)
				c.visibleObjects.Delete(do, false)
			}
		}
	case STATESERVER_OBJECT_CHANGE_OWNER_RECV:
		c.log.Warn("TODO: STATESERVER_OBJECT_CHANGE_OWNER_RECV")
	case DBSERVER_CREATE_STORED_OBJECT_RESP:
		context, code, doId := dgi.ReadUint32(), dgi.ReadUint8(), dgi.ReadDoid()
		c.handleCreateDatabaseResp(context, code, doId)
	case DBSERVER_GET_STORED_VALUES_RESP:
		c.handleGetStoredValuesResp(dgi)
	case STATESERVER_OBJECT_QUERY_FIELDS_RESP:
		c.handleQueryFieldsResp(dgi)
	default:
		if luaFunc, ok := c.ca.L.GetGlobal("handleDatagram").(*lua.LFunction); ok {
			c.ca.CallLuaFunction(luaFunc, c,
				// Arguments:
				NewLuaClient(c.ca.L, c),
				lua.LNumber(msgType),
				NewLuaDatagramIteratorFromExisting(c.ca.L, dgi))
		} else {
			c.log.Errorf("Received unknown server msgtype %d", msgType)
		}
	}
}

type InterestOperation struct {
	hasTotal   bool
	totalCount int
	finished   bool
	total      int

	timeout      *time.Ticker
	timedOut     bool
	finishedChan chan bool

	client         *Client
	interestId     uint16
	clientContext  uint32
	requestContext uint32
	parent         Doid_t

	zones   []Zone_t
	callers []Channel_t

	// generateQueue []Datagram
	generateQueue map[uint16][]Datagram
	pendingQueue  []Datagram
}

func NewInterestOperation(client *Client, timeout int, interestId uint16,
	clientContext uint32, requestContext uint32, parent Doid_t, zones []Zone_t, caller Channel_t) *InterestOperation {
	iop := &InterestOperation{
		client:         client,
		interestId:     interestId,
		clientContext:  clientContext,
		requestContext: requestContext,
		parent:         parent,
		zones:          zones,
		timeout:        time.NewTicker(time.Duration(timeout) * time.Second),
		finishedChan:   make(chan bool),
		generateQueue:  map[uint16][]Datagram{},
		pendingQueue:   []Datagram{},
		callers:        []Channel_t{caller},
	}

	// Timeout
	go func() {
		select {
		case <-iop.timeout.C:
			if !iop.finished {
				client.log.Warnf("Interest operation timed out; Got %d generates and was expecting %d. Forcing finish.", len(iop.generateQueue), iop.total)
				iop.timedOut = true
				iop.finish()
			}
		case <-iop.finishedChan:
			return
		}
	}()

	return iop
}

func (i *InterestOperation) hasZone(zone Zone_t) bool {
	for _, z := range i.zones {
		if z == zone {
			return true
		}
	}
	return false
}

func (i *InterestOperation) setExpected(total int) {
	if !i.hasTotal {
		i.total = total
		i.hasTotal = true
		if i.ready() {
			go i.finish()
		}
	}
}

func (i *InterestOperation) ready() bool {
	return i.hasTotal && i.totalCount >= i.total
}

func (i *InterestOperation) finish() {
	// We need to acquire our client's lock because we can't risk
	//  concurrent writes to pendingInterests
	i.client.Lock()
	defer i.client.Unlock()

	i.finished = true
	i.timeout.Stop()
	if !i.timedOut {
		go func() {
			i.finishedChan <- true
		}()
	}

	// Sort generates by class number
	keys := make([]int, len(i.generateQueue))
	for k := range i.generateQueue {
		keys = append(keys, int(k))
	}
	sort.Ints(keys)

	for _, k := range keys {
		for _, generate := range i.generateQueue[uint16(k)] {
			dgi := NewDatagramIterator(&generate)
			dgi.SeekPayload()
			dgi.Skip(Chansize) // Skip sender

			msgType := dgi.ReadUint16()
			other := msgType == STATESERVER_OBJECT_ENTER_INTEREST_WITH_REQUIRED_OTHER

			dgi.Skip(Dgsize) // Skip request context
			i.client.handleObjectEntrance(dgi, other)
		}
	}
	i.generateQueue = nil

	// Send out interest done messages
	i.client.notifyInterestDone(i.interestId, i.callers)
	i.client.handleInterestDone(i.interestId, i.clientContext)

	// Delete the IOP
	i.client.pendingInterests.Delete(i.requestContext, false)
	go func() {
		for _, dg := range i.pendingQueue {
			dgi := NewDatagramIterator(&dg)
			dgi.SeekPayload()
			i.client.HandleDatagram(dg, dgi)
		}
		i.pendingQueue = nil
	}()
}

type InterestPermission int

const (
	INTERESTS_ENABLED InterestPermission = iota
	INTERESTS_VISIBLE
	INTERESTS_DISABLED
)

// N.B. The purpose of this file is to separate implementations of ReceiveDatagram
//  and HandleDatagram and their associated functions-- normally, this would be done
//  by having two separate classes Client and AstronClient, but Go has zero support
//  for the virtual functions required to achieve this level of organization. Thus, two
//  distinct files still exist, but implement functions to the same class.

func (c *Client) init(config core.Role, conn gonet.Conn) {
	c.allowedInterests = INTERESTS_ENABLED
	if config.Client.Heartbeat_Timeout != 0 {
		c.heartbeat = time.NewTimer(time.Duration(config.Client.Heartbeat_Timeout) * time.Second)
		c.stopHeartbeat = make(chan bool, 1)
		go c.startHeartbeat()
	}

	socket := net.NewSocketTransport(conn,
		time.Duration(config.Client.Keepalive)*time.Second, config.Client.Write_Buffer_Size)
	c.client = net.NewClient(socket, c, time.Duration(5)*time.Second)

	event := eventlogger.NewLoggedEvent("client-connected", "Client", strconv.FormatUint(uint64(c.allocatedChannel), 10),
		fmt.Sprintf("%s|%s", conn.RemoteAddr().String(), conn.LocalAddr().String()),
	)
	event.Send()
}

func (c *Client) startHeartbeat() {
	// Even though it is unnecessary, the heartbeat is contained in a select statement so that
	//  the timer can be replaced each time a heartbeat is sent.
	select {
	case <-c.heartbeat.C:
		// Time to disconnect!
		c.lock.Lock()
		c.sendDisconnect(CLIENT_DISCONNECT_NO_HEARTBEAT, "Server timed out while waiting for heartbeat.", false)
		c.lock.Unlock()
	case <-c.stopHeartbeat:
		return
	}
}

func (c *Client) Terminate(err error) {
	termSwapped := c.terminationBegun.CompareAndSwap(false, true)
	if !termSwapped {
		// If we didn't swap the boolean, another goroutine has already begun to terminate this client. Bail now.
		return
	}

	// Lock if the client has not been fully initialized first.
	c.terminationLock.Lock()

	if !c.cleanDisconnect && err != nil {
		event := eventlogger.NewLoggedEvent("client-lost", "Client", strconv.FormatUint(uint64(c.allocatedChannel), 10),
			fmt.Sprintf("%s|%s|%s", c.conn.RemoteAddr().String(), c.conn.LocalAddr().String(), err.Error()),
		)
		event.Send()
	}

	// (Sending to these channels from ReceiveDatagram or startHeartbeat
	// will deadlock, starting a separate goroutine fixes this.)
	go func() {
		// Stop the queue goroutine
		c.stopChan <- true
		if c.config.Client.Heartbeat_Timeout != 0 {
			c.heartbeat.Stop()
			// Stop the heartbeat goroutine
			c.stopHeartbeat <- true
		}
	}()
	go c.annihilate()

	c.terminationLock.Unlock()
	c.client.Close(true)
}

func (c *Client) ReceiveDatagram(dg Datagram) {
	c.queueLock.Lock()
	c.queue = append(c.queue, dg)
	c.queueLock.Unlock()

	select {
	case c.shouldProcess <- true:
	default:
	}
}

func (c *Client) getDatagramFromQueue() Datagram {
	c.queueLock.Lock()
	defer c.queueLock.Unlock()

	dg := c.queue[0]
	c.queue[0] = Datagram{}
	c.queue = c.queue[1:]
	if len(c.queue) == 0 {
		// Recreate the queue slice. This prevents the capacity from growing indefinitely and allows old entries to drop off as soon as possible from the backing array.
		c.queue = make([]Datagram, 0)
	}
	return dg
}

func (c *Client) queueLoop() {
	for {
		select {
		case <-c.shouldProcess:
			for len(c.queue) > 0 {
				dg := c.getDatagramFromQueue()

				c.lock.Lock()
				dgi := NewDatagramIterator(&dg)
				finish := make(chan bool)
				go func() {
					defer func() {
						if r := recover(); r != nil {
							if _, ok := r.(DatagramIteratorEOF); ok {
								c.sendDisconnect(CLIENT_DISCONNECT_TRUNCATED_DATAGRAM, "Datagram unexpectedly ended while iterating.", false)
							}
							finish <- true
						}
					}()

					// Pass the datagram over to Lua to handle:
					c.ca.CallLuaFunction(c.ca.receiveDatagramFunc, c,
						// Arguments:
						NewLuaClient(c.ca.L, c),
						NewLuaDatagramIteratorFromExisting(c.ca.L, dgi))
					finish <- true
				}()

				<-finish
				c.lock.Unlock()
			}
		case <-c.stopChan:
			return
		case <-core.StopChan:
			return
		}
	}
}

func (c *Client) handleHeartbeat() {
	if c.config.Client.Heartbeat_Timeout != 0 {
		if c.heartbeat.Stop() {
			c.heartbeat.Reset(time.Duration(c.config.Client.Heartbeat_Timeout) * time.Second)
		}
	}
}

func (c *Client) createDatabaseObject(objectType uint16, packedValues map[string]dc.Vector, callback func(doId Doid_t)) {
	context := c.createContextMap.Set(c.context.Add(1), callback, true)
	defer c.createContextMap.Unlock()

	dg := NewDatagram()
	dg.AddServerHeader(c.ca.database, c.channel, DBSERVER_CREATE_STORED_OBJECT)
	dg.AddUint32(context)
	dg.AddString("") // Unknown
	dg.AddUint16(objectType)
	dg.AddUint16(uint16(len(packedValues)))

	for name, value := range packedValues {
		dg.AddString(name)
		dg.AddUint16(uint16(value.Size()))
		dg.AddVector(value)
		dc.DeleteVector(value)
	}
	c.RouteDatagram(dg)
}

func (c *Client) handleCreateDatabaseResp(context uint32, code uint8, doId Doid_t) {
	callback, ok := c.createContextMap.Get(context)

	if !ok {
		c.log.Warnf("Got CreateDatabaseRsp with missing context %d", context)
		return
	}

	if code > 0 {
		c.log.Warnf("CreateDatabaseResp returned an error!")
	}

	callback(doId)

	c.createContextMap.Delete(context, false)
}

func (c *Client) getDatabaseValues(doId Doid_t, fields []string, callback func(doId Doid_t, dgi *DatagramIterator)) {
	context := c.getContextMap.Set(c.context.Add(1), callback, true)
	defer c.getContextMap.Unlock()

	dg := NewDatagram()
	dg.AddServerHeader(c.ca.database, c.channel, DBSERVER_GET_STORED_VALUES)
	dg.AddUint32(context)
	dg.AddDoid(doId)
	dg.AddUint16(uint16(len(fields)))
	for _, name := range fields {
		dg.AddString(name)
	}
	c.RouteDatagram(dg)
}

func (c *Client) handleGetStoredValuesResp(dgi *DatagramIterator) {
	context := dgi.ReadUint32()
	doId := dgi.ReadDoid()

	callback, ok := c.getContextMap.Get(context)

	if !ok {
		c.log.Warnf("Got GetStoredResp with missing context %d", context)
		return
	}

	callback(doId, dgi)
	c.getContextMap.Delete(context, false)
}

func (c *Client) handleQueryFieldsResp(dgi *DatagramIterator) {
	dgi.ReadDoid() // doId, unused

	context := dgi.ReadUint32()
	callback, ok := c.queryFieldsContextMap.Get(context)

	if !ok {
		c.log.Warnf("Got QueryFieldsResp with missing context %d", context)
		return
	}

	callback(dgi)
	c.queryFieldsContextMap.Delete(context, false)
}

func (c *Client) setDatabaseValues(doId Doid_t, packedValues map[string]dc.Vector) {
	dg := NewDatagram()
	dg.AddServerHeader(c.ca.database, c.channel, DBSERVER_SET_STORED_VALUES)
	dg.AddDoid(doId)
	dg.AddUint16(uint16(len(packedValues)))

	for name, value := range packedValues {
		dg.AddString(name)
		dg.AddUint16(uint16(value.Size()))
		dg.AddVector(value)
		dc.DeleteVector(value)
	}

	c.RouteDatagram(dg)
}

func (c *Client) handleAddOwnership(do Doid_t, parent Doid_t, zone Zone_t, dc uint16, dgi *DatagramIterator) {
	lFunc := c.ca.L.GetGlobal("handleAddOwnership")
	if lFunc.Type() == lua.LTFunction {
		// Call the Lua function instead of sending the
		// built-in response.
		c.ca.CallLuaFunction(lFunc, c, NewLuaClient(c.ca.L, c), lua.LNumber(do), lua.LNumber(parent), lua.LNumber(zone), lua.LNumber(dc), NewLuaDatagramIteratorFromExisting(c.ca.L, dgi))
		return
	}

	resp := NewDatagram()
	resp.AddUint16(uint16(CLIENT_CREATE_OBJECT_REQUIRED_OTHER_OWNER))
	resp.AddLocation(parent, zone)
	resp.AddUint16(dc)
	resp.AddDoid(do)
	resp.AddData(dgi.ReadRemainder())
	c.client.SendDatagram(resp)
}

func (c *Client) handleRemoveOwnership(do Doid_t) {
	resp := NewDatagram()
	resp.AddUint16(CLIENT_OBJECT_DISABLE_OWNER)
	resp.AddDoid(do)
	c.client.SendDatagram(resp)
}

func (c *Client) isFieldSendable(do Doid_t, field dc.DCField) bool {
	if _, ok := c.ownedObjects.Get(do); ok && field.IsOwnsend() {
		return true
	} else if fields, ok := c.sendableFields.Get(do); ok {
		for _, v := range fields {
			if v == uint16(field.GetNumber()) {
				return true
			}
		}
	}

	return field.IsClsend()
}

func (c *Client) handleClientUpdateField(do Doid_t, field uint16, dgi *DatagramIterator) {
	dclass := c.lookupObject(do)
	if dclass == nil {
		if c.historicalObject(do) {
			// Skip the data to prevent the excess data ejection.
			dgi.Skip(dgi.RemainingSize())
			return
		}
		c.log.Debugf("Attempted to send field update to unknown object: %d", do)
		// c.sendDisconnect(CLIENT_DISCONNECT_MISSING_OBJECT, fmt.Sprintf("Attempted to send field update to unknown object: %d", do), true)
		// Skip the data to prevent the excess data ejection.
		dgi.Skip(dgi.RemainingSize())
		return
	}

	dcField := dclass.GetFieldByIndex(int(field))
	if dcField == dc.SwigcptrDCField(0) {
		c.sendDisconnect(CLIENT_DISCONNECT_FORBIDDEN_FIELD, fmt.Sprintf("Attempted to send field update to %s(%d) with unknown field: %d", dclass.GetName(), do, field), true)
		// Skip the data to prevent the excess data ejection.
		dgi.Skip(dgi.RemainingSize())
		return
	}

	if !c.isFieldSendable(do, dcField) {
		c.sendDisconnect(CLIENT_DISCONNECT_TRUNCATED_DATAGRAM, fmt.Sprintf("Attempted to send unsendable field %s", dcField.GetName()), true)
		// Skip the data to prevent the excess data ejection.
		dgi.Skip(dgi.RemainingSize())
		return
	}

	DCLock.Lock()
	defer DCLock.Unlock()

	packedData := dgi.ReadRemainderAsVector()
	defer dc.DeleteVector(packedData)

	if !dcField.ValidateRanges(packedData) {
		c.sendDisconnect(CLIENT_DISCONNECT_TRUNCATED_DATAGRAM, fmt.Sprintf("Got truncated update for field %s\n%s\n%s", dcField.GetName(), DumpVector(packedData), dgi), true)
		return
	}

	c.log.Debugf("Got client \"%s\" update for object %s(%d): %s", dcField.GetName(), dclass.GetName(), do, dcField.FormatData(packedData))

	lFunc := c.ca.L.GetGlobal(fmt.Sprintf("handleClient%s_%s", dclass.GetName(), dcField.GetName()))
	if lFunc.Type() == lua.LTFunction {
		// Call the Lua function instead of sending the
		// built-in response.
		unpacker := dc.NewDCPacker()
		defer dc.DeleteDCPacker(unpacker)

		unpacker.SetUnpackData(packedData)
		unpacker.BeginUnpack(dcField)
		lValue := core.UnpackDataToLuaValue(unpacker, c.ca.L)
		if !unpacker.EndUnpack() {
			c.log.Warnf("EndUnpack returned false on handleClientUpdateField somehow...\n%s", DumpUnpacker(unpacker))
			return
		}

		c.ca.CallLuaFunction(lFunc, c, NewLuaClient(c.ca.L, c), lua.LNumber(do), lua.LNumber(field), lValue)
		return
	}

	// Send the message over to the object.
	dg := NewDatagram()
	dg.AddServerHeader(Channel_t(do), c.channel, STATESERVER_OBJECT_UPDATE_FIELD)
	dg.AddDoid(do)
	dg.AddUint16(field)
	dg.AddVector(packedData)

	c.RouteDatagram(dg)
}

func (c *Client) handleUpdateField(do Doid_t, dclass dc.DCClass, dcField dc.DCField, dgi *DatagramIterator) {
	lFunc := c.ca.L.GetGlobal(fmt.Sprintf("handle%s_%s", dclass.GetName(), dcField.GetName()))
	if lFunc.Type() == lua.LTFunction {
		// Call the Lua function instead of sending the
		// built-in response.

		DCLock.Lock()
		defer DCLock.Unlock()

		packedData := dgi.ReadRemainderAsVector()
		defer dc.DeleteVector(packedData)

		unpacker := dc.NewDCPacker()
		defer dc.DeleteDCPacker(unpacker)

		unpacker.SetUnpackData(packedData)
		unpacker.BeginUnpack(dcField)
		lValue := core.UnpackDataToLuaValue(unpacker, c.ca.L)
		if !unpacker.EndUnpack() {
			c.log.Warnf("EndUnpack returned false on handleUpdateField somehow...\n%s", DumpUnpacker(unpacker))
			return
		}

		c.ca.CallLuaFunction(lFunc, c, NewLuaClient(c.ca.L, c), lua.LNumber(do), lua.LNumber(dcField.GetNumber()), lValue)
		return
	}

	resp := NewDatagram()
	resp.AddUint16(CLIENT_OBJECT_UPDATE_FIELD)
	resp.AddDoid(do)
	resp.AddUint16(uint16(dcField.GetNumber()))
	resp.AddData(dgi.ReadRemainder())
	c.client.SendDatagram(resp)
}

func (c *Client) handleRemoveInterest(id uint16, context uint32) {
	resp := NewDatagram()
	resp.AddUint16(CLIENT_REMOVE_INTEREST)
	resp.AddUint16(id)
	resp.AddUint32(context)
	c.client.SendDatagram(resp)
}

func (c *Client) handleAddInterest(i Interest, context uint32) {
	resp := NewDatagram()
	resp.AddUint16(CLIENT_ADD_INTEREST)
	resp.AddUint32(context)
	resp.AddUint16(i.id)
	resp.AddDoid(i.parent)
	for _, zone := range i.zones {
		resp.AddZone(zone)
	}
	c.client.SendDatagram(resp)
}

func (c *Client) handleRemoveObject(do Doid_t, deleted bool) {
	lFunc := c.ca.L.GetGlobal("handleRemoveObject")
	if lFunc.Type() == lua.LTFunction {
		// Call the Lua function instead of sending the
		// built-in response.
		c.ca.CallLuaFunction(lFunc, c, NewLuaClient(c.ca.L, c), lua.LNumber(do))
		return
	}
	resp := NewDatagram()

	msgType := CLIENT_OBJECT_DISABLE

	if deleted {
		msgType = CLIENT_OBJECT_DELETE
	}

	resp.AddUint16(uint16(msgType))
	resp.AddDoid(do)
	c.client.SendDatagram(resp)
}

func (c *Client) handleObjectLocation(do Doid_t, parent Doid_t, zone Zone_t) {
	lFunc := c.ca.L.GetGlobal("handleObjectLocation")
	if lFunc.Type() == lua.LTFunction {
		// Call the Lua function instead of sending the
		// built-in response.
		c.ca.CallLuaFunction(lFunc, c, NewLuaClient(c.ca.L, c), lua.LNumber(do), lua.LNumber(parent), lua.LNumber(zone))
		return
	}
	dg := NewDatagram()
	dg.AddUint16(CLIENT_OBJECT_LOCATION)
	dg.AddDoid(do)
	dg.AddDoid(parent)
	dg.AddZone(zone)
	c.client.SendDatagram(dg)
}

func (c *Client) handleAddObject(do Doid_t, parent Doid_t, zone Zone_t, dc uint16, dgi *DatagramIterator, other bool) {
	c.log.Debugf("handleAddObject: %s(%d)", core.DC.GetClass(int(dc)).GetName(), do)

	msgType := CLIENT_CREATE_OBJECT_REQUIRED
	if other {
		msgType = CLIENT_CREATE_OBJECT_REQUIRED_OTHER
	}

	resp := NewDatagram()
	resp.AddUint16(uint16(msgType))

	if (!c.config.Client.Legacy_Handle_Object) {
		resp.AddLocation(parent, zone)
	}

	resp.AddUint16(dc)
	resp.AddDoid(do)
	resp.AddData(dgi.ReadRemainder())
	c.client.SendDatagram(resp)
}

func (c *Client) handleInterestDone(interestId uint16, context uint32) {
	lFunc := c.ca.L.GetGlobal("handleInterestDone")
	if lFunc.Type() == lua.LTFunction {
		// Call the Lua function instead of sending the
		// built-in response.
		c.ca.CallLuaFunction(lFunc, c, NewLuaClient(c.ca.L, c), lua.LNumber(interestId), lua.LNumber(context))
		return
	}
	if context > 0 {
		resp := NewDatagram()
		resp.AddUint16(CLIENT_DONE_INTEREST_RESP)
		resp.AddUint16(interestId)
		resp.AddUint32(context)
		c.client.SendDatagram(resp)
	}
}

func (c *Client) SetChannel(channel Channel_t) {
	if c.channel == channel {
		return
	}
	if c.channel != c.allocatedChannel {
		c.UnsubscribeChannel(c.channel)
	}
	c.channel = channel
	c.SubscribeChannel(channel)
}
