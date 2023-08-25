package clientagent

import (
	"errors"
	"otpgo/core"
	// "otpgo/eventlogger"
	"fmt"
	gonet "net"
	"otpgo/messagedirector"
	"otpgo/net"
	. "otpgo/util"
	"sync"
	"time"

	dc "github.com/LittleToonCat/dcparser-go"
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

	context          uint32
	createContextMap map[uint32]func(doId Doid_t)
	getContextMap    map[uint32]func(doId Doid_t, dgi *DatagramIterator)

	queue         []Datagram
	queueLock     sync.Mutex
	shouldProcess chan bool
	stopChan      chan bool

	seenObjects       []Doid_t
	sessionObjects    []Doid_t
	historicalObjects []Doid_t

	visibleObjects   map[Doid_t]VisibleObject
	declaredObjects  map[Doid_t]DeclaredObject
	ownedObjects     map[Doid_t]OwnedObject
	pendingObjects   map[Doid_t]uint32
	interests        map[uint16]Interest
	pendingInterests map[uint32]*InterestOperation
	sendableFields   map[Doid_t][]uint16

	conn   gonet.Conn
	client *net.Client
	lock   sync.Mutex

	cleanDisconnect  bool
	allowedInterests InterestPermission
	heartbeat        *time.Ticker
	stopHeartbeat    chan bool
}

func NewClient(config core.Role, ca *ClientAgent, conn gonet.Conn) *Client {
	c := &Client{
		config: config,
		ca: ca,
		queue: []Datagram{},
		shouldProcess: make(chan bool),
		stopChan: make(chan bool),
		createContextMap: map[uint32]func(doId Doid_t){},
		getContextMap: map[uint32]func(doId Doid_t, dgi *DatagramIterator){},
		authenticated: false,
		visibleObjects: map[Doid_t]VisibleObject{},
		declaredObjects: map[Doid_t]DeclaredObject{},
		ownedObjects: map[Doid_t]OwnedObject{},
		pendingObjects: map[Doid_t]uint32{},
		interests: map[uint16]Interest{},
		pendingInterests: map[uint32]*InterestOperation{},
		sendableFields: map[Doid_t][]uint16{},
	}
	c.init(config, conn)
	c.Init(c)

	c.allocatedChannel = ca.Allocate()
	if c.allocatedChannel == 0 {
		c.sendDisconnect(CLIENT_DISCONNECT_GENERIC, "Client capacity reached", false)
		return nil
	}
	c.channel = c.allocatedChannel

	c.log = log.WithFields(log.Fields{
		"name": fmt.Sprintf("Client (%d)", c.channel),
	})

	c.SubscribeChannel(c.channel)
	c.SubscribeChannel(BCHAN_CLIENTS)

	go c.queueLoop()

	return c
}

func (c *Client) sendDisconnect(reason uint16, message string, security bool) {
	// TODO: Implement security loglevel
	// var eventType string
	if security {
		c.log.Errorf("[SECURITY] Ejecting client (%s): %s", reason, message)
		// eventType = "client-ejected-security"
	} else {
		c.log.Errorf("Ejecting client (%s): %s", reason, message)
		// eventType = "client-ejected"
	}

	// event := eventlogger.NewLoggedEvent(eventType, "")
	// event.Add("reason_code", string(reason))
	// event.Add("reason_msg", error)
	// c.logEvent(event)

	if c.client.Connected() {
		resp := NewDatagram()
		resp.AddUint16(CLIENT_GO_GET_LOST)
		resp.AddUint16(reason)
		resp.AddString(message)
		c.client.SendDatagram(resp)

		c.cleanDisconnect = true
		c.Terminate(errors.New(message))
	}
}

// func (c *Client) logEvent(event eventlogger.LoggedEvent) {
// 	event.Add("sender", fmt.Sprintf("Client: %d", c.channel))
// 	event.Send()
// }

func (c *Client) annihilate() {
	c.Lock()
	defer c.Unlock()

	if c.IsTerminated() {
		return
	}

	c.ca.Tracker.free(c.channel)

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

	for _, int := range c.pendingInterests {
		go int.finish()
	}

	c.Cleanup()
}

func (c *Client) lookupInterests(parent Doid_t, zone Zone_t) []Interest {
	var interests []Interest
	for _, int := range c.interests {
		if parent == int.parent && int.hasZone(zone) {
			interests = append(interests, int)
		}
	}
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
	var zones []Zone_t

	for _, zone := range i.zones {
		if len(c.lookupInterests(i.parent, zone)) == 0 {
			zones = append(zones, zone)
		}
	}

	if prevInt, ok := c.interests[i.id]; ok {
		// This interest already exists, so it is being altered
		var killedZones []Zone_t

		for _, zone := range prevInt.zones {
			if len(c.lookupInterests(i.parent, zone)) > 1 {
				// Another interest has this zone, so ignore it
				continue
			}

			if i.parent != prevInt.parent || i.hasZone(zone) {
				killedZones = append(killedZones, zone)
			}
		}

		c.closeZones(prevInt.parent, killedZones)
	}
	c.interests[i.id] = i

	if len(zones) == 0 {
		// We aren't requesting any new zones, so let the client know we finished
		c.notifyInterestDone(i.id, []Channel_t{caller})
		c.handleInterestDone(i.id, context)
		return
	}

	// Build a new IOP otherwise
	c.context++
	iop := NewInterestOperation(c, c.config.Tuning.Interest_Timeout, i.id,
		context, c.context, i.parent, zones, caller)
	c.pendingInterests[c.context] = iop

	resp := NewDatagram()
	resp.AddServerHeader(Channel_t(i.parent), c.channel, STATESERVER_OBJECT_GET_ZONES_OBJECTS)
	resp.AddUint32(c.context)
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

	delete(c.interests, i.id)
}

func (c *Client) closeZones(parent Doid_t, zones []Zone_t) {
	var toRemove []Doid_t

	for _, obj := range c.visibleObjects {
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
						return
					}
				}

				c.handleRemoveObject(obj.do)
				for i, o := range c.seenObjects {
					if o == obj.do {
						c.seenObjects = append(c.seenObjects[:i], c.seenObjects[i+1:]...)
					}
				}
				toRemove = append(toRemove, obj.do)
			}
		}
	}

	for _, do := range toRemove {
		delete(c.visibleObjects, do)
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

func (c *Client) lookupObject(do Doid_t) dc.DCClass {
	// Search UberDOGs
	for i := range core.Uberdogs {
		if core.Uberdogs[i].Id == do {
			return core.Uberdogs[i].Class
		}
	}

	// Check the object cache
	if obj, ok := c.ownedObjects[do]; ok {
		return obj.dc
	}

	for i := range c.seenObjects {
		if c.seenObjects[i] == do {
			if obj, ok := c.visibleObjects[do]; ok {
				return obj.dc
			}
		}
	}

	// Check declared objects
	if obj, ok := c.declaredObjects[do]; ok {
		return obj.dc
	}

	// We don't know :(
	return nil
}

func (c *Client) tryQueuePending(do Doid_t, dg Datagram) bool {
	if context, ok := c.pendingObjects[do]; ok {
		if iop, ok := c.pendingInterests[context]; ok {
			iop.pendingQueue = append(iop.pendingQueue, dg)
			return true
		}
	}
	return false
}

func (c *Client) handleObjectEntrance(dgi *DatagramIterator, other bool) {
	do, parent, zone, dc := dgi.ReadDoid(), dgi.ReadDoid(), dgi.ReadZone(), dgi.ReadUint16()

	delete(c.pendingObjects, do)

	for i := range c.seenObjects {
		if c.seenObjects[i] == do {
			return
		}
	}

	if _, ok := c.ownedObjects[do]; ok {
		for i := range c.sessionObjects {
			if c.sessionObjects[i] == do {
				return
			}
		}
	}

	if _, ok := c.visibleObjects[do]; !ok {
		cls := core.DC.Get_class(int(dc))
		c.visibleObjects[do] = VisibleObject{
			DeclaredObject: DeclaredObject{
				do: do,
				dc: cls,
			},
			parent: parent,
			zone:   zone,
		}
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
		c.Terminate(errors.New("Dropped"))
		c.lock.Unlock()
	case CLIENTAGENT_SET_STATE:
		c.state = ClientState(dgi.ReadUint16())
	case CLIENTAGENT_ADD_INTEREST:
		// c.context++
		// int := c.buildInterest(dgi, false)
		// c.handleAddInterest(int, c.context)
		// c.addInterest(int, c.context, sender)
	case CLIENTAGENT_ADD_INTEREST_MULTIPLE:
		// c.context++
		// int := c.buildInterest(dgi, true)
		// c.handleAddInterest(int, c.context)
		// c.addInterest(int, c.context, sender)
	case CLIENTAGENT_REMOVE_INTEREST:
		// c.context++
		// id := dgi.ReadUint16()
		// int := c.interests[id]
		// c.handleRemoveInterest(id, c.context)
		// c.removeInterest(int, c.context, sender)
	case CLIENTAGENT_SET_CLIENT_ID:
		c.SetChannel(dgi.ReadChannel())
	case CLIENTAGENT_SEND_DATAGRAM:
		c.client.SendDatagram(dg)
	case CLIENTAGENT_OPEN_CHANNEL:
		c.SubscribeChannel(dgi.ReadChannel())
	case CLIENTAGENT_CLOSE_CHANNEL:
		c.UnsubscribeChannel(dgi.ReadChannel())
	case CLIENTAGENT_ADD_POST_REMOVE:
		c.AddPostRemove(c.allocatedChannel, *dgi.ReadDatagram())
	case CLIENTAGENT_CLEAR_POST_REMOVES:
		c.ClearPostRemoves(c.allocatedChannel)
	case CLIENTAGENT_DECLARE_OBJECT:
		do, dc := dgi.ReadDoid(), dgi.ReadUint16()

		if _, ok := c.declaredObjects[do]; ok {
			c.log.Warnf("Received object declaration for previously declared object %d", do)
			return
		}

		cls := core.DC.Get_class(int(dc))
		c.declaredObjects[do] = DeclaredObject{
			do: do,
			dc: cls,
		}
	case CLIENTAGENT_UNDECLARE_OBJECT:
		do := dgi.ReadDoid()

		if _, ok := c.declaredObjects[do]; ok {
			c.log.Warnf("Received object de-declaration for previously declared object %d", do)
			return
		}

		delete(c.declaredObjects, do)
	case CLIENTAGENT_SET_FIELDS_SENDABLE:
		do, count := dgi.ReadDoid(), dgi.ReadUint16()

		var fields []uint16
		for count != 0 {
			fields = append(fields, dgi.ReadUint16())
		}
		c.sendableFields[do] = fields
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
		for i, o := range c.sessionObjects {
			if o == do {
				c.sessionObjects = append(c.sessionObjects[:i], c.sessionObjects[i+1:]...)
			}
		}
	case CLIENTAGENT_GET_TLVS:
		resp := NewDatagram()
		resp.AddServerHeader(sender, c.channel, CLIENTAGENT_GET_TLVS_RESP)
		resp.AddUint32(dgi.ReadUint32())
		// resp.AddDataBlob(c.client.Tlvs())
		c.RouteDatagram(resp)
		// TODO: Implement HAProxy
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
		if c.lookupObject(do) == nil {
			if c.tryQueuePending(do, dg) {
				return
			}
			c.log.Warnf("Received server-side field update for unknown object %d", do)
		}

		if sender != c.channel {
			field := dgi.ReadUint16()
			c.handleUpdateField(do, field, dgi)
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
		}

		for i, so := range c.sessionObjects {
			if so == do {
				c.sessionObjects = append(c.sessionObjects[:i], c.sessionObjects[i+1:]...)
				c.sendDisconnect(CLIENT_DISCONNECT_SESSION_OBJECT_DELETED,
					fmt.Sprintf("The session object with id %d has been unexpectedly deleted", do), false)
			}
		}

		for i, so := range c.seenObjects {
			if so == do {
				c.seenObjects = append(c.seenObjects[:i], c.seenObjects[i+1:]...)
				c.handleRemoveObject(do)
			}
		}

		if _, ok := c.ownedObjects[do]; ok {
			c.handleRemoveOwnership(do)
			delete(c.ownedObjects, do)
		}

		c.historicalObjects = append(c.historicalObjects, do)
		delete(c.visibleObjects, do)
	case STATESERVER_OBJECT_ENTER_OWNER_RECV:
		do, parent, zone, dc := dgi.ReadDoid(), dgi.ReadDoid(), dgi.ReadZone(), dgi.ReadUint16()

		if _, ok := c.ownedObjects[do]; !ok {
			cls := core.DC.Get_class(int(dc))
			c.ownedObjects[do] = OwnedObject{
				DeclaredObject: DeclaredObject{
					do: do,
					dc: cls,
				},
				parent: parent,
				zone:   zone,
			}
		}

		c.handleAddOwnership(do, parent, zone, dc, dgi)
	case STATESERVER_OBJECT_ENTER_LOCATION_WITH_REQUIRED:
		fallthrough
	case STATESERVER_OBJECT_ENTER_LOCATION_WITH_REQUIRED_OTHER:
		offset := dgi.Tell()
		do, parent, zone := dgi.ReadDoid(), dgi.ReadDoid(), dgi.ReadZone()
		for id, iop := range c.pendingInterests {
			if iop.parent == parent && iop.hasZone(zone) {
				iop.pendingQueue = append(iop.pendingQueue, dg)
				c.pendingObjects[do] = id
				return
			}
		}
		for _, iop := range c.interests {
			if iop.parent == parent && iop.hasZone(zone) {
				dgi.Seek(offset)
				c.handleObjectEntrance(dgi, msgType == STATESERVER_OBJECT_ENTER_LOCATION_WITH_REQUIRED_OTHER)
			}
		}
	case STATESERVER_OBJECT_GET_ZONE_COUNT_RESP:
		fallthrough
	case STATESERVER_OBJECT_GET_ZONES_COUNT_RESP:
		context := dgi.ReadUint32()
		if iop, ok := c.pendingInterests[context]; ok {
			total := dgi.ReadUint32()
			iop.setExpected(int(total))
		} else {
			c.log.Warnf("Got zone count for unknown interest: %d", context)
		}
	case STATESERVER_OBJECT_ENTER_INTEREST_WITH_REQUIRED:
		fallthrough
	case STATESERVER_OBJECT_ENTER_INTEREST_WITH_REQUIRED_OTHER:
		context := dgi.ReadUint32()
		if iop, ok := c.pendingInterests[context]; ok {
			if !iop.finished {
				iop.generateQueue = append(iop.generateQueue, dg)
				if iop.ready() {
					go iop.finish()
				}
			} else {
				// Message arrived late, announce generate.
				c.handleObjectEntrance(dgi, msgType == STATESERVER_OBJECT_ENTER_INTEREST_WITH_REQUIRED_OTHER)
			}
		}
	case STATESERVER_OBJECT_SET_ZONE:
		c.log.Warn("TODO: STATESERVER_OBJECT_SET_ZONE")
	case STATESERVER_OBJECT_CHANGE_OWNER_RECV:
		c.log.Warn("TODO: STATESERVER_OBJECT_CHANGE_OWNER_RECV")
	case DBSERVER_CREATE_STORED_OBJECT_RESP:
		context, code, doId := dgi.ReadUint32(), dgi.ReadUint8(), dgi.ReadDoid()
		c.handleCreateDatabaseResp(context, code, doId)
	case DBSERVER_GET_STORED_VALUES_RESP:
		c.handleGetStoredValuesResp(dgi)
	default:
		c.log.Errorf("Received unknown server msgtype %d", msgType)
	}
}

type InterestOperation struct {
	hasTotal bool
	finished bool
	total    int

	timeout      *time.Ticker
	finishedChan chan bool

	client         *Client
	interestId     uint16
	clientContext  uint32
	requestContext uint32
	parent         Doid_t

	zones   []Zone_t
	callers []Channel_t

	generateQueue []Datagram
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
		generateQueue:  []Datagram{},
		pendingQueue:   []Datagram{},
		callers:        []Channel_t{caller},
	}

	// Timeout
	go func() {
		select {
		case <-iop.timeout.C:
			if !iop.finished {
				client.log.Warnf("Interest operation timed out; forcing finish.")
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
	return i.hasTotal && len(i.generateQueue) >= i.total
}

func (i *InterestOperation) finish() {
	// We need to acquire our client's lock because we can't risk
	//  concurrent writes to pendingInterests
	i.client.Lock()
	defer i.client.Unlock()

	i.finished = true
	i.timeout.Stop()
	go func() {
		i.finishedChan <- true
	}()

	for _, generate := range i.generateQueue {
		dgi := NewDatagramIterator(&generate)
		dgi.SeekPayload()
		dgi.Skip(Chansize) // Skip sender

		msgType := dgi.ReadUint16()
		other := msgType == STATESERVER_OBJECT_ENTER_INTEREST_WITH_REQUIRED_OTHER

		dgi.Skip(Dgsize) // Skip request context
		i.client.handleObjectEntrance(dgi, other)
	}
	i.generateQueue = nil

	// Send out interest done messages
	i.client.notifyInterestDone(i.interestId, i.callers)
	i.client.handleInterestDone(i.interestId, i.clientContext)

	// Delete the IOP
	delete(i.client.pendingInterests, i.requestContext)
	for _, dg := range i.pendingQueue {
		dgi := NewDatagramIterator(&dg)
		dgi.SeekPayload()
		i.client.HandleDatagram(dg, dgi)
	}
	i.pendingQueue = nil
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
		c.heartbeat = time.NewTicker(time.Duration(config.Client.Heartbeat_Timeout) * time.Second)
		c.stopHeartbeat = make(chan bool)
		go c.startHeartbeat()
	}

	socket := net.NewSocketTransport(conn,
		time.Duration(config.Client.Keepalive)*time.Second, config.Client.Write_Buffer_Size)
	c.client = net.NewClient(socket, c, time.Duration(5)*time.Second)

	if !c.client.Local() {
		// event := eventlogger.NewLoggedEvent("client-connected", "")
		// event.Add("remote_address", conn.RemoteAddr().String())
		// event.Add("local_address", conn.LocalAddr().String())
		// c.logEvent(event)
	}
}

func (c *Client) startHeartbeat() {
	// Even though it is unnecessary, the heartbeat is contained in a select statement so that
	//  the ticker can be replaced each time a heartbeat is sent.
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
	// if !c.cleanDisconnect && !c.client.Local() {
	// 	event := eventlogger.NewLoggedEvent("client-lost", "")
	// 	event.Add("remote_address", c.conn.RemoteAddr().String())
	// 	event.Add("local_address", c.conn.LocalAddr().String())
	// 	event.Add("reason", err.Error())
	// 	event.Send()
	// }


	c.heartbeat.Stop()
	// (Sending to these channels from ReceiveDatagram or startHeartbeat
	// will deadlock, starting a separate goroutine fixes this.)
	go func() {
		// Stop the queue goroutine
		c.stopChan <- true
		// Stop the heartbeat goroutine
		c.stopHeartbeat <- true
	}()
	c.annihilate()

	c.client.Close()
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
	c.queue = c.queue[1:]
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
					c.ca.CallLuaFunction(c.ca.L.GetGlobal("receiveDatagram"), c,
					// Arguments:
					NewLuaClient(c.ca.L, c),
					NewLuaDatagramIteratorFromExisting(c.ca.L, dgi))
					finish <- true
				}()

				<-finish
				if len(dgi.ReadRemainder()) != 0 {
					c.sendDisconnect(CLIENT_DISCONNECT_OVERSIZED_DATAGRAM, "Datagram contains excess datc.", true)
				}
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
	c.heartbeat = time.NewTicker(time.Duration(c.config.Client.Heartbeat_Timeout) * time.Second)
}

func (c *Client) createDatabaseObject(objectType uint16, packedValues map[string]dc.Vector_uchar, callback func(doId Doid_t)) {
	c.createContextMap[c.context] = callback

	dg := NewDatagram()
	dg.AddServerHeader(c.ca.database, c.channel, DBSERVER_CREATE_STORED_OBJECT)
	dg.AddUint32(c.context)
	dg.AddString("") // Unknown
	dg.AddUint16(objectType)
	dg.AddUint16(uint16(len(packedValues)))

	for name, value := range packedValues {
		dg.AddString(name)
		dg.AddUint16(uint16(value.Size()))
		dg.AddVector(value)
		dc.DeleteVector_uchar(value)
	}
	c.RouteDatagram(dg)
	c.context++
}

func (c *Client) handleCreateDatabaseResp(context uint32, code uint8, doId Doid_t) {
	callback, ok := c.createContextMap[context]
	if !ok {
		c.log.Warnf("Got CreateDatabaseRsp with missing context %d", context)
		return
	}

	if code > 0 {
		c.log.Warnf("CreateDatabaseResp returned an error!")
	}

	callback(doId)
	delete(c.createContextMap, context)
}

func (c *Client) getDatabaseValues(doId Doid_t, fields []string, callback func(doId Doid_t, dgi *DatagramIterator)) {
	c.getContextMap[c.context] = callback

	dg := NewDatagram()
	dg.AddServerHeader(c.ca.database, c.channel, DBSERVER_GET_STORED_VALUES)
	dg.AddUint32(c.context)
	dg.AddDoid(doId)
	dg.AddUint16(uint16(len(fields)))
	for _, name := range fields {
		dg.AddString(name)
	}
	c.RouteDatagram(dg)
	c.context++
}

func (c *Client) handleGetStoredValuesResp(dgi *DatagramIterator) {
	context := dgi.ReadUint32()
	doId := dgi.ReadDoid()

	callback, ok := c.getContextMap[context]
	if !ok {
		c.log.Warnf("Got GetStoredResp with missing context %d", context)
		return
	}

	callback(doId, dgi)
	delete(c.getContextMap, context)
}

func (c *Client) setDatabaseValues(doId Doid_t, packedValues map[string]dc.Vector_uchar) {
	dg := NewDatagram()
	dg.AddServerHeader(c.ca.database, c.channel, DBSERVER_SET_STORED_VALUES)
	dg.AddDoid(doId)
	dg.AddUint16(uint16(len(packedValues)))

	for name, value := range packedValues {
		dg.AddString(name)
		dg.AddUint16(uint16(value.Size()))
		dg.AddVector(value)
		dc.DeleteVector_uchar(value)
	}

	c.RouteDatagram(dg)
}

func (c *Client) handleAddOwnership(do Doid_t, parent Doid_t, zone Zone_t, dc uint16, dgi *DatagramIterator) {
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
	if _, ok := c.ownedObjects[do]; ok && field.Is_ownsend() {
		return true
	} else if fields, ok := c.sendableFields[do]; ok {
		for _, v := range fields {
			if v == uint16(field.Get_number()) {
				return true
			}
		}
	}

	return field.Is_clsend()
}

func (c *Client) handleClientUpdateField(do Doid_t, field uint16, dgi *DatagramIterator) {
	dclass := c.lookupObject(do)
	if dclass == nil {
		c.sendDisconnect(CLIENT_DISCONNECT_MISSING_OBJECT, fmt.Sprintf("Attempted to send field update to unknown object: %d", do), true)
		return
	}

	dcField := dclass.Get_field_by_index(int(field))
	if dcField == dc.SwigcptrDCField(0) {
		c.sendDisconnect(CLIENT_DISCONNECT_FORBIDDEN_FIELD, fmt.Sprintf("Attempted to send field update to %s(%d) with unknown field: %d", dclass.Get_name(), do, field), true)
		return
	}

	if !c.isFieldSendable(do, dcField) {
		c.sendDisconnect(CLIENT_DISCONNECT_TRUNCATED_DATAGRAM, fmt.Sprintf("Attempted to send unsendable field %s", dcField.Get_name()), true)
		return
	}

	packedData := dgi.ReadRemainderAsVector()
	defer dc.DeleteVector_uchar(packedData)

	if !dcField.Validate_ranges(packedData) {
		c.sendDisconnect(CLIENT_DISCONNECT_TRUNCATED_DATAGRAM, fmt.Sprintf("Got truncated update for field %s", dcField.Get_name()), true)
		return
	}

	c.log.Debugf("Got client \"%s\" update for object %s(%d): %s", dcField.Get_name(), dclass.Get_name(), do, dcField.Format_data(packedData))

	// Send the message over to the object.
	dg := NewDatagram()
	dg.AddServerHeader(Channel_t(do), c.channel, STATESERVER_OBJECT_UPDATE_FIELD)
	dg.AddDoid(do)
	dg.AddUint16(field)
	dg.AddVector(packedData)

	c.RouteDatagram(dg)
}

func (c *Client) handleUpdateField(do Doid_t, field uint16, dgi *DatagramIterator) {
	resp := NewDatagram()
	resp.AddUint16(CLIENT_OBJECT_UPDATE_FIELD)
	resp.AddDoid(do)
	resp.AddUint16(field)
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
	c.log.Warn("TODO: handleAddInterest")
	// msgType := uint16(CLIENT_ADD_INTEREST)
	// if len(i.zones) > 0 {
	// 	msgType = uint16(CLIENT_ADD_INTEREST_MULTIPLE)
	// }

	// resp := NewDatagram()
	// resp.AddUint16(msgType)
	// resp.AddUint32(context)
	// resp.AddUint16(i.id)
	// resp.AddDoid(i.parent)
	// if len(i.zones) > 0 {
	// 	resp.AddUint16(uint16(len(i.zones)))
	// }
	// for _, zone := range i.zones {
	// 	resp.AddZone(zone)
	// }
	// c.client.SendDatagram(resp)
}

func (c *Client) handleRemoveObject(do Doid_t) {
	resp := NewDatagram()
	resp.AddUint16(CLIENT_OBJECT_DELETE)
	resp.AddDoid(do)
	c.client.SendDatagram(resp)
}

func (c *Client) handleAddObject(do Doid_t, parent Doid_t, zone Zone_t, dc uint16, dgi *DatagramIterator, other bool) {
	msgType := CLIENT_CREATE_OBJECT_REQUIRED
	if other {
		msgType = CLIENT_CREATE_OBJECT_REQUIRED_OTHER
	}

	resp := NewDatagram()
	resp.AddUint16(uint16(msgType))
	resp.AddLocation(parent, zone)
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
		c.ca.CallLuaFunction(lFunc, c, lua.LNumber(interestId), lua.LNumber(context))
		return
	}
	resp := NewDatagram()
	resp.AddUint16(CLIENT_DONE_INTEREST_RESP)
	resp.AddUint16(interestId)
	resp.AddUint32(context)
	c.client.SendDatagram(resp)
}

func (c *Client) SetChannel(channel Channel_t) {
	if (c.channel == channel) {
		return
	}
	if (c.channel != c.allocatedChannel) {
		c.UnsubscribeChannel(c.channel)
	}
	c.channel = channel
	c.SubscribeChannel(channel)
}
