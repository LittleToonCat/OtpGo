package util

type Channel_t uint64
type Doid_t uint32
type Zone_t uint32
type Dgsize_t uint32

const Dgsize = 4   // size of uint32
const Doidsize = 4 // size of uint32
const Zonesize = 4 // size of uint32
const Chansize = 8 // size of uint64
const Blobsize = 2 // size of uint16

const (
	CHANNEL_MAX = ^Channel_t(0)
	DOID_MAX    = ^Doid_t(0)
	ZONE_MAX    = ^Zone_t(0)
	ZONE_BITS   = 32
)

const INVALID_DOID = Doid_t(0)
const INVALID_ZONE = Zone_t(0)

const (
	INVALID_CHANNEL    = Channel_t(0)
	BCHAN_CLIENTS      = Channel_t(10)
	BCHAN_STATESERVERS = Channel_t(12)
	BCHAN_DBSERVERS    = Channel_t(13)
	PARENT_PREFIX      = Channel_t(1) << ZONE_BITS
	DATABASE_PREFIX    = Channel_t(2) << ZONE_BITS
	CONTROL_MESSAGE    = Channel_t(4001)
)

func LocationAsChannel(parent Doid_t, zone Zone_t) Channel_t {
	return Channel_t(parent)<<ZONE_BITS | Channel_t(zone)
}

func ParentToChildren(parent Doid_t) Channel_t {
	return PARENT_PREFIX | Channel_t(parent)
}

func DatabaseToObject(object Doid_t) Channel_t {
	return DATABASE_PREFIX | Channel_t(object)
}
