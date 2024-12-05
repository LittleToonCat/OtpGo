// A storage for preallocated DistributedObjects.
package stateserver

import (
	"fmt"
	. "otpgo/util"
	"sync"

	"otpgo/dc"

	"github.com/apex/log"
)

type DOStorage struct {
	DOPool sync.Pool
}

func NewDOStorage(preallocNum int) *DOStorage {
	doStore := &DOStorage{
		DOPool: sync.Pool{
			New: func() any {
				return &DistributedObject{requiredFields: make(map[dc.DCField][]byte),
					ramFields:   make(map[dc.DCField][]byte),
					zoneObjects: make(map[Zone_t][]Doid_t),
				}
			},
		},
	}

	for i := 0; i < preallocNum; i++ {
		do := &DistributedObject{requiredFields: make(map[dc.DCField][]byte),
			ramFields:   make(map[dc.DCField][]byte),
			zoneObjects: make(map[Zone_t][]Doid_t),
		}
		doStore.DOPool.Put(do)
	}

	return doStore
}

func (doStore *DOStorage) recycleDO(do *DistributedObject) {
	do.log = nil
	do.stateserver = nil
	do.do = 0
	do.parent = 0
	do.zone = 0
	do.dclass = nil
	// We want to reuse the maps where possible; these are already cleared on annihilation.
	do.aiChannel = 0
	do.ownerChannel = 0
	do.explicitAi = false
	do.parentSynchronized = false
	do.RecycleParticipant()
	doStore.DOPool.Put(do)
}

func (doStore *DOStorage) createDO(ss *StateServer, doid Doid_t,
	dclass dc.DCClass, requiredFields FieldValues,
	ramFields FieldValues) *DistributedObject {
	do := doStore.DOPool.Get().(*DistributedObject)
	do.log = log.WithFields(log.Fields{
		"name":    fmt.Sprintf("%s (%d)", dclass.Get_name(), doid),
		"modName": dclass.Get_name(),
		"id":      fmt.Sprintf("%d", doid),
	})
	do.stateserver = ss
	do.do = doid
	do.zone = INVALID_ZONE
	do.dclass = dclass

	if requiredFields != nil {
		do.requiredFields = requiredFields
	}

	if ramFields != nil {
		do.ramFields = ramFields
	}

	// All DistributedObjects reuse their zoneObjects map, so we don't need to set it here.

	return do
}
