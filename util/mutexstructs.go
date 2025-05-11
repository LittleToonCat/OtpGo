// Functions to support structures that need locking.

package util

import (
	"maps"
	"reflect"
	"sync"
)

// MutexMap is a struct containing a map and a mutex. MutexMaps can use supporting functions to read and write data with appropriate locking.
type MutexMap[keyType comparable, valueType any] struct {
	innerMap map[keyType]valueType
	mutex sync.RWMutex
}

// NewMutexMap returns a pointer to a new MutexMap.
func NewMutexMap[keyType comparable, valueType any]() *MutexMap[keyType, valueType]{
	return &MutexMap[keyType, valueType] {
		innerMap: make(map[keyType]valueType),
	}
}


// Get returns the value assigned to a given key in the mutex map, along with a bool indicating whether the key was found.
func (mutexMap *MutexMap[keyType, valueType]) Get(key keyType) (valueType, bool) {
	mutexMap.mutex.RLock()
	defer mutexMap.mutex.RUnlock()
	mapValue, ok := mutexMap.innerMap[key]
	return mapValue, ok
}

// GetNoLock returns the value assigned to a given key in the mutex map, along with a bool indicating whether the key was found.
// Should only be used if you've already acquired a lock, for example inside an Iterator.
func (mutexMap *MutexMap[keyType, valueType]) GetNoLock(key keyType) (valueType, bool) {
	mapValue, ok := mutexMap.innerMap[key]
	return mapValue, ok
}

// Set adds a value to the mutex map with the given key and returns the key.
// If holdLock is true, then the mutex will not be unlocked automatically; call [MutexMap.Unlock] to unlock the mutex as needed.
func (mutexMap *MutexMap[keyType, valueType]) Set(key keyType, value valueType, holdLock bool) keyType {
	mutexMap.mutex.Lock()
	if (!holdLock) { 
		defer mutexMap.mutex.Unlock()
	}
	mutexMap.innerMap[key] = value
	return key
}

// SetNoLock adds a value to the mutex map with the given key and returns the key.
// Should only be used if you've already acquired the write-lock, for example inside a WriteIterator.
func (mutexMap *MutexMap[keyType, valueType]) SetNoLock(key keyType, value valueType) keyType {
	mutexMap.innerMap[key] = value
	return key
}

// Delete deletes the key/value pair with the given key from the mutex map.
// If holdLock is true, then the mutex will not be unlocked automatically; call [MutexMap.Unlock] to unlock the mutex as needed.
func (mutexMap *MutexMap[keyType, valueType]) Delete(key keyType, holdLock bool) {
	mutexMap.mutex.Lock()
	if (!holdLock) {
		defer mutexMap.mutex.Unlock()
	}
	delete(mutexMap.innerMap, key)
}

// Clear removes all key/value pairs from the mutex map.
// If holdLock is true, then the mutex will not be unlocked automatically; call [MutexMap.Unlock] to unlock the mutex as needed.
func (mutexMap *MutexMap[keyType, valueType]) Clear(holdLock bool) {
	mutexMap.mutex.Lock()
	if (!holdLock) {
		defer mutexMap.mutex.Unlock()
	}
	clear(mutexMap.innerMap)
}

// Clone returns a copy of the inner map.
func (mutexMap *MutexMap[keyType, valueType]) Clone() *map[keyType]valueType {
	mutexMap.mutex.RLock()
	defer mutexMap.mutex.RUnlock()
	cloneMap := maps.Clone(mutexMap.innerMap)
	return &cloneMap
}

// Iterator returns a MapIter for the inner map. The mutex is read-locked, but must be manually read-unlocked by calling [MutexMap.RUnlock] once iteration is complete.
func (mutexMap *MutexMap[keyType, valueType]) Iterator() *reflect.MapIter {
	mutexMap.mutex.RLock()
	return reflect.ValueOf(mutexMap.innerMap).MapRange()
}

// Iterator returns a MapIter for the inner map. The mutex is write-locked, but must be manually write-unlocked by calling [MutexMap.Unlock] once iteration is complete.
func (mutexMap *MutexMap[keyType, valueType]) WriteIterator() *reflect.MapIter {
	mutexMap.mutex.Lock()
	return reflect.ValueOf(mutexMap.innerMap).MapRange()
}

// Length returns the length of the mutex map.
func (mutexMap *MutexMap[keyType, valueType]) Length() int {
	mutexMap.mutex.RLock()
	defer mutexMap.mutex.RUnlock()
	return len(mutexMap.innerMap)
}

// Unlock write-unlocks the mutex map's mutex. Only use this if you called a write function and now need to unlock the mutex manually.
func (mutexMap *MutexMap[keyType, valueType]) Unlock() {
	mutexMap.mutex.Unlock()
}

// RUnlock read-unlocks the mutex map's mutex. Only use this if you have finished with an iterator provided by [MutexMap.Iterator]. 
func (mutexMap *MutexMap[keyType, valueType]) RUnlock() {
	mutexMap.mutex.RUnlock()
}