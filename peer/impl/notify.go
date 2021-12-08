package impl

import (
	"sync"

	"go.dedis.ch/cs438/types"
)

// I want to implement this with generics but this is a go 1.17 feature, not sure if allowed
// type notifyMap(type T) struct {
// 	sync.Mutex
// 	waitingChan map[string](chan T)
// }

// // Notify <id> channel with <content>
// func (notifyChan *notifyMap(type T)) chanNotify(id string, content interface{}) {
// 	notifyChan.Lock()
// 	defer notifyChan.Unlock()
// 	val, ok := notifyChan.waitingChan[id]
// 	if ok {
// 		val <- content
// 	}
// }

// // Thread safe creation of SearchChanMap map entry
// // Create and return a channel for the key
// func (notifyChan *notifyMap(type T)) create(key string) chan interface{} {
// 	notifyChan.Lock()
// 	defer notifyChan.Unlock()
// 	notifyChan.waitingChan[key] = make(chan interface{})
// 	return notifyChan.waitingChan[key]
// }

// // Thread safe deletion of SearchChanMap map entry
// func (notifyChan *notifyMap) deleteChan(key string) {
// 	notifyChan.Lock()
// 	defer notifyChan.Unlock()
// 	delete(notifyChan.waitingChan, key)
// }

type ackChanMap struct {
	sync.Mutex
	waitingChan map[string](chan struct{})
}

type dataChanMap struct {
	sync.Mutex
	waitingChan map[string](chan []byte)
}

type searchChanMap struct {
	sync.Mutex
	waitingChan map[string](chan []types.FileInfo)
}

// Thread notified a ack as been received for a given packet
func (waitedChan *ackChanMap) chanNotify(id string) {
	waitedChan.Lock()
	defer waitedChan.Unlock()
	val, ok := waitedChan.waitingChan[id]
	if ok {
		val <- struct{}{}
	}
}

// Thread safe creation of AckChanMap map entry
// Create and return a channel for the pktID
func (waitedChan *ackChanMap) createChan(key string) chan struct{} {
	waitedChan.Lock()
	defer waitedChan.Unlock()
	waitedChan.waitingChan[key] = make(chan struct{})
	return waitedChan.waitingChan[key]
}

// Thread safe deletion of AckChanMap map entry
func (waitedChan *ackChanMap) deleteChan(key string) {
	waitedChan.Lock()
	defer waitedChan.Unlock()
	delete(waitedChan.waitingChan, key)
}

// Thread notified data has been received for a given request
func (waitedChan *dataChanMap) chanNotify(id string, data []byte) {
	waitedChan.Lock()
	defer waitedChan.Unlock()
	val, ok := waitedChan.waitingChan[id]
	if ok {
		val <- data
	}
}

// Thread safe creation of DataChanMap map entry
// Create and return a channel for the key
func (waitedChan *dataChanMap) createChan(key string) chan []byte {
	waitedChan.Lock()
	defer waitedChan.Unlock()
	waitedChan.waitingChan[key] = make(chan []byte)
	return waitedChan.waitingChan[key]
}

// Thread safe deletion of DataChanMap map entry
func (waitedChan *dataChanMap) deleteChan(key string) {
	waitedChan.Lock()
	defer waitedChan.Unlock()
	delete(waitedChan.waitingChan, key)
}

// Thread notified Search result has been received for a given request
func (waitedChan *searchChanMap) chanNotify(id string, responses []types.FileInfo) {
	waitedChan.Lock()
	defer waitedChan.Unlock()
	val, ok := waitedChan.waitingChan[id]
	if ok {
		val <- responses
	}
}

// Thread safe creation of SearchChanMap map entry
// Create and return a channel for the key
func (waitedChan *searchChanMap) createChan(key string) chan []types.FileInfo {
	waitedChan.Lock()
	defer waitedChan.Unlock()
	waitedChan.waitingChan[key] = make(chan []types.FileInfo)
	return waitedChan.waitingChan[key]
}

// Thread safe deletion of SearchChanMap map entry
func (waitedChan *searchChanMap) deleteChan(key string) {
	waitedChan.Lock()
	defer waitedChan.Unlock()
	delete(waitedChan.waitingChan, key)
}
