package impl

import (
	"math/rand"
	"sync"

	"go.dedis.ch/cs438/peer"
)

// type idRoutingTable map[crypto.PublicKey]string

// // Thread safe routing table
// type lockedIdRoutingTable struct {
// 	sync.Mutex
// 	routingTable idRoutingTable
// }

// // Thread safe get of a routing table entry
// func (lockedRoutingTable *lockedIdRoutingTable) get(key crypto.PublicKey) (string, bool) {
// 	lockedRoutingTable.Lock()
// 	defer lockedRoutingTable.Unlock()
// 	val, ok := lockedRoutingTable.routingTable[key]
// 	return val, ok
// }

// // Thread safe add or edit of a routing table entry
// func (lockedRoutingTable *lockedIdRoutingTable) add(key, value string) {
// 	lockedRoutingTable.Lock()
// 	defer lockedRoutingTable.Unlock()
// 	lockedRoutingTable.routingTable[key] = value
// }

// // Thread safe delete of a routing table entry
// func (lockedRoutingTable *lockedIdRoutingTable) delete(key string) {
// 	lockedRoutingTable.Lock()
// 	defer lockedRoutingTable.Unlock()
// 	delete(lockedRoutingTable.routingTable, key)

// }

// // Get all neighbors from a routing table
// func getNeighbors(routingTable idRoutingTable) map[crypto.PublicKey]struct{} {
// 	neighbors := make(map[crypto.PublicKey]struct{})
// 	for k, v := range routingTable {
// 		if k == v {
// 			neighbors[k] = struct{}{}
// 		}
// 	}
// 	return neighbors
// }

// // Return a random neighbor from a routing table excluding a list of addresses
// func getRandomNeighbor(routingTable idRoutingTable, exclude ...string) crypto.PublicKey {
// 	for _, v := range exclude {
// 		delete(routingTable, v)
// 	}
// 	neighbors := getNeighbors(routingTable)
// 	return getRandomPeer(neighbors)
// }

// // Return a random peer in a set of peers
// // Return an empty string if there is 0 peer in the set
// func getRandomPeer(peers map[crypto.PublicKey]struct{}) crypto.PublicKey {
// 	var randPeer crypto.PublicKey
// 	nbPeers := (len(peers))
// 	if nbPeers == 0 {
// 		return randPeer
// 	}
// 	rand := rand.Intn(nbPeers)
// 	for k := range peers {
// 		randPeer = k
// 		if rand == 0 {
// 			break
// 		}
// 		rand--
// 	}
// 	return randPeer
// }

// Thread safe routing table
type lockedRoutingTable struct {
	sync.Mutex
	routingTable peer.RoutingTable
}

// Thread safe get of a routing table entry
func (lockedRoutingTable *lockedRoutingTable) get(key string) (string, bool) {
	lockedRoutingTable.Lock()
	defer lockedRoutingTable.Unlock()
	val, ok := lockedRoutingTable.routingTable[key]
	return val, ok
}

// Thread safe add or edit of a routing table entry
func (lockedRoutingTable *lockedRoutingTable) add(key, value string) {
	lockedRoutingTable.Lock()
	defer lockedRoutingTable.Unlock()
	lockedRoutingTable.routingTable[key] = value
}

// Thread safe delete of a routing table entry
func (lockedRoutingTable *lockedRoutingTable) delete(key string) {
	lockedRoutingTable.Lock()
	defer lockedRoutingTable.Unlock()
	delete(lockedRoutingTable.routingTable, key)

}

// Get all neighbors from a routing table
func getNeighbors(routingTable peer.RoutingTable) map[string]struct{} {
	neighbors := make(map[string]struct{})
	for _, v := range routingTable {
		neighbors[v] = struct{}{}
	}
	return neighbors
}

// Return a random neighbor from a routing table excluding a list of addresses
func getRandomNeighbor(routingTable peer.RoutingTable, exclude ...string) string {
	for _, v := range exclude {
		delete(routingTable, v)
	}
	neighbors := getNeighbors(routingTable)
	return getRandomPeer(neighbors)
}

// Return a random peer in a set of peers
// Return an empty string if there is 0 peer in the set
func getRandomPeer(peers map[string]struct{}) string {
	var randPeer string
	nbPeers := (len(peers))
	if nbPeers == 0 {
		return randPeer
	}
	rand := rand.Intn(nbPeers)
	for k := range peers {
		randPeer = k
		if rand == 0 {
			break
		}
		rand--
	}
	return randPeer
}
