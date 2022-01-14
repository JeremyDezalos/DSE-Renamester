package impl

import (
	"math/rand"
	"sync"

	"go.dedis.ch/cs438/peer"
	"go.dedis.ch/cs438/types"
	"golang.org/x/xerrors"
)

// Thread safe routing table
// type lockedRoutingTable struct {
// 	sync.Mutex
// 	routingTable peer.RoutingTable
// 	neighbors    map[string]string
// }

type lockedRoutingTable struct {
	sync.Mutex
	routingTable map[string]types.RoutingTableEntry
}

func (lockedRoutingTable *lockedRoutingTable) getCopy() map[string]types.RoutingTableEntry {
	lockedRoutingTable.Lock()
	defer lockedRoutingTable.Unlock()
	copy := make(map[string]types.RoutingTableEntry, len(lockedRoutingTable.routingTable))
	for k, v := range lockedRoutingTable.routingTable {
		entry := types.RoutingTableEntry{
			v.NextHop,
			v.Address,
			v.Alias,
		}
		copy[k] = entry
	}
	return copy
}

// Thread safe get of a routing table next hop
func (lockedRoutingTable *lockedRoutingTable) get(key string) (string, bool) {
	lockedRoutingTable.Lock()
	defer lockedRoutingTable.Unlock()
	val, ok := lockedRoutingTable.routingTable[key]
	return val.NextHop, ok
}

// Thread safe add or edit of a routing table next hop
func (lockedRoutingTable *lockedRoutingTable) add(origin, relay string) {
	lockedRoutingTable.Lock()
	defer lockedRoutingTable.Unlock()
	val, ok := lockedRoutingTable.routingTable[origin]
	if ok {
		lockedRoutingTable.routingTable[origin] = types.RoutingTableEntry{relay, val.Address, val.Alias}
	} else {
		lockedRoutingTable.routingTable[origin] = types.RoutingTableEntry{relay, "", origin}
	}
}

// Thread safe add or edit of a complete routing table entry
func (lockedRoutingTable *lockedRoutingTable) setEntry(key, nextHop, address, alias string) {
	lockedRoutingTable.Lock()
	defer lockedRoutingTable.Unlock()
	lockedRoutingTable.routingTable[key] = types.RoutingTableEntry{nextHop, address, alias}
}

// Thread safe update of the alias of a routing table entry
func (lockedRoutingTable *lockedRoutingTable) updateAlias(key, alias string) error {
	lockedRoutingTable.Lock()
	defer lockedRoutingTable.Unlock()
	val, ok := lockedRoutingTable.routingTable[key]
	if !ok {
		return xerrors.Errorf("peer is not present in the routing table: %s", key)
	}
	lockedRoutingTable.routingTable[key] = types.RoutingTableEntry{val.NextHop, val.Address, alias}
	return nil
}

// Thread safe get of a neighbhor entry
func (lockedRoutingTable *lockedRoutingTable) resolveNeighbor(key string) (string, bool) {
	lockedRoutingTable.Lock()
	defer lockedRoutingTable.Unlock()
	val, ok := lockedRoutingTable.routingTable[key]
	addr := val.Address
	if ok {
		ok = addr != ""
	}
	return addr, ok
}

// Thread safe add of a neighbhor entry
func (lockedRoutingTable *lockedRoutingTable) addNeighbor(key string, address string) {
	lockedRoutingTable.Lock()
	defer lockedRoutingTable.Unlock()
	val, ok := lockedRoutingTable.routingTable[key]
	if ok {
		lockedRoutingTable.routingTable[key] = types.RoutingTableEntry{val.NextHop, address, val.Alias}
	} else {

		lockedRoutingTable.routingTable[key] = types.RoutingTableEntry{key, address, key}
	}
}

// Thread safe delete of a routing table entry (and it's corresponding neighbor entry)
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
