package impl

import (
	"sync"

	"go.dedis.ch/cs438/types"
)

type rumorsCollection struct {
	sync.Mutex
	lockedRumors map[string][]types.Rumor
}

// Add all rumors to the rumorsCollection
// Return a slice with all accepted rumors
func (rumorsCollection *rumorsCollection) addRumors(rumors []types.Rumor) []types.Rumor {
	rumorsCollection.Lock()
	defer rumorsCollection.Unlock()

	acceptedRumors := make([]types.Rumor, 0, len(rumors))
	// This does not try to reorder the rumor
	// (assumes you receive them in order and drop them if it's not the case)
	for i, rumor := range rumors {
		var expected uint = 1
		currRumors, ok := rumorsCollection.lockedRumors[rumor.Origin]
		if ok {
			expected = uint(len(currRumors) + 1)
		} else {
			rumorsCollection.lockedRumors[rumor.Origin] = make([]types.Rumor, 0, 10)
		}
		if rumor.Sequence == expected {
			rumorsCollection.lockedRumors[rumor.Origin] = append(currRumors, rumor)
			acceptedRumors = append(acceptedRumors, rumors[i])
		}
	}
	return acceptedRumors
}

// Get all rumors from first to last available for a peer
func (rumorsCollection *rumorsCollection) getRumors(peer string, first uint) []types.Rumor {
	rumorsCollection.Lock()
	defer rumorsCollection.Unlock()
	rumors := rumorsCollection.lockedRumors[peer][first-1:]
	return rumors
}

// Generate status message out of a collection of rumors
func (rumorsCollection *rumorsCollection) generateStatusMessage() types.StatusMessage {
	rumorsCollection.Lock()
	defer rumorsCollection.Unlock()
	var statusMsg = make(types.StatusMessage)
	for k, v := range rumorsCollection.lockedRumors {
		statusMsg[k] = uint(len(v))
	}
	return statusMsg
}
