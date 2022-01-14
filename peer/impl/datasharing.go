package impl

import (
	"crypto"
	"encoding/hex"
	"io"
	"regexp"
	"strings"
	"sync"
	"time"

	"github.com/rs/xid"
	"go.dedis.ch/cs438/peer"
	"go.dedis.ch/cs438/transport"
	"go.dedis.ch/cs438/types"
	"golang.org/x/xerrors"
)

type lockedCatalog struct {
	sync.Mutex
	catalog peer.Catalog
}

type searchesReceived struct {
	sync.Mutex
	searches map[string]struct{}
}

func (n *node) Upload(data io.Reader) (metahash string, err error) {
	n.checkAndwaitReconnection()
	blob := n.conf.Storage.GetDataBlobStore()
	content, err := io.ReadAll(data)
	if err != nil {
		return "", xerrors.Errorf("failed read data: %v", err)
	}
	chunkSize := int(n.conf.ChunkSize)
	contentSize := len(content)
	nbChunk := contentSize / chunkSize
	if contentSize%chunkSize != 0 {
		nbChunk++
	}
	metafileValue := ""
	metafileKeyHash := make([]byte, 0)
	for i := 0; i < nbChunk; i++ {
		end := (i + 1) * chunkSize
		if end >= contentSize {
			end = contentSize
		}
		// Encode and store inividual chunk
		chunk := content[i*chunkSize : end]
		h := crypto.SHA256.New()
		_, err = h.Write(chunk)
		if err != nil {
			return "", xerrors.Errorf("failed hash chunk %d: %v", i, err)
		}
		metahashSlice := h.Sum(nil)
		fChunk := hex.EncodeToString(metahashSlice)
		blob.Set(fChunk, chunk)
		// Compute metafile key and value
		metafileValue = metafileValue + fChunk
		if end != contentSize {
			metafileValue = metafileValue + peer.MetafileSep
		}
		metafileKeyHash = append(metafileKeyHash, metahashSlice...)
	}
	// Store metafile
	h := crypto.SHA256.New()
	_, err = h.Write(metafileKeyHash)
	if err != nil {
		return "", xerrors.Errorf("failed hash metafileKey: %v", err)
	}
	metahash = hex.EncodeToString(h.Sum(nil))
	blob.Set(metahash, []byte(metafileValue))
	return
}

func (n *node) Download(metahash string) ([]byte, error) {
	n.checkAndwaitReconnection()
	// Get metafile
	metafile, err := n.getLocalOrRemoteData(metahash)
	if err != nil {
		return nil, xerrors.Errorf("metafile download failed: %v", err)
	}

	// Get each chunk of the metafile and add it to the file
	file := make([]byte, 0)
	metafileStr := string(metafile)
	fChunks := strings.Split(metafileStr, peer.MetafileSep)
	for _, fChunk := range fChunks {
		chunk, err := n.getLocalOrRemoteData(fChunk)
		if err != nil {
			return nil, xerrors.Errorf("chunk download failed: %v", err)
		}
		file = append(file, chunk...)
	}
	return file, nil
}

func (n *node) getLocalOrRemoteData(hash string) ([]byte, error) {
	blob := n.conf.Storage.GetDataBlobStore()
	data := blob.Get(hash)
	if data == nil {
		peers, ok := n.GetCatalog()[hash]
		if !ok {
			return nil, xerrors.Errorf("unknown hash")
		}
		peer := getRandomPeer(peers)
		requestID := xid.New().String()
		request := types.DataRequestMessage{
			RequestID: requestID,
			Key:       hash,
		}
		header := transport.NewHeader(n.id, n.id, peer, 0)
		msg, err := n.conf.MessageRegistry.MarshalMessage(&request)
		if err != nil {
			return nil, xerrors.Errorf("failed to marshal data request message: %v", err)
		}
		pkt := transport.Packet{
			Header: &header,
			Msg:    &msg,
		}
		data, err = n.waitForDataReply(pkt, requestID)
		if err != nil {
			return nil, xerrors.Errorf("failed to receive data reply from peer: %v", err)
		}
		if data == nil {
			return nil, xerrors.Errorf("received empty data reply from peer")
		}
		blob.Set(hash, data)
	}
	return data, nil
}

// Wait for data reply of a request with requestID
// Return the data received in the reply
func (n *node) waitForDataReply(pkt transport.Packet, requestID string) ([]byte, error) {
	backoff := n.conf.BackoffDataRequest
	waitChan := n.waitedDataReply.createChan(requestID)
	defer n.waitedDataReply.deleteChan(requestID)
	// Handout does not mention possibilty of zero valued backoff parameter
	currentTimeout := backoff.Initial
	tries := 0
	err := n.sendPacket(pkt)
	if err != nil {
		return nil, xerrors.Errorf("failed to send data request packet: %v", err)
	}
	t := time.NewTimer(currentTimeout)
	for {
		select {
		case data := <-waitChan:
			// Stop timer
			t.Stop()
			return data, nil
		case <-t.C:
			// Retry
			tries++
			if tries >= int(backoff.Retry) {
				return nil, xerrors.Errorf("too many retries for request : %s", requestID)
			}
			currentTimeout = currentTimeout * time.Duration(backoff.Factor)
			err := n.sendPacket(pkt)
			if err != nil {
				return nil, xerrors.Errorf("failed to send data request packet: %v", err)
			}
			t.Reset(currentTimeout)
		}
	}
}

func (n *node) Tag(name string, mh string) error {
	n.checkAndwaitReconnection()
	store := n.conf.Storage.GetNamingStore()
	check := store.Get(name)
	if check != nil {
		return xerrors.Errorf("name already exists")
	}
	if n.conf.TotalPeers <= 1 {
		store.Set(name, []byte(mh))
	} else {
		n.paxos.proposer.MsgsPreparedMutex.Lock()
		id := n.conf.PaxosID + n.conf.TotalPeers*uint(len(n.paxos.proposer.MsgsPrepared))
		n.paxos.proposer.MsgsPreparedMutex.Unlock()
		prepareMsg := types.PaxosPrepareMessage{
			Step:   0,
			ID:     id,
			Source: n.id,
		}

		prepare, err := n.conf.MessageRegistry.MarshalMessage(prepareMsg)
		if err != nil {
			return xerrors.Errorf("failed to marshall prepare message")
		}
		n.paxos.proposer.PhaseLock.Lock()
		n.paxos.proposer.Phase = 1
		n.paxos.proposer.PhaseLock.Unlock()
		n.paxos.proposer.PromisesLock.Lock()
		n.paxos.proposer.PromisesReceived = 0
		n.paxos.proposer.PromisesLock.Unlock()
		n.paxos.proposer.CountLock.Lock()
		n.paxos.proposer.AcceptCount = make(map[string]uint)
		n.paxos.proposer.CountLock.Unlock()
		uniqId := xid.New().String()
		n.paxos.proposer.AcceptedValue = PaxosAcceptStatus{
			acceptedID: id,
			acceptedValue: types.PaxosValue{
				UniqID:   uniqId,
				Filename: name,
				Metahash: mh,
			},
		}

		n.paxos.prepareMsg <- prepare
		n.paxos.proposer.MsgsPreparedMutex.Lock()
		n.paxos.proposer.MsgsPrepared[uniqId] = struct{}{}
		n.paxos.proposer.MsgsPreparedMutex.Unlock()
		//fmt.Println("retry tag 1")
		n.RetryTag(name, mh)
		<-n.paxos.proposer.TagIsDone

		return err
	}
	return nil
}

func (n *node) Resolve(name string) (metahash string) {
	return string(n.conf.Storage.GetNamingStore().Get(name))
}

func (n *node) GetCatalog() peer.Catalog {
	n.lockedCatalog.Lock()
	defer n.lockedCatalog.Unlock()
	catalogCopy := make(peer.Catalog, len(n.lockedCatalog.catalog))
	for key, peers := range n.lockedCatalog.catalog {
		peersCopy := make(map[string]struct{}, len(peers))
		for peer, empty := range peers {
			peersCopy[peer] = empty
		}
		catalogCopy[key] = peersCopy
	}
	return catalogCopy
}

func (n *node) UpdateCatalog(key string, peer string) {
	n.lockedCatalog.Lock()
	defer n.lockedCatalog.Unlock()
	_, ok := n.lockedCatalog.catalog[key]
	if !ok {
		n.lockedCatalog.catalog[key] = make(map[string]struct{})
	}
	n.lockedCatalog.catalog[key][peer] = struct{}{}
}

func (n *node) SearchAll(reg regexp.Regexp, budget uint, timeout time.Duration) (names []string, err error) {
	n.checkAndwaitReconnection()
	requestID := xid.New().String()
	err = n.Search(reg, budget, requestID, n.id, getNeighbors(n.GetRoutingTable()))
	if err != nil {
		err = xerrors.Errorf("failed to search (all): %v", err)
		names = nil
		return
	}
	namesSet := make(map[string]struct{})
	waitChan := n.waitedSearchReply.createChan(requestID)
	defer n.waitedSearchReply.deleteChan(requestID)
	t := time.NewTimer(timeout)
	for {
		select {
		case responses := <-waitChan:
			// Append search responses
			for _, response := range responses {
				namesSet[response.Name] = struct{}{}
			}
		case <-t.C:
			// Append local files
			n.conf.Storage.GetNamingStore().ForEach(func(key string, val []byte) bool {
				namesSet[key] = struct{}{}
				return true
			})
			names = make([]string, 0)
			for name := range namesSet {
				names = append(names, name)
			}

			return
		}
	}
}

func (n *node) Search(reg regexp.Regexp, budget uint, requestID string, origin string, neighbors map[string]struct{}) error {
	delete(neighbors, n.id)
	if len(neighbors) == 0 {
		return nil
	}
	budgetDiv := int(budget) / len(neighbors)
	budgetRemain := int(budget) % len(neighbors)
	// Ussing Go randomness of range on map
	for neighbor := range neighbors {
		reqBudget := budgetDiv
		if budgetRemain > 0 {
			budgetRemain--
			reqBudget++
		}
		if reqBudget == 0 {
			break
		}
		searchReq := types.SearchRequestMessage{
			RequestID: requestID,
			Origin:    origin,
			Pattern:   reg.String(),
			Budget:    uint(reqBudget),
		}
		searchMsg, err := n.conf.MessageRegistry.MarshalMessage(&searchReq)
		if err != nil {
			return xerrors.Errorf("failed to marshal search request message: %v", err)
		}
		searchHeader := transport.NewHeader(n.id, n.id, neighbor, 0)
		searchPkt := transport.Packet{
			Header: &searchHeader,
			Msg:    &searchMsg,
		}
		n.sendPacket(searchPkt)
		if err != nil {
			return xerrors.Errorf("failed to send search request packet: %v", err)
		}
	}
	return nil
}

func (n *node) SearchFirst(pattern regexp.Regexp, conf peer.ExpandingRing) (name string, err error) {
	n.checkAndwaitReconnection()
	// Same as search all but in waitForSearchFirstReplies :
	//    - Discard responses names if the fileInfo does not indicate all chunks are available
	//    - Stop on first full name found
	//    - Expand ring on timeout (until Retry limit)
	name = ""
	// Local search
	naming := n.conf.Storage.GetNamingStore()
	namesToHash := make(map[string]string)
	naming.ForEach(func(key string, val []byte) bool {
		validName := pattern.FindString(key)
		if validName != "" {
			namesToHash[key] = string(val)
		}
		return true
	})
	blob := n.conf.Storage.GetDataBlobStore()
	for localName, metahash := range namesToHash {
		metafile := blob.Get(metahash)
		if metafile != nil {
			fChunks := strings.Split(string(metafile), peer.MetafileSep)
			fullyKnown := true
			for _, fChunk := range fChunks {
				if blob.Get(fChunk) == nil {
					fullyKnown = false
					break
				}
			}
			if fullyKnown {
				name = localName
				return
			}
		}
	}

	budget := conf.Initial
	neighbors := getNeighbors(n.GetRoutingTable())
	if len(neighbors) == 1 {
		return
	}
	requestID := xid.New().String()
	err = n.Search(pattern, budget, requestID, n.id, getNeighbors(n.GetRoutingTable()))
	if err != nil {
		xerrors.Errorf("failed to search (first): %v", err)
		return
	}
	tries := 0
	waitChan := n.waitedSearchReply.createChan(requestID)
	defer n.waitedSearchReply.deleteChan(requestID)
	t := time.NewTimer(conf.Timeout)
	for {
		select {
		case responses := <-waitChan:
			// Search for a full match in responses
			for _, response := range responses {
				fullyKnown := true
				for _, chunk := range response.Chunks {
					if chunk == nil {
						fullyKnown = false
						break
					}
				}
				if fullyKnown {
					name = response.Name
					return
				}
			}
		case <-t.C:
			tries++
			if tries >= int(conf.Retry) {
				//err = xerrors.Errorf("search first failed: too many retries")
				return
			}
			budget = budget * conf.Factor
			// Retring needs a new requestID otherwise the other nodes will ignore it
			requestID = xid.New().String()
			// And therefore a new channel to wait on
			waitChan = n.waitedSearchReply.createChan(requestID)
			defer n.waitedSearchReply.deleteChan(requestID)
			err = n.Search(pattern, budget, requestID, n.id, getNeighbors(n.GetRoutingTable()))
			if err != nil {
				err = xerrors.Errorf("failed to search (first): %v", err)
				return
			}
			t.Reset(conf.Timeout)
		}
	}
}

func (searchesReceived *searchesReceived) alreadySearched(requestID string) bool {
	searchesReceived.Lock()
	defer searchesReceived.Unlock()
	_, ok := searchesReceived.searches[requestID]
	if !ok {
		searchesReceived.searches[requestID] = struct{}{}
	}
	return ok
}
