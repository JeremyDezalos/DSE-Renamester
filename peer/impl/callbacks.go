package impl

import (
	"errors"
	"math/rand"
	"regexp"
	"strings"
	"time"

	"go.dedis.ch/cs438/peer"
	"go.dedis.ch/cs438/transport"
	"go.dedis.ch/cs438/types"
	"golang.org/x/xerrors"
)

func (n *node) ExecChatMessage(msg types.Message, pkt transport.Packet) error {
	_, ok := msg.(*types.ChatMessage)
	if !ok {
		return xerrors.Errorf("wrong type: %T", msg)
	}

	return nil
}

func (n *node) ExecRumorsMessage(msg types.Message, pkt transport.Packet) error {
	selfAddr := n.conf.Socket.GetAddress()
	rumorsMessage, ok := msg.(*types.RumorsMessage)
	if !ok {
		return xerrors.Errorf("wrong type: %T", msg)
	}

	// Add expected rumors to collection and process them
	acceptedRumors := n.rumorsCollection.addRumors(rumorsMessage.Rumors)
	for _, v := range acceptedRumors {
		newHeader := transport.NewHeader(pkt.Header.Source, pkt.Header.RelayedBy, pkt.Header.Destination, 0)
		newPkt := transport.Packet{
			Header: &newHeader,
			Msg:    v.Msg,
		}
		err := n.conf.MessageRegistry.ProcessPacket(newPkt)
		if err != nil {
			return xerrors.Errorf("failed to process one of the expected rumor: %v", err)
		}
		// Update routing table
		relay, _ := n.lockedRoutingTable.get(v.Origin)
		// Don't remove neighbors!
		if relay != v.Origin {
			n.SetRoutingEntry(v.Origin, pkt.Header.RelayedBy)
		}
	}

	// Acknowlegment
	statusMsg := n.rumorsCollection.generateStatusMessage()
	ack := types.AckMessage{
		AckedPacketID: pkt.Header.PacketID,
		Status:        statusMsg,
	}
	ackHeader := transport.NewHeader(selfAddr, selfAddr, pkt.Header.Source, 0)
	ackMsg, err := n.conf.MessageRegistry.MarshalMessage(&ack)

	if err != nil {
		return xerrors.Errorf("failed to marshal ack message: %v", err)
	}
	ackPkt := transport.Packet{
		Header: &ackHeader,
		Msg:    &ackMsg,
	}
	// Weird (bypassing routing table) but expected way to send acknowlegment
	err = n.conf.Socket.Send(pkt.Header.Source, ackPkt, time.Second*1)
	if errors.Is(err, transport.TimeoutErr(0)) {
		return xerrors.Errorf("failed to send packet (timeout): %v", err)
	} else if err != nil {
		return xerrors.Errorf("failed to send packet: %v", err)
	}

	// Forward the RumorMessage to another random neighbor if one of the rumor was expected
	if len(acceptedRumors) > 0 {

		randomNeighbor := getRandomNeighbor(n.GetRoutingTable(), selfAddr, pkt.Header.Source)
		if randomNeighbor != "" {
			rumorsForwardHeader := transport.NewHeader(selfAddr, selfAddr, randomNeighbor, 0)
			rumorsForwardMsg, err := n.conf.MessageRegistry.MarshalMessage(rumorsMessage)
			if err != nil {
				return xerrors.Errorf("failed to marshal rumors forward message: %v", err)
			}
			rumorsForwardPkt := transport.Packet{
				Header: &rumorsForwardHeader,
				Msg:    &rumorsForwardMsg,
			}
			err = n.sendPacket(rumorsForwardPkt)
			if err != nil {
				return err
			}
		}
	}

	return nil
}

func (n *node) ExecStatusMessage(msg types.Message, pkt transport.Packet) error {
	selfAddr := n.conf.Socket.GetAddress()
	remoteStatus, ok := msg.(*types.StatusMessage)
	if !ok {
		return xerrors.Errorf("wrong type: %T", msg)
	}
	selfStatus := n.rumorsCollection.generateStatusMessage()

	// Check if we need update, if we do, send status message
	needUpdate := false
	for rk, rv := range *remoteStatus {
		sv, ok := selfStatus[rk]
		if ok && rv > sv {
			needUpdate = true
		} else if !ok {
			needUpdate = true
		}
	}

	if needUpdate {
		sendStatusHeader := transport.NewHeader(selfAddr, selfAddr, pkt.Header.Source, 0)
		selfStatusMsg, err := n.conf.MessageRegistry.MarshalMessage(&selfStatus)
		if err != nil {
			return xerrors.Errorf("failed to marshal status message (ask for update): %v", err)
		}
		sendStatusPkt := transport.Packet{
			Header: &sendStatusHeader,
			Msg:    &selfStatusMsg,
		}

		// Weird (bypassing routing table) but expected way to send status update
		err = n.conf.Socket.Send(pkt.Header.Source, sendStatusPkt, time.Second*1)
		if errors.Is(err, transport.TimeoutErr(0)) {
			return xerrors.Errorf("failed to send packet (timeout): %v", err)
		} else if err != nil {
			return xerrors.Errorf("failed to send packet: %v", err)
		}
	}

	// For each peer store by how much remote need to catch up
	missingRumors := make([]types.Rumor, 0)
	for sk, sv := range selfStatus {
		rv, ok := (*remoteStatus)[sk]
		if ok {
			if sv > rv {
				missingRumors = append(missingRumors, n.rumorsCollection.getRumors(sk, rv)...)
			}
		} else {
			missingRumors = append(missingRumors, n.rumorsCollection.getRumors(sk, 1)...)
		}
	}
	if len(missingRumors) > 0 {

		missingRumorsMsg := types.RumorsMessage{
			Rumors: missingRumors,
		}
		missingHeader := transport.NewHeader(selfAddr, selfAddr, pkt.Header.Source, 0)
		missingMsg, err := n.conf.MessageRegistry.MarshalMessage(&missingRumorsMsg)
		if err != nil {
			return xerrors.Errorf("failed to marshal missing rumors message: %v", err)
		}
		missingPkt := transport.Packet{
			Header: &missingHeader,
			Msg:    &missingMsg,
		}
		// Weird (bypassing routing table) but expected way to send status update
		err = n.conf.Socket.Send(pkt.Header.Source, missingPkt, time.Second*1)
		if errors.Is(err, transport.TimeoutErr(0)) {
			return xerrors.Errorf("failed to send packet (timeout): %v", err)
		} else if err != nil {
			return xerrors.Errorf("failed to send packet: %v", err)
		}
	}

	// ContinueMongering
	if !needUpdate && len(missingRumors) == 0 {
		if rand.Float64() < n.conf.ContinueMongering {
			randNeighbor := getRandomNeighbor(n.GetRoutingTable(), selfAddr, pkt.Header.Source)
			if randNeighbor != "" {
				mongeringHeader := transport.NewHeader(selfAddr, selfAddr, randNeighbor, 0)
				mongeringMsg, err := n.conf.MessageRegistry.MarshalMessage(&selfStatus)
				if err != nil {
					return xerrors.Errorf("failed to marshal ContinueMongering (status) message: %v", err)
				}
				mongeringPkt := transport.Packet{
					Header: &mongeringHeader,
					Msg:    &mongeringMsg,
				}
				err = n.sendPacket(mongeringPkt)
				if err != nil {
					return err
				}
			}
		}
	}

	return nil
}

func (n *node) ExecAckMessage(msg types.Message, pkt transport.Packet) error {
	ackMessage, ok := msg.(*types.AckMessage)
	if !ok {
		return xerrors.Errorf("wrong type: %T", msg)
	}

	pktID := ackMessage.AckedPacketID
	n.waitedAck.chanNotify(pktID)

	statusMsg, err := n.conf.MessageRegistry.MarshalMessage(&ackMessage.Status)
	if err != nil {
		return xerrors.Errorf("failed to marshal status message (from ack): %v", err)
	}
	statusHeader := transport.NewHeader(pkt.Header.Source, pkt.Header.RelayedBy, pkt.Header.Destination, 0)
	statusPkt := transport.Packet{
		Header: &statusHeader,
		Msg:    &statusMsg,
	}
	err = n.conf.MessageRegistry.ProcessPacket(statusPkt)
	if err != nil {
		return xerrors.Errorf("failed to process status packet (from ack): %v", err)
	}
	return nil
}

func (n *node) ExecEmptyMessage(msg types.Message, pkt transport.Packet) error {
	_, ok := msg.(*types.EmptyMessage)
	if !ok {
		return xerrors.Errorf("wrong type: %T", msg)
	}
	return nil
}

func (n *node) ExecPrivateMessage(msg types.Message, pkt transport.Packet) error {
	privateMessage, ok := msg.(*types.PrivateMessage)
	if !ok {
		return xerrors.Errorf("wrong type: %T", msg)
	}

	_, ok = privateMessage.Recipients[n.conf.Socket.GetAddress()]
	if ok {
		privateHeader := transport.NewHeader(pkt.Header.Source, pkt.Header.RelayedBy, pkt.Header.Destination, 0)
		privatePkt := transport.Packet{
			Header: &privateHeader,
			Msg:    privateMessage.Msg,
		}

		err := n.conf.MessageRegistry.ProcessPacket(privatePkt)
		if err != nil {
			return xerrors.Errorf("failed to process private message: %v", err)
		}
	}
	return nil
}

func (n *node) ExecDataRequestMessage(msg types.Message, pkt transport.Packet) error {
	self := n.conf.Socket.GetAddress()
	dataRequestMessage, ok := msg.(*types.DataRequestMessage)
	if !ok {
		return xerrors.Errorf("wrong type: %T", msg)
	}
	blob := n.conf.Storage.GetDataBlobStore()
	data := blob.Get(dataRequestMessage.Key)
	dataReply := types.DataReplyMessage{
		RequestID: dataRequestMessage.RequestID,
		Key:       dataRequestMessage.Key,
		Value:     data,
	}
	dataReplyMsg, err := n.conf.MessageRegistry.MarshalMessage(&dataReply)
	if err != nil {
		return xerrors.Errorf("failed to marshal data reply message: %v", err)
	}
	dataReplyHeader := transport.NewHeader(self, self, pkt.Header.Source, 0)
	dataReplyPkt := transport.Packet{
		Header: &dataReplyHeader,
		Msg:    &dataReplyMsg,
	}
	err = n.sendPacket(dataReplyPkt)
	if err != nil {
		return xerrors.Errorf("failed to send data reply message: %v", err)
	}

	return nil
}

func (n *node) ExecDataReplyMessage(msg types.Message, pkt transport.Packet) error {
	dataReplyMessage, ok := msg.(*types.DataReplyMessage)
	if !ok {
		return xerrors.Errorf("wrong type: %T", msg)
	}
	n.waitedDataReply.chanNotify(dataReplyMessage.RequestID, dataReplyMessage.Value)
	return nil
}

func (n *node) ExecSearchRequestMessage(msg types.Message, pkt transport.Packet) error {
	self := n.conf.Socket.GetAddress()
	searchRequestMessage, ok := msg.(*types.SearchRequestMessage)
	if !ok {
		return xerrors.Errorf("wrong type: %T", msg)
	}
	// Peers need to detect if they receive a duplicate search request and ignore it.
	// Each request contains a unique identifier that allows peers to identify them.
	if n.alreadySearched(searchRequestMessage.RequestID) {
		return nil
	}
	neighbors := getNeighbors(n.GetRoutingTable())
	delete(neighbors, pkt.Header.Source)
	// Forward search
	reg := *regexp.MustCompile(searchRequestMessage.Pattern)
	err := n.Search(reg, searchRequestMessage.Budget-1, searchRequestMessage.RequestID, searchRequestMessage.Origin, neighbors)
	if err != nil {
		return xerrors.Errorf("failed to forward search: %v", err)
	}
	// Search locally to send back
	naming := n.conf.Storage.GetNamingStore()
	namesToHash := make(map[string]string)
	naming.ForEach(func(key string, val []byte) bool {
		validName := reg.FindString(key)
		if validName != "" {
			namesToHash[key] = string(val)
		}
		return true
	})
	fileInfos := make([]types.FileInfo, 0)
	blob := n.conf.Storage.GetDataBlobStore()
	for name, metahash := range namesToHash {
		metafile := blob.Get(metahash)
		if metafile != nil {
			fChunks := strings.Split(string(metafile), peer.MetafileSep)
			chunks := make([][]byte, len(fChunks))
			for i := range fChunks {
				// We have to check presence
				chunks[i] = blob.Get(fChunks[i])
				// But we want the chunks keys not value;
				if chunks[i] != nil {
					chunks[i] = []byte(fChunks[i])
				}
			}
			fileInfo := types.FileInfo{
				Name:     name,
				Metahash: metahash,
				Chunks:   chunks,
			}
			fileInfos = append(fileInfos, fileInfo)
		}
	}
	searchReply := types.SearchReplyMessage{
		RequestID: searchRequestMessage.RequestID,
		Responses: fileInfos,
	}
	searchReplyMsg, err := n.conf.MessageRegistry.MarshalMessage(&searchReply)
	if err != nil {
		return xerrors.Errorf("failed to marshal search reply message: %v", err)
	}
	searchReplyHeader := transport.NewHeader(self, self, searchRequestMessage.Origin, 0)
	searchReplyPkt := transport.Packet{
		Header: &searchReplyHeader,
		Msg:    &searchReplyMsg,
	}
	// "The reply must be directly sent to the packet’s source"
	err = n.conf.Socket.Send(pkt.Header.Source, searchReplyPkt, time.Second*1)
	if errors.Is(err, transport.TimeoutErr(0)) {
		return xerrors.Errorf("failed to send search reply packet (timeout): %v", err)
	} else if err != nil {
		return xerrors.Errorf("failed to send search reply packet: %v", err)
	}

	return nil
}

func (n *node) ExecSearchReplyMessage(msg types.Message, pkt transport.Packet) error {
	searchReplyMessage, ok := msg.(*types.SearchReplyMessage)
	if !ok {
		return xerrors.Errorf("wrong type: %T", msg)
	}
	for _, response := range searchReplyMessage.Responses {

		err := n.Tag(response.Name, response.Metahash)
		if err != nil {
			// Not fatal
			log.Warn().Msgf("failed to add tag received from research: %v", err)
		}
		n.UpdateCatalog(response.Metahash, pkt.Header.Source)
		for _, chunk := range response.Chunks {
			if chunk != nil {
				n.UpdateCatalog(string(chunk), pkt.Header.Source)
			}
		}
	}
	n.waitedSearchReply.chanNotify(searchReplyMessage.RequestID, searchReplyMessage.Responses)
	return nil
}

func (n *node) ExecPaxosPrepareMessage(msg types.Message, pkt transport.Packet) error {
	paxosPrepareMessage, ok := msg.(*types.PaxosPrepareMessage)
	if !ok {
		return xerrors.Errorf("wrong type: %T", msg)
	}
	err := n.paxos.acceptor.promise(*paxosPrepareMessage, n)
	if err != nil {
		return xerrors.Errorf("failed promise: %v", err)
	}
	return nil
}

func (n *node) ExecPaxosPromiseMessage(msg types.Message, pkt transport.Packet) error {
	paxosPromiseMessage, ok := msg.(*types.PaxosPromiseMessage)
	if !ok {
		return xerrors.Errorf("wrong type: %T", msg)
	}

	n.paxos.collectPromise(*paxosPromiseMessage, n)
	return nil
}

func (n *node) ExecPaxosProposeMessage(msg types.Message, pkt transport.Packet) error {
	paxosProposeMessage, ok := msg.(*types.PaxosProposeMessage)
	if !ok {
		return xerrors.Errorf("wrong type: %T", msg)
	}

	err := n.paxos.acceptor.accept(*paxosProposeMessage, n)
	if err != nil {
		return xerrors.Errorf("failed accept: %v", err)
	}
	return nil
}

func (n *node) ExecPaxosAcceptMessage(msg types.Message, pkt transport.Packet) error {
	paxosAcceptMessage, ok := msg.(*types.PaxosAcceptMessage)
	if !ok {
		return xerrors.Errorf("wrong type: %T", msg)
	}

	if n.paxos.status.instanceRunning() && n.paxos.proposer.phase.get() == 2 {
		n.paxos.proposer.collectAccept(*paxosAcceptMessage, n)
	} else {
		err := n.paxos.collectAccept(*paxosAcceptMessage, n)
		if err != nil {
			return xerrors.Errorf("failed to process paxos accept message: %v", err)
		}
	}

	return nil
}

func (n *node) ExecTLCMessage(msg types.Message, pkt transport.Packet) error {
	tlcMessage, ok := msg.(*types.TLCMessage)
	if !ok {
		return xerrors.Errorf("wrong type: %T", msg)
	}

	err := n.paxos.collectTLC(*tlcMessage, n)
	if err != nil {
		return xerrors.Errorf("failed to process TLC message: %v", err)
	}
	return nil
}
