package controller

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"

	"github.com/rs/zerolog"
	"go.dedis.ch/cs438/gui/httpnode/types"
	"go.dedis.ch/cs438/peer"

	"go.dedis.ch/cs438/transport"
)

// NewMessaging returns a new initialized messaging.
func NewMessaging(node peer.Peer, log *zerolog.Logger) messaging {
	return messaging{
		node: node,
		log:  log,
	}
}

type messaging struct {
	node peer.Peer
	log  *zerolog.Logger
}

func (m messaging) PeerHandler() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodPost:
			m.peerPost(w, r)
		case http.MethodOptions:
			w.Header().Set("Access-Control-Allow-Origin", "*")
			w.Header().Set("Access-Control-Allow-Headers", "*")
			return
		default:
			http.Error(w, "forbidden method", http.StatusMethodNotAllowed)
			return
		}
	}
}

func (m messaging) RoutingHandler() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodGet:
			m.routingGet(w, r)
		case http.MethodPost:
			m.routingPost(w, r)
		case http.MethodOptions:
			w.Header().Set("Access-Control-Allow-Origin", "*")
			w.Header().Set("Access-Control-Allow-Headers", "*")
			return
		default:
			http.Error(w, "forbidden method", http.StatusMethodNotAllowed)
			return
		}
	}
}

func (m messaging) UnicastHandler() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodPost:
			m.unicastPost(w, r)
		case http.MethodOptions:
			w.Header().Set("Access-Control-Allow-Origin", "*")
			w.Header().Set("Access-Control-Allow-Headers", "*")
			return
		default:
			http.Error(w, "forbidden method", http.StatusMethodNotAllowed)
			return
		}
	}
}

func (m messaging) BroadcastHandler() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodPost:
			m.broadcastPost(w, r)
		case http.MethodOptions:
			w.Header().Set("Access-Control-Allow-Origin", "*")
			w.Header().Set("Access-Control-Allow-Headers", "*")
			return
		default:
			http.Error(w, "forbidden method", http.StatusMethodNotAllowed)
			return
		}
	}
}

// [
// 	 "127.0.0.1:xxx",
// 	 "127.0.0.1:yyy"
// ]
func (m messaging) peerPost(w http.ResponseWriter, r *http.Request) {
	buf, err := ioutil.ReadAll(r.Body)
	if err != nil {
		http.Error(w, "failed to read body: "+err.Error(), http.StatusInternalServerError)
		return
	}

	res := types.AddPeerArgument{}
	err = json.Unmarshal(buf, &res)
	if err != nil {
		http.Error(w, "failed to unmarshal addPeerArgument: "+err.Error(),
			http.StatusInternalServerError)
		return
	}

	m.log.Info().Msgf("got the following peers: %v", res)

	w.Header().Set("Access-Control-Allow-Origin", "*")
	w.Header().Set("Access-Control-Allow-Headers", "*")

	err = m.node.AddPeer(res...)
	if err != nil {
		http.Error(w, "failed to add peers: "+err.Error(), http.StatusInternalServerError)
	}
}

func (m messaging) routingGet(w http.ResponseWriter, r *http.Request) {
	table := m.node.GetRoutingTable()
	neighors := m.node.GetNeighborsTable()

	// Create go object easily convertible to JSON
	goToJson := struct {
		N peer.RoutingTable
		T map[string]string
	}{neighors, table}

	err := r.ParseForm()
	if err != nil {
		http.Error(w, fmt.Sprintf("failed tp parse form: %v", err),
			http.StatusInternalServerError)
	}

	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Access-Control-Allow-Origin", "*")

	if r.Form.Get("graphviz") == "on" {
		table.DisplayGraph(w)
	} else {
		// debugBuffer := new(bytes.Buffer)
		// debugEnc := json.NewEncoder(debugBuffer)
		// debugEnc.SetIndent("", "\t")
		// err = debugEnc.Encode(&neighors)
		// if err != nil {
		// 	m.log.Info().Msgf("Encoding failed uwu\n")
		// }
		// fmt.Print(debugBuffer.String())
		// err = debugEnc.Encode(&table)
		// if err != nil {
		// 	m.log.Info().Msgf("Encoding failed step 2 uwu\n")
		// }
		// fmt.Print(debugBuffer.String())

		enc := json.NewEncoder(w)
		enc.SetIndent("", "\t")

		err = enc.Encode(&goToJson)
		if err != nil {
			http.Error(w, "failed to marshal routing table", http.StatusInternalServerError)
			return
		}
	}
}

// types.SetRoutingEntryArgument:
// {
//     "Origin": "XXX",
//     "RelayAddr": "XXX"
// }
func (m messaging) routingPost(w http.ResponseWriter, r *http.Request) {
	buf, err := ioutil.ReadAll(r.Body)
	if err != nil {
		http.Error(w, "failed to read body: "+err.Error(), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Access-Control-Allow-Origin", "*")

	m.log.Info().Msgf("got the following message: %s", buf)

	res := types.SetRoutingEntryArgument{}
	err = json.Unmarshal(buf, &res)
	if err != nil {
		http.Error(w, "failed to unmarshal addPeerArgument: "+err.Error(),
			http.StatusInternalServerError)
		return
	}

	m.log.Info().Msgf("got the following message: %v", res)

	m.node.SetRoutingEntry(res.Origin, res.RelayAddr)
}

// "Msg" can be of any type, based on "Type". Here a chat message.
//
// {
//     "Dest": "127.0.0.1:xxxx",
//     "Type": "chat",
//     "Msg": {
//         "Message": "Hello, world"
//     }
// }
func (m messaging) unicastPost(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Access-Control-Allow-Origin", "*")
	w.Header().Set("Access-Control-Allow-Headers", "*")

	buf, err := ioutil.ReadAll(r.Body)
	if err != nil {
		http.Error(w, "failed to read body: "+err.Error(), http.StatusInternalServerError)
		return
	}

	m.log.Info().Msgf("got the following message: %s", buf)

	res := types.UnicastArgument{}
	err = json.Unmarshal(buf, &res)
	if err != nil {
		http.Error(w, "failed to unmarshal unicast argument: "+err.Error(),
			http.StatusInternalServerError)
		return
	}

	err = m.node.Unicast(res.Dest, res.Msg)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
	}
}

func (m messaging) broadcastPost(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Access-Control-Allow-Origin", "*")
	w.Header().Set("Access-Control-Allow-Headers", "*")

	buf, err := ioutil.ReadAll(r.Body)
	if err != nil {
		http.Error(w, "failed to read body: "+err.Error(), http.StatusInternalServerError)
		return
	}

	m.log.Info().Msgf("broadcast got the following message: %s", buf)

	res := transport.Message{}
	err = json.Unmarshal(buf, &res)
	if err != nil {
		http.Error(w, "failed to unmarshal broadcast argument: "+err.Error(),
			http.StatusInternalServerError)
		return
	}

	err = m.node.Broadcast(res)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
	}
}
