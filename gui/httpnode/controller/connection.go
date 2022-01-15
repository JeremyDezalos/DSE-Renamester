package controller

import (
	"net/http"

	"github.com/rs/zerolog"
	"go.dedis.ch/cs438/peer"
)

// NewServiceCtrl returns a new initialized service controller.
func NewConnectionStatusCtrl(node peer.Peer, initialAddr string, socketctrl *socketctrl, log *zerolog.Logger) deco_reco {
	return deco_reco{
		node:        node,
		initialAddr: initialAddr,
		socketctrl:  socketctrl,
		log:         log,
	}
}

type deco_reco struct {
	node        peer.Peer
	initialAddr string
	socketctrl  *socketctrl
	log         *zerolog.Logger
}

func (s deco_reco) ConnectionStatusHandler() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodPost:
			s.connectionStatusPost(w, r)
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

func (s deco_reco) PrivateKeyHandler() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			http.Error(w, "forbidden method", http.StatusMethodNotAllowed)
			return
		}
		s.getPrivateKey(w, r)
	}
}

func (s deco_reco) connectionStatusPost(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Access-Control-Allow-Origin", "*")
	w.Header().Set("Access-Control-Allow-Headers", "*")

	if s.node.GetConnectionStatus() {
		err := s.node.Disconnect()
		if err != nil {
			http.Error(w, "failed to disconnect: "+err.Error(), http.StatusBadRequest)
			return
		}
		w.Write([]byte("disconnected"))
	} else {
		socket, err := s.node.Reconnect(s.initialAddr + ":0")
		if err != nil {
			http.Error(w, "failed to reconnect: "+err.Error(), http.StatusBadRequest)
			return
		}
		s.socketctrl.updateSocket(socket)
		s.log.Info().Msgf("new addr: %s\n", socket.GetAddress())
		w.Write([]byte("connected"))
	}
}

func (s deco_reco) getPrivateKey(w http.ResponseWriter, r *http.Request) {
	key := s.node.GetPrivateKey()
	w.Header().Set("Access-Control-Allow-Origin", "*")
	w.Header().Set("Access-Control-Allow-Headers", "*")

	w.Write([]byte(key))
}
