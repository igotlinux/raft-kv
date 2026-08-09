package kv

import (
	"encoding/json"
	"net/http"

	"raftkv/internal/raft"
)

// HTTPAPI is the client-facing surface: plain HTTP+JSON so it's curl-able
// for demos, chaos scripts, and interviews alike.
type HTTPAPI struct {
	store *Store
	node  *raft.Node
	peers map[string]string // node id -> client-facing "host:port", for redirect hints
}

func NewHTTPAPI(store *Store, node *raft.Node, clientPeers map[string]string) *HTTPAPI {
	return &HTTPAPI{store: store, node: node, peers: clientPeers}
}

func (a *HTTPAPI) Register(mux *http.ServeMux) {
	mux.HandleFunc("/kv/put", a.handlePut)
	mux.HandleFunc("/kv/get", a.handleGet)
	mux.HandleFunc("/kv/delete", a.handleDelete)
	mux.HandleFunc("/status", a.handleStatus)
}

type putRequest struct {
	Key   string `json:"key"`
	Value string `json:"value"`
}

type errorResponse struct {
	Error      string `json:"error"`
	LeaderHint string `json:"leader_hint,omitempty"`
}

func (a *HTTPAPI) writeNotLeader(w http.ResponseWriter, hint string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusMisdirectedRequest) // 421: "you talked to the wrong node"
	addr := a.peers[hint]
	json.NewEncoder(w).Encode(errorResponse{Error: "not leader", LeaderHint: addr})
}

func (a *HTTPAPI) handlePut(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "POST required", http.StatusMethodNotAllowed)
		return
	}
	var req putRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.Key == "" {
		http.Error(w, "invalid body, expect {\"key\":..,\"value\":..}", http.StatusBadRequest)
		return
	}
	if err := a.store.Put(req.Key, req.Value); err != nil {
		if nl, ok := err.(*ErrNotLeader); ok {
			a.writeNotLeader(w, nl.LeaderHint)
			return
		}
		http.Error(w, err.Error(), http.StatusGatewayTimeout)
		return
	}
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(map[string]string{"status": "ok"})
}

func (a *HTTPAPI) handleDelete(w http.ResponseWriter, r *http.Request) {
	key := r.URL.Query().Get("key")
	if key == "" {
		http.Error(w, "missing ?key=", http.StatusBadRequest)
		return
	}
	if err := a.store.Delete(key); err != nil {
		if nl, ok := err.(*ErrNotLeader); ok {
			a.writeNotLeader(w, nl.LeaderHint)
			return
		}
		http.Error(w, err.Error(), http.StatusGatewayTimeout)
		return
	}
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(map[string]string{"status": "ok"})
}

func (a *HTTPAPI) handleGet(w http.ResponseWriter, r *http.Request) {
	key := r.URL.Query().Get("key")
	if key == "" {
		http.Error(w, "missing ?key=", http.StatusBadRequest)
		return
	}
	v, ok := a.store.Get(key)
	w.Header().Set("Content-Type", "application/json")
	if !ok {
		w.WriteHeader(http.StatusNotFound)
		json.NewEncoder(w).Encode(errorResponse{Error: "key not found"})
		return
	}
	json.NewEncoder(w).Encode(map[string]string{"key": key, "value": v})
}

func (a *HTTPAPI) handleStatus(w http.ResponseWriter, r *http.Request) {
	id, state, term, leaderID, commitIndex, logLen := a.node.Status()
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{
		"id":           id,
		"state":        state.String(),
		"term":         term,
		"leader_id":    leaderID,
		"commit_index": commitIndex,
		"log_len":      logLen,
	})
}
