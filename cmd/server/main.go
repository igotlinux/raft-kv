// Command server runs one node of the Raft-backed KV cluster. Every
// piece of cluster topology comes from environment variables so the same
// binary/image runs unmodified whether it's under `go run`, Docker
// Compose, or on an EC2 instance -- only the env differs.
//
// Required env vars:
//
//	NODE_ID            this node's id, must be a key in PEERS (e.g. "node1")
//	PEERS              comma-separated id=host:port for the RAFT-INTERNAL
//	                    port on every node, including self
//	                    e.g. "node1=node1:7000,node2=node2:7000,node3=node3:7000"
//	CLIENT_ADDRS       same shape, but the CLIENT-FACING port on every
//	                    node; used only to build "talk to the leader at
//	                    ___" redirect hints
//	DATA_DIR           directory for this node's WAL + state file
//
// Optional:
//
//	RAFT_LISTEN_ADDR   defaults to ":7000"
//	CLIENT_LISTEN_ADDR defaults to ":8000"
package main

import (
	"fmt"
	"log"
	"net/http"
	"os"
	"strings"

	"raftkv/internal/kv"
	"raftkv/internal/raft"
	"raftkv/internal/transport"
)

func mustEnv(key string) string {
	v := os.Getenv(key)
	if v == "" {
		log.Fatalf("missing required env var %s", key)
	}
	return v
}

func envOr(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

// parsePeerMap turns "id1=addr1,id2=addr2" into a map.
func parsePeerMap(s string) transport.PeerMap {
	m := make(transport.PeerMap)
	for _, pair := range strings.Split(s, ",") {
		pair = strings.TrimSpace(pair)
		if pair == "" {
			continue
		}
		kvpair := strings.SplitN(pair, "=", 2)
		if len(kvpair) != 2 {
			log.Fatalf("malformed peer entry %q (want id=host:port)", pair)
		}
		m[kvpair[0]] = kvpair[1]
	}
	return m
}

func main() {
	nodeID := mustEnv("NODE_ID")
	raftPeers := parsePeerMap(mustEnv("PEERS"))
	clientPeers := parsePeerMap(mustEnv("CLIENT_ADDRS"))
	dataDir := mustEnv("DATA_DIR")
	raftListen := envOr("RAFT_LISTEN_ADDR", ":7000")
	clientListen := envOr("CLIENT_LISTEN_ADDR", ":8000")

	if _, ok := raftPeers[nodeID]; !ok {
		log.Fatalf("NODE_ID %q not present in PEERS", nodeID)
	}

	var ids []string
	for id := range raftPeers {
		ids = append(ids, id)
	}

	persister, err := raft.NewPersister(dataDir)
	if err != nil {
		log.Fatalf("persister: %v", err)
	}

	applyCh := make(chan raft.ApplyMsg, 256)
	faults := transport.NewFaultInjector()
	tr := transport.NewHTTPTransport(nodeID, raftPeers, faults)

	logf := func(format string, args ...interface{}) {
		log.Printf(format, args...)
	}
	node, err := raft.NewNode(nodeID, ids, tr, persister, applyCh, logf)
	if err != nil {
		log.Fatalf("raft.NewNode: %v", err)
	}

	store := kv.NewStore(node)
	go store.Run(applyCh)

	node.Run()

	// Internal cluster RPC server (Raft only, plus admin fault controls
	// live here too since they govern this node's outbound RPCs).
	raftMux := http.NewServeMux()
	transport.RegisterRaftHandlers(raftMux, node, faults)
	transport.RegisterAdminHandlers(raftMux, faults)
	go func() {
		log.Printf("node %s: raft/admin listening on %s", nodeID, raftListen)
		if err := http.ListenAndServe(raftListen, raftMux); err != nil {
			log.Fatalf("raft server: %v", err)
		}
	}()

	// Client-facing KV API.
	clientMux := http.NewServeMux()
	api := kv.NewHTTPAPI(store, node, clientPeers)
	api.Register(clientMux)
	log.Printf("node %s: client API listening on %s (peers: %s)", nodeID, clientListen, fmt.Sprint(ids))
	if err := http.ListenAndServe(clientListen, clientMux); err != nil {
		log.Fatalf("client server: %v", err)
	}
}
