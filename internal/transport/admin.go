package transport

import (
	"encoding/json"
	"net/http"
	"time"
)

// RegisterAdminHandlers exposes the chaos knobs used by
// scripts/fault_injection.sh against a real running cluster (Docker
// Compose or EC2), mirroring the same failure modes the in-process
// FakeNetwork drives in the unit tests:
//
//	POST /admin/crash              simulate `kill -9`: drop everything
//	POST /admin/recover            undo /admin/crash
//	POST /admin/partition {peers}  stop talking to the given peer ids
//	POST /admin/heal               undo /admin/partition
//	POST /admin/droprate {rate}    randomly fail a fraction of outbound RPCs
//	POST /admin/delay {ms}         add latency to outbound RPCs
//	GET  /admin/faults             current fault configuration
func RegisterAdminHandlers(mux *http.ServeMux, faults *FaultInjector) {
	mux.HandleFunc("/admin/crash", func(w http.ResponseWriter, r *http.Request) {
		faults.SetCrashed(true)
		w.Write([]byte("crashed\n"))
	})
	mux.HandleFunc("/admin/recover", func(w http.ResponseWriter, r *http.Request) {
		faults.SetCrashed(false)
		w.Write([]byte("recovered\n"))
	})
	mux.HandleFunc("/admin/partition", func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			Peers []string `json:"peers"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		faults.SetPartition(req.Peers, true)
		w.Write([]byte("partitioned\n"))
	})
	mux.HandleFunc("/admin/heal", func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			Peers []string `json:"peers"`
		}
		_ = json.NewDecoder(r.Body).Decode(&req)
		if len(req.Peers) == 0 {
			p, _, _, _ := faults.Snapshot()
			req.Peers = p
		}
		faults.SetPartition(req.Peers, false)
		w.Write([]byte("healed\n"))
	})
	mux.HandleFunc("/admin/droprate", func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			Rate float64 `json:"rate"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		faults.SetDropRate(req.Rate)
		w.Write([]byte("droprate set\n"))
	})
	mux.HandleFunc("/admin/delay", func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			Ms int `json:"ms"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		faults.SetDelay(time.Duration(req.Ms) * time.Millisecond)
		w.Write([]byte("delay set\n"))
	})
	mux.HandleFunc("/admin/faults", func(w http.ResponseWriter, r *http.Request) {
		partitioned, dropRate, delay, crashed := faults.Snapshot()
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]interface{}{
			"partitioned_from": partitioned,
			"drop_rate":        dropRate,
			"delay_ms":         delay.Milliseconds(),
			"crashed":          crashed,
		})
	})
}
