package bridge

import (
	"encoding/json"
	"net/http"
	"os"
	"path/filepath"
	"sort"
)

// bundleFiles are the files exposed over HTTP for each call.
var bundleFiles = map[string]bool{
	"audio.wav":    true,
	"meta.json":    true,
	"events.jsonl": true,
}

// RegisterRoutes mounts the recording fetch API on mux:
//
//	GET    /recordings                  list finished bundles (meta.json each)
//	GET    /recordings/{uuid}/{file}    audio.wav | meta.json | events.jsonl
//	DELETE /recordings/{uuid}           remove a bundle
func (r *Recorder) RegisterRoutes(mux *http.ServeMux) {
	mux.HandleFunc("GET /recordings", r.handleList)
	mux.HandleFunc("GET /recordings/{uuid}/{file}", r.handleFile)
	mux.HandleFunc("DELETE /recordings/{uuid}", r.handleDelete)
}

func (r *Recorder) handleList(w http.ResponseWriter, req *http.Request) {
	entries, err := os.ReadDir(r.Dir)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	metas := []json.RawMessage{}
	for _, e := range entries {
		if !e.IsDir() {
			continue
		}
		data, err := os.ReadFile(filepath.Join(r.Dir, e.Name(), "meta.json"))
		if err != nil {
			continue // in-progress or incomplete bundle
		}
		metas = append(metas, json.RawMessage(data))
	}
	// Newest first.
	sort.Slice(metas, func(i, j int) bool {
		return string(metas[i]) > string(metas[j]) // cheap proxy; metas contain ISO timestamps
	})
	w.Header().Set("Content-Type", "application/json")
	out, _ := json.Marshal(metas)
	_, _ = w.Write(out)
}

func (r *Recorder) handleFile(w http.ResponseWriter, req *http.Request) {
	uuid := sanitizeName(req.PathValue("uuid"))
	file := req.PathValue("file")
	if uuid == "" || !bundleFiles[file] {
		http.Error(w, "not found", http.StatusNotFound)
		return
	}
	path := filepath.Join(r.Dir, uuid, file)
	if file == "audio.wav" {
		w.Header().Set("Content-Type", "audio/wav")
	} else if file == "meta.json" {
		w.Header().Set("Content-Type", "application/json")
	} else {
		w.Header().Set("Content-Type", "application/x-ndjson")
	}
	http.ServeFile(w, req, path)
}

func (r *Recorder) handleDelete(w http.ResponseWriter, req *http.Request) {
	uuid := sanitizeName(req.PathValue("uuid"))
	if uuid == "" {
		http.Error(w, "not found", http.StatusNotFound)
		return
	}
	dir := filepath.Join(r.Dir, uuid)
	if _, err := os.Stat(dir); err != nil {
		http.Error(w, "not found", http.StatusNotFound)
		return
	}
	if err := os.RemoveAll(dir); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}
