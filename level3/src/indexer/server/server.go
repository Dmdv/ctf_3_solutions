package server

import (
	"encoding/json"
	"indexer/index"
	"indexer/response"
	"io"
	"net/http"
)

type Server struct {
	index *index.Index
}

func (server *Server) HealthCheckHandler(w http.ResponseWriter, r *http.Request) {
	io.WriteString(w, `{"success": "true"}`)
}

func (server *Server) IndexHandler(w http.ResponseWriter, r *http.Request) {
	server.index.IndexPath(r.URL.Query().Get("path"))
}

func (server *Server) IsIndexedHandler(w http.ResponseWriter, r *http.Request) {
	if server.index.IsIndexed() {
		io.WriteString(w, `{"success": "true"}`)
	} else {
		io.WriteString(w, `{"success": "false"}`)
	}
}

func (server *Server) QueryHandler(w http.ResponseWriter, r *http.Request) {
	query := r.URL.Query().Get("q")
	matches := server.index.FindMatches(query)
	response := response.New(matches)
	body, _ := json.Marshal(response)

	w.Write(body)
}

func New() *Server {
	return &Server{
		index: index.New(),
	}
}
