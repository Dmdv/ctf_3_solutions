package main

import (
	"github.com/gorilla/pat"
	"indexer/server"
	"net/http"
)

func main() {
	server := server.New()

	r := pat.New()
	r.Get("/healthcheck", server.HealthCheckHandler)
	r.Get("/index", server.IndexHandler)
	r.Get("/isIndexed", server.IsIndexedHandler)
	r.Get("/", server.QueryHandler)

	http.Handle("/", r)
	http.ListenAndServe(":9090", nil)
}
