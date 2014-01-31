package server

import (
	"errors"
	"fmt"
	"github.com/goraft/raft"
	"github.com/gorilla/mux"
	"io"
	"io/ioutil"
	"net/http"
	"path/filepath"
	"strings"
	"stripe-ctf.com/sqlcluster/log"
	"stripe-ctf.com/sqlcluster/sql"
	"stripe-ctf.com/sqlcluster/transport"
	"stripe-ctf.com/sqlcluster/util"
)

type Server struct {
	name       string
	path       string
	listen     string
	router     *mux.Router
	httpServer *http.Server
	sql        *sql.SQL
	client     *transport.Client
	raftServer raft.Server
	connectionString string
	sqlCache map[string][]byte
	sequenceNumber int
}

type SqlCommand struct {
	Query []byte `json:"query"`
}

func NewSqlCommand(query []byte) *SqlCommand {
	return &SqlCommand{
		Query: query,
	}
}

func (c *SqlCommand) CommandName() string {
	return "sql"
}

func (c *SqlCommand) Apply(server raft.Server) (interface{}, error) {
	s := server.Context().(*Server)

	sql := s.sql
	cache := s.sqlCache

	query := c.Query

	output, err := sql.Execute(server.State(), string(query))

	if err != nil {
		var msg string
		if output != nil && len(output.Stderr) > 0 {
			template := `Error executing %#v (%s)

			SQLite error: %s`
			msg = fmt.Sprintf(template, query, err.Error(), util.FmtOutput(output.Stderr))
		} else {
			msg = err.Error()
		}

		return nil, errors.New(msg)
	}

	formatted := []byte(fmt.Sprintf("SequenceNumber: %d\n%s", output.SequenceNumber, output.Stdout))

	s.sequenceNumber += 1

	cache[string(query)] = formatted

	return formatted, nil
}

// Creates a new server.
func New(path, listen string) (*Server, error) {
	log.Printf("Listen string %s", listen)

	cs, err := transport.Encode(listen)
	if err != nil {
		return nil, err
	}

	sqlPath := filepath.Join(path, "storage.sql")
	util.EnsureAbsent(sqlPath)
	pathParts := strings.Split(path, "/")

	s := &Server{
		path:    path,
		listen:  listen,
		sql:     sql.NewSQL(sqlPath),
		router:  mux.NewRouter(),
		client:  transport.NewClient(),
		name: pathParts[3],
		connectionString: cs,
		sqlCache: make(map[string][]byte),
		sequenceNumber: 0,
	}

	return s, nil
}

func (s *Server) HandleFunc(pattern string, handler func(http.ResponseWriter, *http.Request)) {
	s.router.HandleFunc(pattern, handler)
}

func (s *Server) leaderCs() string {
	peer, ok := s.raftServer.Peers()[s.raftServer.Leader()]

	if !ok {
		return ""
	} else {
		return peer.ConnectionString
	}
}

// Starts the server.
func (s *Server) ListenAndServe(primary string) error {
	var err error
	s.httpServer = &http.Server{
		Handler: s.router,
	}

	transporter := raft.NewHTTPTransporter("/raft")
	transporter.Transport.Dial = transport.UnixDialer

	s.raftServer, err = raft.NewServer(s.name, s.path, transporter, nil, s, "")
	if err != nil {
		log.Printf("Couldn't start raft server")
	}

	transporter.Install(s.raftServer, s)
	s.raftServer.Start()

	if primary == "" {
		log.Println("Initializing new cluster")

		_, err := s.raftServer.Do(&raft.DefaultJoinCommand{
			Name:             s.raftServer.Name(),
			ConnectionString: s.connectionString,
		})

		if err != nil {
			log.Fatal(err)
		}
	} else {
		log.Println("Joining cluster %s", primary)
		s.Join(primary)
	}

	s.router.HandleFunc("/sql", s.sqlHandler).Methods("POST")
	s.router.HandleFunc("/join", s.joinHandler).Methods("POST")

	l, err := transport.Listen(s.listen)
	if err != nil {
		log.Fatal(err)
	}
	return s.httpServer.Serve(l)
}

func (s *Server) Join(primary string) error {
	log.Printf("Attempting to join existing cluster at %s", primary)
	command := &raft.DefaultJoinCommand{
		Name:             s.raftServer.Name(),
		ConnectionString: s.connectionString,
	}

	b := util.JSONEncode(command)

	cs, err := transport.Encode(primary)

	if err != nil {
		return err
	}

	_, err = s.client.SafePost(cs, "/join", b)

	if err != nil {
		log.Printf("Unable to join cluster: %s", err)
		return err
	}

	return nil
}

func (s *Server) joinHandler(w http.ResponseWriter, req *http.Request) {
	command := &raft.DefaultJoinCommand{}

	if err := util.JSONDecode(req.Body, command); err != nil {
		log.Printf("Invalid join request: %s", err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	if _, err := s.raftServer.Do(command); err != nil {
		log.Printf("Error performing join command: %s", err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
}

func (s *Server) sqlHandler(w http.ResponseWriter, req *http.Request) {
	state := s.raftServer.State()

	if state != "leader" {
		leaderCs := s.leaderCs()

		if leaderCs == "" {
			http.Error(w, "No leader yet :(", http.StatusBadRequest)
			return
		}

		sqlResp, err := s.client.SafePost(leaderCs, "/sql", req.Body)

		if err != nil {
			log.Printf("The primary appears to be down when proxying sql")
			http.Error(w, err.Error(), http.StatusBadRequest)
		} else {
			io.Copy(w, sqlResp)
		}

		return
	}

	query, err := ioutil.ReadAll(req.Body)
	cachedResp, cached := s.sqlCache[string(query)]

	if cached {
		log.Printf("Returning cached response")
		w.Write(cachedResp)
		return
	}

	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	command := NewSqlCommand(query)
	resp, err := s.raftServer.Do(command)

	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	w.Write(resp.([]byte))
}
