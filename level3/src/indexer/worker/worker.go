package worker

import (
	"bytes"
	"io/ioutil"
	"path/filepath"
	"strconv"
)

type Worker struct {
	fileIndex map[string][]byte
	Index int
	Query chan string
	Response chan []string
}

func (worker *Worker) IndexFile(root, path string) {
	contents, _ := ioutil.ReadFile(path)
	relPath, _ := filepath.Rel(root, path)
	worker.fileIndex[relPath] = contents
}

func (worker *Worker) query(query string) {
	results := make([]string, 0)

	for path, contents := range worker.fileIndex {
		for index, line := range bytes.Split(contents, []byte("\n")) {
			if bytes.Contains(line, []byte(query)) {
				results = append(results, path+":"+strconv.Itoa(index+1))
			}
		}
	}

	worker.Response <- results
}

func (worker *Worker) run() {
	for {
		q := <-worker.Query
		worker.query(q)
	}
}

func New(index int) *Worker {
	worker := &Worker{
		fileIndex: make(map[string][]byte),
		Index: index,
		Query: make(chan string),
		Response: make(chan []string),
	}

	go worker.run()

	return worker
}
