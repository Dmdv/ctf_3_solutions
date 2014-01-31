package index

import (
	"indexer/worker"
	"math"
	"os"
	"path/filepath"
)

var numWorkers int = 9

type Index struct {
	isIndexed bool
	workers []*worker.Worker
}

func (index *Index) FindMatches(query string) []string {
	results := make([]string, 0)

	for _, worker := range index.workers {
		worker.Query <- query
	}

	for _, worker := range index.workers {
		response := <-worker.Response
		results = append(results, response...)
	}

	return results
}

func (index *Index) IndexPath(root string) {
	i := 0

	filepath.Walk(root, func(path string, info os.FileInfo, err error) error {
		if info.IsDir() {
			return nil
		}

		for _, worker := range index.workers {
			if math.Mod(float64(i), float64(numWorkers)) == float64(worker.Index) {
				worker.IndexFile(root, path)
			}
		}

		i += 1

		return nil
	})

	index.isIndexed = true
}

func (index *Index) IsIndexed() bool {
	return index.isIndexed
}

func New() *Index {
	workers := make([]*worker.Worker, 0)

	for i := 0; i < numWorkers; i++ {
		workers = append(workers, worker.New(i))
	}

	return &Index{
		isIndexed: false,
		workers: workers,
	}
}
