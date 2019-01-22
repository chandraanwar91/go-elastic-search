package myhttp

import (
	"context"

	"github.com/olivere/elastic"
	"github.com/pkg/errors"
)

type HTTPGetter interface {
	CreateIndex(index string) (*elastic.IndicesCreateResult, error)
	ImportData(data string, index string) (*elastic.IndexResponse, error)
	GetData(field string, keyword string, index string, sort string, limit int) (*elastic.SearchResult, error)
}

// Getter are for implementing HTTPGetter interface
// and reserved for the future work
type Getter struct {
	client *elastic.Client
}

// New creates a new Getter
func New(url string, port string) (*Getter, error) {
	c, err := elastic.NewClient(elastic.SetURL(url + ":" + port))
	if err != nil {
		return nil, err
	}
	return &Getter{client: c}, nil
}

// Get fetches url with a timeout
func (g *Getter) CreateIndex(index string) (*elastic.IndicesCreateResult, error) {
	ctx := context.Background()
	// Use the IndexExists service to check if a specified index exists.
	exists, err := g.client.IndexExists(index).Do(ctx)
	if err != nil {
		return nil, err
	}

	if !exists {
		// Create a new index.
		createIndex, err := g.client.CreateIndex(index).Do(ctx)
		if err != nil {
			return nil, err
		}

		return createIndex, nil
	}

	return nil, errors.New("Index " + index + " already exist")
}

// Get fetches url with a timeout
func (g *Getter) ImportData(data string, index string) (*elastic.IndexResponse, error) {
	ctx := context.Background()
	put2, err := g.client.Index().
		Index(index).
		BodyString(data).
		Do(ctx)
	if err != nil {
		return nil, err
	}

	return put2, nil
}

// Get fetches url with a timeout
func (g *Getter) GetData(field string, keyword string, index string, sort string, limit int) (*elastic.SearchResult, error) {
	ctx := context.Background()
	// Search with a term query
	termQuery := elastic.NewTermQuery(field, keyword)
	searchResult, err := g.client.Search().
		Index(index).        // search in index "twitter"
		Query(termQuery).    // specify the query
		Sort(sort, true).    // sort by "user" field, ascending
		From(0).Size(limit). // take documents 0-9
		Pretty(true).        // pretty print request and response JSON
		Do(ctx)              // execute
	if err != nil {
		return nil, err
	}

	return searchResult, nil
}
