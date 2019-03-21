package myhttp

import (
	"context"

	"github.com/olivere/elastic"
	"github.com/pkg/errors"
)

type HTTPElastic interface {
	CreateIndex(index string) (*elastic.IndicesCreateResult, error)
	ImportData(data string, index string) (*elastic.IndexResponse, error)
	GetData(field string, keyword string, index string, sort string, limit int) (*elastic.SearchResult, error)
}

type Elastic struct {
	client *elastic.Client
}

// New creates a client
func New(url string, port string) (*Elastic, error) {
	c, err := elastic.NewClient(
		elastic.SetURL(url+":"+port),
		elastic.SetSniff(false),
		elastic.SetHealthcheck(false),
	)
	if err != nil {
		return nil, err
	}

	return &Elastic{client: c}, nil
}

// Get fetches url with a timeout
func (g *Elastic) CreateIndex(index string) (*elastic.IndicesCreateResult, error) {
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

// Create document if not exist / update document if exist
func (g *Elastic) CreateOrUpdateDocumentById(index string, indexType string, id string, body interface{}) (indexResponse *elastic.IndexResponse, err error) {
	indexResponse, err = g.client.
		Index().
		Index(index).
		Type(indexType).
		Id(id).
		BodyJson(body).
		Do(context.TODO())

	return indexResponse, err
}

// Delete / truncate all data in index/type
func (g *Elastic) DeleteIndexType(index string, indexType string) (deleteResponse *elastic.DeleteResponse, err error) {
	deleteResponse, err = g.client.
		Delete().
		Index(index).
		Type(indexType).
		Do(context.TODO())

	return deleteResponse, err
}

// Get documents by ids and size
func (g *Elastic) GetDocumentsByIdsInAndSize(index string, indexType string, ids []int, size int) (searchResult *elastic.SearchResult, err error) {
	// 1. Set condition
	var queryShoulds []elastic.Query
	for _, id := range ids {
		queryShoulds = append(queryShoulds, elastic.NewMatchPhraseQuery("id", id))
	}

	// 2. Search
	searchResult, err = g.client.
		Search().
		Index(index).
		Type(indexType).
		Query(
			elastic.NewBoolQuery().Should(queryShoulds...),
		).
		Size(size).
		Do(context.TODO())

	return searchResult, err
}

// Get documents by query string
/*
	{
		"match": [{
			"id": "",
			...
		}],
		"wildcard": [{
			"name": "",
			...
		}],
		"sort": [{
			"name.raw": "asc",
			...
		}],
		"size": 10
	}
*/
func (g *Elastic) GetDocumentsByMapString(index string, indexType string, body map[string]interface{}) (searchResult *elastic.SearchResult, err error) {
	// 1. Set condition
	var size int = 10
	var sorter []elastic.Sorter
	var queryMusts []elastic.Query

	for bodyKey, bodyValue := range body {

		if bodyKey == "match" { // 1.1. Check key match
			for _, bodyValueMatch := range bodyValue.([]interface{}) {

				// 1.1.1 Get column, value
				for column, value := range bodyValueMatch.(map[string]interface{}) {
					if value.(string) != "" {
						queryMusts = append(queryMusts, elastic.NewTermQuery(column, value.(string)))
					}
				}

			}
		} else if bodyKey == "size" { // 1.2. Check key size
			if value, ok := bodyValue.(float64); ok {
				size = int(value)
			}
		} else if bodyKey == "sort" { // 1.3. Check key sort
			for _, bodyValueSort := range bodyValue.([]interface{}) {

				// 1.3.1 Get column, value
				for column, direction := range bodyValueSort.(map[string]interface{}) {
					if direction.(string) != "" {
						if direction.(string) == "asc" {
							sorter = append(sorter, elastic.NewFieldSort(column).Asc())
						} else if direction.(string) == "desc" {
							sorter = append(sorter, elastic.NewFieldSort(column).Desc())
						}
					}
				}

			}
		} else if bodyKey == "wildcard" { // 1.4. Check key match
			for _, bodyValueWildcard := range bodyValue.([]interface{}) {

				// 1.4.1 Get column, value
				for column, value := range bodyValueWildcard.(map[string]interface{}) {
					if value.(string) != "" {
						queryMusts = append(queryMusts, elastic.NewWildcardQuery(column, "*"+value.(string)+"*"))
					}
				}

			}
		}

	}

	// 2. Search
	searchResult, err = g.client.
		Search().
		Index(index).
		Type(indexType).
		Query(
			elastic.NewBoolQuery().Must(queryMusts...),
		).
		SortBy(sorter...).
		Size(size).
		Do(context.TODO())

	return searchResult, err
}

// Get data elastic
func (g *Elastic) GetData(field string, keyword string, index string, sort string, limit int) (*elastic.SearchResult, error) {
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

// Import Data to elastic
func (g *Elastic) ImportData(data string, index string) (*elastic.IndexResponse, error) {
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

//  Refresh index
func (g *Elastic) RefreshIndex(index string) (refreshResult *elastic.RefreshResult, err error) {
	refreshResult, err = g.client.
		Refresh(index).
		Do(context.TODO())

	return refreshResult, err
}

// Update mapping
/*
	{
		"{type}": {
			"properties": {
				"name": {
					"fields": {
						"raw": {
							"normalizer": "keyword_normalizer",
							"type": "keyword"
						}
					},
					"type": "text"
				},
				"name_en": {
					"fields": {
						"raw": {
							"normalizer": "keyword_normalizer",
							"type": "keyword"
						}
					},
					"type": "text"
				}
			}
		}
	}
*/
func (g *Elastic) UpdateMapping(index string, indexType string, bodyString string) (puttingMappingResponse *elastic.PutMappingResponse, err error) {
	puttingMappingResponse, err = g.client.PutMapping().
		Index(index).
		Type(indexType).
		BodyString(bodyString).
		Do(context.TODO())

	return puttingMappingResponse, err
}

//  Update settings
/*
	{
		"settings": {
			"analysis": {
				"normalizer": {
					"keyword_normalizer": {
						"char_filter": [],
						"filter": [
							"asciifolding",
							"lowercase"
						],
						"type": "custom"
					}
				}
			}
		}
	}
*/
func (g *Elastic) UpdateSettings(index string, bodyString string) (indicesPutSettingsResponse *elastic.IndicesPutSettingsResponse, err error) {
	indicesPutSettingsResponse, err = g.client.IndexPutSettings().
		Index(index).
		BodyString(bodyString).
		Do(context.TODO())

	return indicesPutSettingsResponse, err
}
