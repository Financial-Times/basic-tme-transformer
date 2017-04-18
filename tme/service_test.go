package tme

import (
	"bytes"
	"io/ioutil"
	"net/http"
	"testing"

	"github.com/Financial-Times/tme-reader/tmereader"
	"github.com/stretchr/testify/assert"
)

type mockHttpClient struct {
	resp       string
	statusCode int
	err        error
}

func (c mockHttpClient) Do(req *http.Request) (resp *http.Response, err error) {
	cb := ioutil.NopCloser(bytes.NewReader([]byte(c.resp)))
	return &http.Response{Body: cb, StatusCode: c.statusCode}, c.err
}

type mockTmeRepo struct {
	terms []Term
	err   error
	count int
}

func (t *mockTmeRepo) GetTmeTermsFromIndex(i int) ([]interface{}, error) {
	defer func() {
		t.count++
	}()
	if len(t.terms) == t.count {
		return nil, t.err
	}
	return []interface{}{t.terms[t.count]}, t.err
}

// Never used
func (t *mockTmeRepo) GetTmeTermById(s string) (interface{}, error) {
	return nil, nil
}

func createTestService() *ServiceImpl {
	repo := &mockTmeRepo{terms: []Term{{CanonicalName: "Bob", RawID: "bob"}, {CanonicalName: "Fred", RawID: "fred"}}}
	return &ServiceImpl{
		repos: map[string]tmereader.Repository{
			"topics": repo,
		},
		cacheFileName: "cache",
		httpClient: mockHttpClient{
			statusCode: 200,
			err:        nil,
			resp:       "{}",
		},
		baseURL:        "/base/url",
		maxTmeRecords:  1,
		writerEndpoint: "/endpoint",
		writerWorkers:  1,
	}
}

func TestNewService(t *testing.T) {
	svc := createTestService()

	actualService := NewService(svc.repos, svc.cacheFileName, svc.httpClient, svc.baseURL,
		svc.maxTmeRecords, svc.writerEndpoint, svc.writerWorkers)

	assert.EqualValues(t, svc, actualService)
}

func TestIsDataLoaded(t *testing.T) {
	svc := createTestService()

	svc.setDataLoaded(true)
	assert.True(t, svc.IsDataLoaded())
	svc.setDataLoaded(false)
	assert.False(t, svc.IsDataLoaded())
}

func TestServiceImpl_GetCount(t *testing.T) {
	t.Run("Success", func(t *testing.T) {

	})
}
