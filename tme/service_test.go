package tme

import (
	"bytes"
	"io"
	"io/ioutil"
	"net/http"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/Financial-Times/tme-reader/tmereader"
	log "github.com/Sirupsen/logrus"
	"github.com/stretchr/testify/assert"
)

var (
	GetAllConceptsResult  string        = "{\"uuid\":\"14fa0405-c625-3061-a1a0-a00643fc073f\",\"prefLabel\":\"Fred\",\"type\":\"Topic\",\"authority\":\"TME\",\"authorityValue\":\"ZnJlZA==-VG9waWNz\"}\n{\"uuid\":\"c0e2b109-2212-35c1-8645-2a13bc2cc3db\",\"prefLabel\":\"Bob\",\"type\":\"Topic\",\"authority\":\"TME\",\"authorityValue\":\"Ym9i-VG9waWNz\"}\n"
	GetConceptUUIDsResult string        = "{\"uuid\":\"14fa0405-c625-3061-a1a0-a00643fc073f\"}\n{\"uuid\":\"c0e2b109-2212-35c1-8645-2a13bc2cc3db\"}\n"
	RepoSleepDuration     time.Duration = 5 * time.Second
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

func createBasicTestService() *ServiceImpl {
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
	svc := createBasicTestService()

	actualService := NewService(svc.repos, svc.cacheFileName, svc.httpClient, svc.baseURL,
		svc.maxTmeRecords, svc.writerEndpoint, svc.writerWorkers)

	assert.EqualValues(t, svc, actualService)
}

func TestIsDataLoaded(t *testing.T) {
	svc := createBasicTestService()

	svc.setDataLoaded(true)
	assert.True(t, svc.IsDataLoaded())
	svc.setDataLoaded(false)
	assert.False(t, svc.IsDataLoaded())
}

func TestServiceImpl_GetCount(t *testing.T) {
	tmpfile := getTempFile(t)
	defer os.Remove(tmpfile.Name())
	repo := &mockTmeRepo{terms: []Term{{CanonicalName: "Bob", RawID: "bob"}, {CanonicalName: "Fred", RawID: "fred"}}}
	repos := map[string]tmereader.Repository{
		"topics": repo,
	}
	successHTTPclient := mockHttpClient{
		statusCode: 200,
		err:        nil,
		resp:       "{}",
	}

	t.Run("Success", func(t *testing.T) {
		svc := NewService(repos, tmpfile.Name(), successHTTPclient, "/base/url", 1, "/endpoint", 1)
		time.Sleep(RepoSleepDuration)
		count, err := svc.GetCount("topics")
		assert.NoError(t, err)
		assert.Equal(t, 2, count)
	})
	t.Run("Error - not loaded", func(t *testing.T) {
		svc := NewService(repos, tmpfile.Name(), successHTTPclient, "/base/url", 1, "/endpoint", 1)
		count, err := svc.GetCount("topics")
		assert.Error(t, err)
		assert.Equal(t, 0, count)
	})
	t.Run("Error - wrong bucket", func(t *testing.T) {
		svc := NewService(repos, tmpfile.Name(), successHTTPclient, "/base/url", 1, "/endpoint", 1)
		time.Sleep(RepoSleepDuration)
		count, err := svc.GetCount("fake")
		assert.Error(t, err)
		assert.Equal(t, 0, count)
	})
}

func TestServiceImpl_GetAllConcepts_Success(t *testing.T) {
	svc := createTestService(t, 200, nil)
	time.Sleep(RepoSleepDuration)
	pv, err := svc.GetAllConcepts("topics")
	var wg sync.WaitGroup
	var res string
	wg.Add(1)
	go func(reader io.Reader, w *sync.WaitGroup) {
		var err error
		blob, err := ioutil.ReadAll(reader)
		assert.NoError(t, err)
		log.Infof("Got bytes: %v", string(blob[:]))
		res = string(blob[:])
		wg.Done()
	}(&pv, &wg)
	wg.Wait()
	assert.NoError(t, err)
	assert.Equal(t, GetAllConceptsResult, res)
}

func TestServiceImpl_GetAllConcepts_Error(t *testing.T) {
	svc := createTestService(t, 200, nil)
	time.Sleep(RepoSleepDuration)
	pv, err := svc.GetAllConcepts("fake")
	var wg sync.WaitGroup
	var res string
	wg.Add(1)
	go func(reader io.Reader, w *sync.WaitGroup) {
		var err error
		blob, err := ioutil.ReadAll(reader)
		assert.NoError(t, err)
		log.Infof("Got bytes: %v", string(blob[:]))
		res = string(blob[:])
		wg.Done()
	}(&pv, &wg)
	wg.Wait()

	assert.NoError(t, err)
	assert.Empty(t, res)
}

func TestServiceImpl_GetConceptUUIDs_Success(t *testing.T) {
	svc := createTestService(t, 200, nil)
	time.Sleep(RepoSleepDuration)
	pv, err := svc.GetConceptUUIDs("topics")
	var wg sync.WaitGroup
	var res string
	wg.Add(1)
	go func(reader io.Reader, w *sync.WaitGroup) {
		var err error
		blob, err := ioutil.ReadAll(reader)
		assert.NoError(t, err)
		log.Infof("Got bytes: %v", string(blob[:]))
		res = string(blob[:])
		wg.Done()
	}(&pv, &wg)
	wg.Wait()
	assert.NoError(t, err)
	assert.Equal(t, GetConceptUUIDsResult, res)
}

func TestServiceImpl_GetConceptByUUID(t *testing.T) {
	svc := createTestService(t, 200, nil)
	time.Sleep(RepoSleepDuration)

	t.Run("Success", func(t *testing.T) {
		bc, found, err := svc.GetConceptByUUID("topics", "14fa0405-c625-3061-a1a0-a00643fc073f")

		assert.NoError(t, err)
		assert.True(t, found)
		assert.Equal(t, "Fred", bc.PrefLabel)
	})

	t.Run("Error - bad type", func(t *testing.T) {
		bc, found, err := svc.GetConceptByUUID("fake", "14fa0405-c625-3061-a1a0-a00643fc073f")

		assert.Error(t, err)
		assert.False(t, found)
		assert.Empty(t, bc.PrefLabel)
	})

	t.Run("Error - wrong uuid", func(t *testing.T) {
		bc, found, err := svc.GetConceptByUUID("topics", "12345678-c625-3061-a1a0-a00643fc073f")

		assert.NoError(t, err)
		assert.False(t, found)
		assert.Empty(t, bc.PrefLabel)
	})
}

func TestServiceImpl_SendConcepts(t *testing.T) {
	svc := createTestService(t, 200, nil)
	time.Sleep(RepoSleepDuration)

	t.Run("Success", func(t *testing.T) {
		err := svc.SendConcepts("topics", "job123")
		assert.NoError(t, err)
	})

	t.Run("Success - No job ID", func(t *testing.T) {
		err := svc.SendConcepts("topics", "")
		assert.NoError(t, err)
	})

	t.Run("Error - wrong type", func(t *testing.T) {
		err := svc.SendConcepts("fake", "job123")
		assert.Error(t, err)
	})
}

func TestServiceImpl_SendConcepts_ServiceError(t *testing.T) {
	svc := createTestService(t, 503, nil)
	time.Sleep(RepoSleepDuration)

	err := svc.SendConcepts("topics", "job123")
	// We're expecting that there's no error as we don't want a single failure to kill the entire job.
	assert.NoError(t, err)
}

func createTestService(t *testing.T, statusCode int, clientError error) Service {
	tmpfile := getTempFile(t)
	defer os.Remove(tmpfile.Name())
	repo := &mockTmeRepo{terms: []Term{{CanonicalName: "Bob", RawID: "bob"}, {CanonicalName: "Fred", RawID: "fred"}}}
	repos := map[string]tmereader.Repository{
		"topics": repo,
	}
	successHTTPclient := mockHttpClient{
		statusCode: statusCode,
		err:        clientError,
		resp:       "{}",
	}
	return NewService(repos, tmpfile.Name(), successHTTPclient, "/base/url", 1, "/endpoint", 1)
}

func getTempFile(t *testing.T) *os.File {
	tmpfile, err := ioutil.TempFile("", "example")
	assert.NoError(t, err)
	assert.NoError(t, tmpfile.Close())
	log.Debug("File:%s", tmpfile.Name())
	return tmpfile
}
