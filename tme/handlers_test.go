package tme

import (
	"encoding/json"
	"errors"
	"io"
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
)

type MockService struct {
	isAllLoaded bool
	db          map[string]map[string]BasicConcept
	err         error
}

func NewMockService(isAllLoaded bool, db map[string]map[string]BasicConcept, err error) Service {
	return &MockService{
		isAllLoaded: isAllLoaded,
		db:          db,
		err:         err,
	}
}

func (ms *MockService) IsDataLoaded() bool {
	return ms.isAllLoaded
}

func (ms *MockService) GetCount(endpoint string) (int, error) {
	return len(ms.db[endpoint]), ms.err
}

func (ms *MockService) Reload(endpoint string) error {
	return ms.err
}

func (ms *MockService) GetAllConcepts(endpoint string) (io.PipeReader, error) {
	pr, pw := io.Pipe()
	go func() {
		for _, c := range ms.db[endpoint] {
			bArr, _ := json.Marshal(c)
			pw.Write(bArr)
		}
		io.WriteString(pw, "\n")
		pw.Close()
	}()
	return *pr, ms.err
}
func (ms *MockService) GetConceptByUUID(endpoint, uuid string) (BasicConcept, bool, error) {
	c, ok := ms.db[endpoint][uuid]
	return c, ok, ms.err
}
func (ms *MockService) GetConceptUUIDs(endpoint string) (io.PipeReader, error) {
	pr, pw := io.Pipe()
	defer pr.Close()
	go func() {
		for k, _ := range ms.db[endpoint] {
			encoder := json.NewEncoder(pw)
			pl := ConceptUUID{UUID: k}
			encoder.Encode(pl)
		}
		pw.Close()
	}()
	return *pr, ms.err
}
func (ms *MockService) SendConcepts(endpoint, jobID string) error {
	return ms.err
}

func TestHandlers_CodeAndBody(t *testing.T) {

	data := []struct {
		name       string
		method     string
		url        string
		isLoaded   bool
		resultCode int
		resultBody string
		err        error
		db         map[string]map[string]BasicConcept
	}{
		{
			"Success - Concept by UUID",
			"GET",
			"/transformers/genres/2a88a647-59bc-4043-8f1b-5add71ddf3dc",
			true,
			200,
			"{\"uuid\":\"2a88a647-59bc-4043-8f1b-5add71ddf3dc\",\"prefLabel\":\"Test\"}\n",
			nil,
			map[string]map[string]BasicConcept{
				"genres": {
					"2a88a647-59bc-4043-8f1b-5add71ddf3dc": {
						UUID:      "2a88a647-59bc-4043-8f1b-5add71ddf3dc",
						PrefLabel: "Test",
					},
				},
			},
		},
		{
			"Success - IDs",
			"GET",
			"/transformers/genres/__ids",
			true,
			200,
			"",
			nil,
			map[string]map[string]BasicConcept{
				"genres": {
					"2a88a647-59bc-4043-8f1b-5add71ddf3dc": {
						UUID:      "2a88a647-59bc-4043-8f1b-5add71ddf3dc",
						PrefLabel: "Test",
					},
				},
			},
		},
		{
			"Success - All concepts",
			"GET",
			"/transformers/genres",
			true,
			200,
			"{\"uuid\":\"2a88a647-59bc-4043-8f1b-5add71ddf3dc\",\"prefLabel\":\"Test\"}\n",
			nil,
			map[string]map[string]BasicConcept{
				"genres": {
					"2a88a647-59bc-4043-8f1b-5add71ddf3dc": {
						UUID:      "2a88a647-59bc-4043-8f1b-5add71ddf3dc",
						PrefLabel: "Test",
					},
				},
			},
		},
		{
			"Success - Count",
			"GET",
			"/transformers/genres/__count",
			true,
			200,
			"1",
			nil,
			map[string]map[string]BasicConcept{
				"genres": {
					"2a88a647-59bc-4043-8f1b-5add71ddf3dc": {
						UUID:      "2a88a647-59bc-4043-8f1b-5add71ddf3dc",
						PrefLabel: "Test",
					},
				},
			},
		},
		{
			"Fail - Get All Concepts Service Error",
			"GET",
			"/transformers/genres",
			true,
			500,
			"{\"message\": \"Service error\"}\n",
			errors.New("Service error"),
			map[string]map[string]BasicConcept{
				"genres": {
					"2a88a647-59bc-4043-8f1b-5add71ddf3dc": {
						UUID:      "2a88a647-59bc-4043-8f1b-5add71ddf3dc",
						PrefLabel: "Test",
					},
				},
			},
		},
		{
			"Fail - Get Single Concept Service Error",
			"GET",
			"/transformers/genres/2a88a647-59bc-4043-8f1b-5add71ddf3dc",
			true,
			500,
			"{\"message\": \"Service error\"}\n",
			errors.New("Service error"),
			map[string]map[string]BasicConcept{
				"genres": {
					"2a88a647-59bc-4043-8f1b-5add71ddf3dc": {
						UUID:      "2a88a647-59bc-4043-8f1b-5add71ddf3dc",
						PrefLabel: "Test",
					},
				},
			},
		},
		{
			"Fail - Get IDs Service Error",
			"GET",
			"/transformers/genres/__ids",
			true,
			500,
			"{\"message\": \"Service error\"}\n",
			errors.New("Service error"),
			map[string]map[string]BasicConcept{
				"genres": {
					"2a88a647-59bc-4043-8f1b-5add71ddf3dc": {
						UUID:      "2a88a647-59bc-4043-8f1b-5add71ddf3dc",
						PrefLabel: "Test",
					},
				},
			},
		},
		{
			"Fail - Get Count Service Error",
			"GET",
			"/transformers/genres/__count",
			true,
			500,
			"{\"message\": \"Service error\"}\n",
			errors.New("Service error"),
			map[string]map[string]BasicConcept{
				"genres": {
					"2a88a647-59bc-4043-8f1b-5add71ddf3dc": {
						UUID:      "2a88a647-59bc-4043-8f1b-5add71ddf3dc",
						PrefLabel: "Test",
					},
				},
			},
		},
		{
			"Success - Send Service",
			"POST",
			"/transformers/genres/send",
			true,
			202,
			"IGNORE",
			errors.New("Service error"),
			map[string]map[string]BasicConcept{
				"genres": {
					"2a88a647-59bc-4043-8f1b-5add71ddf3dc": {
						UUID:      "2a88a647-59bc-4043-8f1b-5add71ddf3dc",
						PrefLabel: "Test",
					},
				},
			},
		},
		{
			"Fail - Get Concept By UUID Not Found",
			"GET",
			"/transformers/genres/13ea8695-52c3-4557-952d-629fca9717e3",
			true,
			404,
			"{\"message\": \"Genre not found\"}\n",
			nil,
			map[string]map[string]BasicConcept{
				"genres": {
					"2a88a647-59bc-4043-8f1b-5add71ddf3dc": {
						UUID:      "2a88a647-59bc-4043-8f1b-5add71ddf3dc",
						PrefLabel: "Test",
					},
				},
			},
		},
		{
			"Success - Reload Concept",
			"POST",
			"/transformers/genres/__reload",
			true,
			202,
			"{\"message\": \"Reloading genres\"}\n",
			nil,
			map[string]map[string]BasicConcept{
				"genres": {
					"2a88a647-59bc-4043-8f1b-5add71ddf3dc": {
						UUID:      "2a88a647-59bc-4043-8f1b-5add71ddf3dc",
						PrefLabel: "Test",
					},
				},
			},
		},
	}

	for _, d := range data {
		t.Run(d.name, func(t *testing.T) {
			mockService := NewMockService(d.isLoaded, d.db, d.err)
			handler := NewHandler(mockService)

			req, _ := http.NewRequest(d.method, d.url, nil)
			rr := httptest.NewRecorder()

			Router(handler).ServeHTTP(rr, req)

			b, err := ioutil.ReadAll(rr.Body)
			assert.NoError(t, err)
			body := string(b)
			assert.Equal(t, d.resultCode, rr.Code, d.name)
			if d.resultBody != "IGNORE" {
				assert.Equal(t, d.resultBody, body, d.name)
			}

		})
	}

}

func TestWriteJSONResponse_Error(t *testing.T) {
	rr := httptest.NewRecorder()
	value := make(chan int)
	writeJSONResponse(value, true, "type", rr)

	b, err := ioutil.ReadAll(rr.Body)
	assert.NoError(t, err)
	body := string(b)
	assert.Equal(t, 500, rr.Code)
	assert.Equal(t, "{\"message\": \"json: unsupported type: chan int\"}\n", body)
}
