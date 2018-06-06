package tme

import (
	"encoding/json"
	"errors"
	"io"
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	fthealth "github.com/Financial-Times/go-fthealth/v1_1"
	"github.com/Financial-Times/go-logger"
	"github.com/Financial-Times/service-status-go/gtg"
	status "github.com/Financial-Times/service-status-go/httphandlers"
	"github.com/gorilla/mux"
	"github.com/stretchr/testify/assert"
)

type MockService struct {
	db            map[string]map[string]BasicConcept
	err           error
	loadingStatus map[string]bool
}

func init() {
	logger.InitLogger("basic-tme-transformer", "info")
}

func NewMockService(db map[string]map[string]BasicConcept, err error, loadingStatus map[string]bool) Service {
	return &MockService{
		db:            db,
		err:           err,
		loadingStatus: loadingStatus,
	}
}

func (ms *MockService) GetLoadedTypes() []string {
	var types []string
	for k := range ms.db {
		types = append(types, k)
	}
	return types
}

func (ms *MockService) IsDataLoaded(endpoint string) bool {
	return ms.loadingStatus[endpoint]
}

func (ms *MockService) GetCount(endpoint string) (int, error) {
	return len(ms.db[endpoint]), ms.err
}

func (ms *MockService) Reload(endpoint string) error {
	return ms.err
}

func (ms *MockService) RefreshConceptByUUID(endpoint, uuid string) (BasicConcept, bool, error) {
	bucket, ok := ms.db[endpoint]
	if !ok {
		return BasicConcept{}, false, errors.New("not found")
	}

	concept, ok := bucket[uuid]
	if !ok {
		return BasicConcept{}, false, errors.New("not found")
	}

	return concept, true, nil
}

func (ms *MockService) SendConceptByUUID(txID, endpoint, uuid string, ignoreHash bool) error {
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
func (ms *MockService) SendConcepts(endpoint, jobID string, ignoreHash bool) error {
	return ms.err
}

func TestHandlers_CodeAndBody(t *testing.T) {

	data := []struct {
		name          string
		method        string
		url           string
		resultCode    int
		resultBody    string
		err           error
		db            map[string]map[string]BasicConcept
		loadingStatus map[string]bool
	}{
		{
			"Success - Concept by UUID",
			"GET",
			"/transformers/genres/2a88a647-59bc-4043-8f1b-5add71ddf3dc",
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
			map[string]bool{
				"genres": true,
			},
		},
		{
			"Success - IDs",
			"GET",
			"/transformers/genres/__ids",
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
			map[string]bool{
				"genres": true,
			},
		},
		{
			"Success - All concepts",
			"GET",
			"/transformers/genres",
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
			map[string]bool{
				"genres": true,
			},
		},
		{
			"Success - Count",
			"GET",
			"/transformers/genres/__count",
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
			map[string]bool{
				"genres": true,
			},
		},
		{
			"Fail - Get All Concepts Service Error",
			"GET",
			"/transformers/genres",
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
			map[string]bool{
				"genres": true,
			},
		},
		{
			"Fail - Get Single Concept Service Error",
			"GET",
			"/transformers/genres/2a88a647-59bc-4043-8f1b-5add71ddf3dc",
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
			map[string]bool{
				"genres": true,
			},
		},
		{
			"Fail - Get IDs Service Error",
			"GET",
			"/transformers/genres/__ids",
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
			map[string]bool{
				"genres": true,
			},
		},
		{
			"Fail - Get Count Service Error",
			"GET",
			"/transformers/genres/__count",
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
			map[string]bool{
				"genres": true,
			},
		},
		{
			"Success - Send Service",
			"POST",
			"/transformers/genres/send",
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
			map[string]bool{
				"genres": true,
			},
		},
		{
			"Fail - Get Concept By UUID Not Found",
			"GET",
			"/transformers/genres/13ea8695-52c3-4557-952d-629fca9717e3",
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
			map[string]bool{
				"genres": true,
			},
		},
		{
			"Success - Reload Concept",
			"POST",
			"/transformers/genres/__reload",
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
			map[string]bool{
				"genres": true,
			},
		},
		{
			"Success - Send concept",
			"POST",
			"/transformers/genres/2a88a647-59bc-4043-8f1b-5add71ddf3dc/send",
			200,
			"IGNORE",
			nil,
			map[string]map[string]BasicConcept{
				"genres": {
					"2a88a647-59bc-4043-8f1b-5add71ddf3dc": {
						UUID:      "2a88a647-59bc-4043-8f1b-5add71ddf3dc",
						PrefLabel: "Test",
					},
				},
			},
			map[string]bool{
				"genres": true,
			},
		},
		{
			"Failure - Send concept",
			"POST",
			"/transformers/genres/2a88a647-59bc-4043-8f1b-5add71ddf3dc/send",
			500,
			"IGNORE",
			errors.New("not found"),
			map[string]map[string]BasicConcept{
				"genres": {},
			},
			map[string]bool{
				"genres": true,
			},
		},
	}

	for _, d := range data {
		t.Run(d.name, func(t *testing.T) {
			mockService := NewMockService(d.db, d.err, d.loadingStatus)
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

func TestHandler_G2GCheck_Good(t *testing.T) {
	db := map[string]map[string]BasicConcept{
		"genres": {
			"2a88a647-59bc-4043-8f1b-5add71ddf3dc": {
				UUID:      "2a88a647-59bc-4043-8f1b-5add71ddf3dc",
				PrefLabel: "Test",
			},
		},
	}
	loadingStatus := map[string]bool{
		"genres": true,
	}
	mockService := NewMockService(db, nil, loadingStatus)
	handler := NewHandler(mockService)

	router := mux.NewRouter()
	router.HandleFunc("/__gtg", status.NewGoodToGoHandler(gtg.StatusChecker(handler.G2GCheck)))

	req, _ := http.NewRequest("GET", "/__gtg", nil)
	rr := httptest.NewRecorder()

	router.ServeHTTP(rr, req)
	assert.Equal(t, 200, rr.Code)
}

func TestHandler_G2GCheck_Bad(t *testing.T) {
	db := map[string]map[string]BasicConcept{
		"genres": {
			"2a88a647-59bc-4043-8f1b-5add71ddf3dc": {
				UUID:      "2a88a647-59bc-4043-8f1b-5add71ddf3dc",
				PrefLabel: "Test",
			},
		},
	}
	loadingStatus := map[string]bool{
		"genres": false,
	}
	mockService := NewMockService(db, nil, loadingStatus)
	handler := NewHandler(mockService)

	router := mux.NewRouter()
	router.HandleFunc("/__gtg", status.NewGoodToGoHandler(gtg.StatusChecker(handler.G2GCheck)))

	req, _ := http.NewRequest("GET", "/__gtg", nil)
	rr := httptest.NewRecorder()

	router.ServeHTTP(rr, req)
	assert.Equal(t, 503, rr.Code)
}

func TestHandler_Healthcheck_Good(t *testing.T) {
	db := map[string]map[string]BasicConcept{
		"genres": {
			"2a88a647-59bc-4043-8f1b-5add71ddf3dc": {
				UUID:      "2a88a647-59bc-4043-8f1b-5add71ddf3dc",
				PrefLabel: "Test",
			},
		},
	}
	loadingStatus := map[string]bool{
		"genres": true,
	}
	mockService := NewMockService(db, nil, loadingStatus)
	handler := NewHandler(mockService)

	router := mux.NewRouter()

	var checks = handler.HealthCheck()

	timedHC := fthealth.TimedHealthCheck{
		HealthCheck: fthealth.HealthCheck{
			SystemCode:  "basic-tme-transformer",
			Description: "Transforms concepts in TME to UPP representation",
			Name:        "basic-tme-transformer",
			Checks:      checks,
		},
		Timeout: 10 * time.Second,
	}

	router.HandleFunc("/__health", fthealth.Handler(&timedHC))

	req, _ := http.NewRequest("GET", "/__health", nil)
	rr := httptest.NewRecorder()

	router.ServeHTTP(rr, req)

	b, err := ioutil.ReadAll(rr.Body)
	assert.NoError(t, err)
	var output struct {
		Name string
		OK   bool
	}

	err = json.Unmarshal(b, &output)
	assert.NoError(t, err)

	assert.Equal(t, 200, rr.Code)
	assert.Equal(t, "basic-tme-transformer", output.Name)
	assert.Equal(t, true, output.OK)
}

func TestHandler_Healthcheck_Bad(t *testing.T) {
	db := map[string]map[string]BasicConcept{
		"genres": {
			"2a88a647-59bc-4043-8f1b-5add71ddf3dc": {
				UUID:      "2a88a647-59bc-4043-8f1b-5add71ddf3dc",
				PrefLabel: "Test",
			},
		},
	}
	loadingStatus := map[string]bool{
		"genres": false,
	}
	mockService := NewMockService(db, nil, loadingStatus)
	handler := NewHandler(mockService)

	router := mux.NewRouter()
	var checks = handler.HealthCheck()

	timedHC := fthealth.TimedHealthCheck{
		HealthCheck: fthealth.HealthCheck{
			SystemCode:  "basic-tme-transformer",
			Description: "Transforms concepts in TME to UPP representation",
			Name:        "basic-tme-transformer",
			Checks:      checks,
		},
		Timeout: 10 * time.Second,
	}

	router.HandleFunc("/__health", fthealth.Handler(&timedHC))

	req, _ := http.NewRequest("GET", "/__health", nil)
	rr := httptest.NewRecorder()

	router.ServeHTTP(rr, req)

	b, err := ioutil.ReadAll(rr.Body)
	assert.NoError(t, err)
	var output struct {
		Name string
		OK   bool
	}

	err = json.Unmarshal(b, &output)
	assert.NoError(t, err)

	assert.Equal(t, 200, rr.Code)
	assert.Equal(t, "basic-tme-transformer", output.Name)
	assert.Equal(t, false, output.OK)
}
