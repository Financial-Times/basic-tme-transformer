package tme

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestHandler_Middleware(t *testing.T) {

	data := []struct {
		name          string
		url           string
		loadingStatus map[string]bool
		resultCode    int
	}{
		{
			"Success",
			"/transformers/genres/__count",
			map[string]bool{"genres": true},
			200,
		},
		{
			"Fail - No data loaded",
			"/transformers/genres/__count",
			map[string]bool{"genres": false},
			503,
		},
		{
			"Fail - Taxonomy incorrect",
			"/transformers/fake/__count",
			map[string]bool{"fake": true},
			400,
		},
	}

	for _, d := range data {
		t.Run(d.name, func(t *testing.T) {
			mockService := NewMockService(nil, nil, d.loadingStatus)
			handler := NewHandler(mockService)

			req, _ := http.NewRequest("GET", d.url, nil)
			rr := httptest.NewRecorder()

			Router(handler).ServeHTTP(rr, req)

			assert.Equal(t, rr.Code, d.resultCode, d.name)
		})
	}

}
