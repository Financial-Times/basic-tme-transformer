package tme

import (
	"fmt"
	"net/http"

	"github.com/gorilla/mux"
)

func (th *Handler) EnforceDataLoaded(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !th.service.IsDataLoaded() {
			w.Header().Set("Content-Type", "application/json")
			writeJSONMessageWithStatus(w, "Data not loaded", http.StatusServiceUnavailable)
			return
		}
		next.ServeHTTP(w, r)
	})
}

func (th *Handler) EnforceTaxonomy(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		vars := mux.Vars(r)
		t := vars[endpointURLParameter]

		if _, ok := EndpointTypeMappings[t]["type"]; !ok {
			w.Header().Add("Content-Type", "application/json")
			writeJSONMessageWithStatus(w, fmt.Sprintf("Taxonomy %s is not supported", t), http.StatusBadRequest)
			return
		}
		next.ServeHTTP(w, r)
	})
}
