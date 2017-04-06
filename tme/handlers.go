package tme

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strconv"

	log "github.com/Sirupsen/logrus"
	"github.com/gorilla/mux"
)

type Handler struct {
	service *Service
}

func NewHandler(service *Service) *Handler {
	return &Handler{
		service: service,
	}
}

func (th *Handler) HandleGetFullTaxonomy(resp http.ResponseWriter, req *http.Request) {
	vars := mux.Vars(req)
	t := vars["type"]

	if c, _ := th.service.getCount(t); c == 0 {
		writeJSONMessageWithStatus(resp, fmt.Sprintf("%s not found", EndpointTypeMappings[t]["type"]), http.StatusNotFound)
		return
	}

	pv, err := th.service.getAllConcepts(t)

	if err != nil {
		writeJSONMessageWithStatus(resp, err.Error(), http.StatusInternalServerError)
		return
	}
	defer pv.Close()
	resp.WriteHeader(http.StatusOK)
	io.Copy(resp, &pv)
}

func (th *Handler) HandleGetSingleConcept(resp http.ResponseWriter, req *http.Request) {
	vars := mux.Vars(req)
	t := vars["type"]
	uuid := vars["uuid"]

	resp.Header().Add("Content-Type", "application/json")

	obj, found, err := th.service.getConceptByUUID(t, uuid)
	if err != nil {
		writeJSONMessageWithStatus(resp, err.Error(), http.StatusInternalServerError)
	}
	writeJSONResponse(obj, found, EndpointTypeMappings[t]["type"].(string), resp)
}

func (th *Handler) GetIDs(resp http.ResponseWriter, req *http.Request) {
	vars := mux.Vars(req)
	t := vars["type"]

	if c, _ := th.service.getCount(t); c == 0 {
		writeJSONMessageWithStatus(resp, "People not found", http.StatusNotFound)
		return
	}

	pv, err := th.service.getConceptUUIDs(t)

	if err != nil {
		writeJSONMessageWithStatus(resp, err.Error(), http.StatusInternalServerError)
		return
	}
	defer pv.Close()
	resp.WriteHeader(http.StatusOK)
	io.Copy(resp, &pv)
}

func (th *Handler) GetCount(resp http.ResponseWriter, req *http.Request) {
	vars := mux.Vars(req)
	t := vars["type"]

	if !th.service.isDataLoaded() {
		resp.Header().Add("Content-Type", "application/json")
		writeStatusServiceUnavailable(resp)
		return
	}

	if _, ok := EndpointTypeMappings[t]["type"]; !ok {
		resp.Header().Add("Content-Type", "application/json")
		writeJSONMessageWithStatus(resp, fmt.Sprintf("Taxonomy %s is not supported", t), http.StatusBadRequest)
		return
	}

	count, err := th.service.getCount(t)
	if err != nil {
		resp.Header().Add("Content-Type", "application/json")
		writeJSONMessageWithStatus(resp, err.Error(), http.StatusInternalServerError)
		return
	}
	resp.Write([]byte(strconv.Itoa(count)))
}

func writeJSONMessageWithStatus(w http.ResponseWriter, msg string, statusCode int) {
	w.WriteHeader(statusCode)
	fmt.Fprintln(w, fmt.Sprintf("{\"message\": \"%s\"}", msg))
}

func writeStatusServiceUnavailable(w http.ResponseWriter) {
	writeJSONMessageWithStatus(w, "Service Unavailable", http.StatusServiceUnavailable)
}

func writeJSONResponse(obj interface{}, found bool, theType string, writer http.ResponseWriter) {
	if !found {
		writeJSONMessageWithStatus(writer, fmt.Sprintf("%s not found", theType), http.StatusNotFound)
		return
	}

	enc := json.NewEncoder(writer)
	if err := enc.Encode(obj); err != nil {
		log.Errorf("Error on json encoding=%v", err)
		writeJSONMessageWithStatus(writer, err.Error(), http.StatusInternalServerError)
		return
	}
}
