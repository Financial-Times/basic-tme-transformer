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

	if c := th.service.checkAllLoaded(); !c {
		resp.Header().Set("Content-Type", "application/json")
		writeJSONMessageWithStatus(resp, "Data is not loaded", http.StatusServiceUnavailable)
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

	if c := th.service.checkAllLoaded(); !c {
		resp.Header().Set("Content-Type", "application/json")
		writeJSONMessageWithStatus(resp, "Data is not loaded", http.StatusServiceUnavailable)
		return
	}

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

	if c := th.service.checkAllLoaded(); !c {
		resp.Header().Set("Content-Type", "application/json")
		writeJSONMessageWithStatus(resp, "Data is not loaded", http.StatusServiceUnavailable)
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

	if c := th.service.checkAllLoaded(); !c {
		resp.Header().Set("Content-Type", "application/json")
		writeJSONMessageWithStatus(resp, "Data is not loaded", http.StatusServiceUnavailable)
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

func (th *Handler) HandleSendConcepts(resp http.ResponseWriter, req *http.Request) {
	vars := mux.Vars(req)
	t := vars["type"]

	if c := th.service.checkAllLoaded(); !c {
		resp.Header().Set("Content-Type", "application/json")
		writeJSONMessageWithStatus(resp, "Data is not loaded", http.StatusServiceUnavailable)
		return
	}

	if _, ok := EndpointTypeMappings[t]["type"]; !ok {
		resp.Header().Add("Content-Type", "application/json")
		writeJSONMessageWithStatus(resp, fmt.Sprintf("Taxonomy %s is not supported", t), http.StatusBadRequest)
		return
	}

	jobID, successCount, errorCount, err := th.service.sendConcepts(t)
	if err != nil {
		resp.Header().Add("Content-Type", "application/json")
		writeJSONMessageWithStatus(resp, err.Error(), http.StatusInternalServerError)
		return
	}
	resp.WriteHeader(http.StatusOK)
	//fmt.Fprintln(resp, fmt.Sprintf("{\"jobID\": \"%s\"}", jobID))
	writeJSONResponse(map[string]string{
		"jobID":        jobID,
		"successCount": string(successCount),
		"errorCount":   string(errorCount),
	}, true, t, resp)

}

func writeJSONMessageWithStatus(w http.ResponseWriter, msg string, statusCode int) {
	w.WriteHeader(statusCode)
	fmt.Fprintln(w, fmt.Sprintf("{\"message\": \"%s\"}", msg))
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
