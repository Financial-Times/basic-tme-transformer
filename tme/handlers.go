package tme

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strconv"

	"strings"

	"github.com/Financial-Times/transactionid-utils-go"
	log "github.com/Sirupsen/logrus"
	"github.com/gorilla/handlers"
	"github.com/gorilla/mux"
)

type Handler struct {
	service Service
}

func NewHandler(service Service) *Handler {
	return &Handler{
		service: service,
	}
}

func (th *Handler) HandleGetFullTaxonomy(resp http.ResponseWriter, req *http.Request) {
	vars := mux.Vars(req)
	t := vars["type"]

	pv, err := th.service.GetAllConcepts(t)

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

	obj, found, err := th.service.GetConceptByUUID(t, uuid)
	if err != nil {
		writeJSONMessageWithStatus(resp, err.Error(), http.StatusInternalServerError)
		return
	}
	writeJSONResponse(obj, found, EndpointTypeMappings[t]["type"].(string), resp)
}

func (th *Handler) GetIDs(resp http.ResponseWriter, req *http.Request) {
	vars := mux.Vars(req)
	t := vars["type"]

	pv, err := th.service.GetConceptUUIDs(t)

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

	count, err := th.service.GetCount(t)
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

	jobID := strings.Replace(transactionidutils.NewTransactionID(), "tid", "job", -1)

	go func(th *Handler, t string, jobID string) {
		th.service.SendConcepts(t, jobID)
	}(th, t, jobID)

	resp.WriteHeader(http.StatusAccepted)
	writeJSONResponse(map[string]interface{}{
		"jobID": jobID,
	}, true, t, resp)

}

func (th *Handler) HandleReloadConcepts(resp http.ResponseWriter, req *http.Request){
	vars := mux.Vars(req)
	t := vars["type"]
	go th.service.Reload(t)

	writeJSONMessageWithStatus(resp, fmt.Sprintf("Reloading %s", t), http.StatusAccepted)
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

func Router(th *Handler) *mux.Router {
	servicesRouter := mux.NewRouter()

	getFullHandler := handlers.MethodHandler{
		"GET": th.EnforceDataLoaded(th.EnforceTaxonomy(http.HandlerFunc(th.HandleGetFullTaxonomy))),
	}

	getSingleHandler := handlers.MethodHandler{
		"GET": th.EnforceDataLoaded(th.EnforceTaxonomy(http.HandlerFunc(th.HandleGetSingleConcept))),
	}

	countHandler := handlers.MethodHandler{
		"GET": th.EnforceDataLoaded(th.EnforceTaxonomy(http.HandlerFunc(th.GetCount))),
	}

	getIDsHandler := handlers.MethodHandler{
		"GET": th.EnforceDataLoaded(th.EnforceTaxonomy(http.HandlerFunc(th.GetIDs))),
	}

	sendConceptsHandler := handlers.MethodHandler{
		"POST": th.EnforceDataLoaded(th.EnforceTaxonomy(http.HandlerFunc(th.HandleSendConcepts))),
	}

	reloadConceptsHandler := handlers.MethodHandler{
		"POST": th.EnforceTaxonomy(http.HandlerFunc(th.HandleReloadConcepts)),
	}

	servicesRouter.Handle("/transformers/{type}", getFullHandler)
	servicesRouter.Handle("/transformers/{type}/__count", countHandler)
	servicesRouter.Handle("/transformers/{type}/__ids", getIDsHandler)
	servicesRouter.Handle("/transformers/{type}/__reload", reloadConceptsHandler)
	servicesRouter.Handle("/transformers/{type}/send", sendConceptsHandler)

	servicesRouter.Handle("/transformers/{type}/{uuid}", getSingleHandler)
	//servicesRouter.Handle("/transformers/{type}/{uuid:?([0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12})}", getSingleHandler)
	return servicesRouter

}
