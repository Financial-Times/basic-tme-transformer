package tme

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strconv"

	"strings"

	fthealth "github.com/Financial-Times/go-fthealth/v1_1"
	"github.com/Financial-Times/go-logger"
	"github.com/Financial-Times/service-status-go/gtg"
	"github.com/Financial-Times/transactionid-utils-go"
	"github.com/gorilla/handlers"
	"github.com/gorilla/mux"
	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"
)

type Handler struct {
	service Service
}

func NewHandler(service Service) *Handler {
	return &Handler{
		service: service,
	}
}

const (
	endpointURLParameter = "endpoint"
	uuidURLParameter     = "uuid"
)

func (th *Handler) HandleGetFullTaxonomy(resp http.ResponseWriter, req *http.Request) {
	vars := mux.Vars(req)
	t := vars[endpointURLParameter]

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
	t := vars[endpointURLParameter]
	uuid := vars[uuidURLParameter]

	resp.Header().Add("Content-Type", "application/json")

	obj, found, err := th.service.GetConceptByUUID(t, uuid)
	if err != nil {
		writeJSONMessageWithStatus(resp, err.Error(), http.StatusInternalServerError)
		return
	}
	writeJSONResponse(obj, found, EndpointTypeMappings[t]["type"].(string), resp)
}

func (th *Handler) HandleSendSingle(resp http.ResponseWriter, req *http.Request) {
	var ignoreHash bool
	var err error
	vars := mux.Vars(req)
	ignoreHashHeader := req.Header.Get("X-Ignore-Hash")
	if ignoreHashHeader != "" {
		ignoreHash, err = strconv.ParseBool(ignoreHashHeader)
		if err != nil {
			logger.WithError(err).Error("Error parsing X-Ignore-Hash request header")
			writeJSONMessageWithStatus(resp, err.Error(), http.StatusInternalServerError)
			return
		}
	}
	endp := vars[endpointURLParameter]
	uuid := vars[uuidURLParameter]
	txID := transactionidutils.NewTransactionID()

	resp.Header().Add("Content-Type", "application/json")
	if err := th.service.SendConceptByUUID(txID, endp, uuid, ignoreHash); err != nil {
		writeJSONMessageWithStatus(resp, err.Error(), http.StatusInternalServerError)
		return
	}

	writeJSONMessageWithStatus(resp, fmt.Sprintf("Updated %s %s", endp, uuid), http.StatusOK)
}

func (th *Handler) GetIDs(resp http.ResponseWriter, req *http.Request) {
	vars := mux.Vars(req)
	t := vars[endpointURLParameter]

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
	t := vars[endpointURLParameter]

	count, err := th.service.GetCount(t)
	if err != nil {
		resp.Header().Add("Content-Type", "application/json")
		writeJSONMessageWithStatus(resp, err.Error(), http.StatusInternalServerError)
		return
	}
	resp.Write([]byte(strconv.Itoa(count)))
}

func (th *Handler) HandleSendConcepts(resp http.ResponseWriter, req *http.Request) {
	var ignoreHash bool
	var err error
	vars := mux.Vars(req)
	ignoreHashHeader := req.Header.Get("X-Ignore-Hash")
	if ignoreHashHeader != "" {
		ignoreHash, err = strconv.ParseBool(ignoreHashHeader)
		if err != nil {
			logger.WithError(err).Error("Error parsing X-Ignore-Hash request header")
			writeJSONMessageWithStatus(resp, err.Error(), http.StatusInternalServerError)
			return
		}
	}
	t := vars[endpointURLParameter]

	jobID := strings.Replace(transactionidutils.NewTransactionID(), "tid", "job", -1)

	go func(th *Handler, t string, jobID string) {
		th.service.SendConcepts(t, jobID, ignoreHash)
	}(th, t, jobID)

	resp.WriteHeader(http.StatusAccepted)
	writeJSONResponse(map[string]interface{}{
		"jobID": jobID,
	}, true, t, resp)

}

func (th *Handler) HandleReloadConcepts(resp http.ResponseWriter, req *http.Request) {
	vars := mux.Vars(req)
	t := vars[endpointURLParameter]
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

func (th *Handler) HealthCheck() []fthealth.Check {
	var checks []fthealth.Check
	for _, v := range th.service.GetLoadedTypes() {
		checks = append(checks, fthealth.Check{
			BusinessImpact:   "Unable to respond to requests",
			Name:             fmt.Sprintf("%s type has data loaded", EndpointTypeMappings[v]["type"]),
			PanicGuide:       "https://dewey.ft.com/basic-tme-transformer.html",
			Severity:         3,
			TechnicalSummary: "Cannot publish concepts as data not loaded.",
			Checker: func() (string, error) {
				if !th.service.IsDataLoaded(v) {
					return "Data is not loaded", errors.New("Data is not loaded")
				}
				return "Service is up and running", nil
			},
		})
	}
	return checks
}

func (th *Handler) G2GCheck() gtg.Status {
	for _, v := range th.service.GetLoadedTypes() {
		if th.service.IsDataLoaded(v) {
			return gtg.Status{GoodToGo: true}
		}
	}
	return gtg.Status{GoodToGo: false}
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

	sendSingleHandler := handlers.MethodHandler{
		"POST": th.EnforceDataLoaded(th.EnforceTaxonomy(http.HandlerFunc(th.HandleSendSingle))),
	}

	servicesRouter.Handle("/transformers/{endpoint}", getFullHandler)
	servicesRouter.Handle("/transformers/{endpoint}/__count", countHandler)
	servicesRouter.Handle("/transformers/{endpoint}/__ids", getIDsHandler)
	servicesRouter.Handle("/transformers/{endpoint}/__reload", reloadConceptsHandler)
	servicesRouter.Handle("/transformers/{endpoint}/send", sendConceptsHandler)
	servicesRouter.Handle("/transformers/{endpoint}/{uuid}", getSingleHandler)
	servicesRouter.Handle("/transformers/{endpoint}/{uuid}/send", sendSingleHandler)
	return servicesRouter

}
