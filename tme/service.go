package tme

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"sync"

	"strconv"

	"github.com/Financial-Times/tme-reader/tmereader"
	"github.com/Financial-Times/transactionid-utils-go"
	log "github.com/sirupsen/logrus"
)

type httpClient interface {
	Do(req *http.Request) (resp *http.Response, err error)
}

// Service - the main service interface
type Service interface {
	IsDataLoaded(endpoint string) bool
	GetCount(endpoint string) (int, error)
	GetAllConcepts(endpoint string) (io.PipeReader, error)
	GetConceptByUUID(endpoint, uuid string) (*BasicConcept, bool, error)
	GetConceptUUIDs(endpoint string) (io.PipeReader, error)
	SendConcepts(endpoint, jobID string, ignoreHash bool) error
	RefreshConceptByUUID(endpoint, uuid string) (*BasicConcept, bool, error)
	SendConceptByUUID(txID, endpoint, uuid string, ignoreHash bool) error
	Reload(endpoint string) error
	GetLoadedTypes() []string
}

// ServiceImpl - implements the interface above
type ServiceImpl struct {
	// sync.RWMutex
	repos          map[string]tmereader.Repository
	httpClient     httpClient
	baseURL        string
	dataLoaded     *sync.Map
	data           *sync.Map
	maxTmeRecords  int
	writerEndpoint string
	writerWorkers  int
}

// NewService - creates an instance of Service
func NewService(repos map[string]tmereader.Repository, httpClient httpClient, baseURL string, maxTmeRecords int, writerEndpoint string, writerWorkers int) Service {
	svc := &ServiceImpl{
		repos:          repos,
		httpClient:     httpClient,
		baseURL:        baseURL,
		maxTmeRecords:  maxTmeRecords,
		writerEndpoint: writerEndpoint,
		writerWorkers:  writerWorkers,
		dataLoaded:     &sync.Map{},
		data:           &sync.Map{},
	}
	go func(service *ServiceImpl) {
		err := service.loadDB()
		if err != nil {
			log.Errorf("Error while creating service: [%v]", err.Error())
		}
	}(svc)
	return svc
}

// GetLoadedTypes - returns a list of the loaded types, rather than assuming everything in the mapping.
func (s *ServiceImpl) GetLoadedTypes() []string {
	var types []string
	for k := range s.repos {
		types = append(types, k)
	}
	return types
}

// IsDataLoaded - is data for a specific endpoint loaded?
func (s *ServiceImpl) IsDataLoaded(endpoint string) bool {
	loaded, ok := s.dataLoaded.Load(endpoint)
	return ok && loaded.(bool)
}

func (s *ServiceImpl) setDataLoaded(endpoint string, val bool) {
	s.dataLoaded.Store(endpoint, true)
}

// Reload - Reloads the data for a specific endpoint
func (s *ServiceImpl) Reload(endpoint string) error {
	log.Infof("Reloading %s", endpoint)
	r, ok := s.repos[endpoint]
	if !ok {
		return errors.New("endpoint invalid")
	}

	err := s.loadConcept(endpoint, r, nil)
	if err != nil {
		return err
	}
	log.Infof("Completed %s load", endpoint)
	return nil
}

func (s *ServiceImpl) loadDB() error {
	log.Info("Loading DB...")
	var wg sync.WaitGroup
	wg.Add(len(s.repos))
	for k, v := range s.repos {
		go s.loadConcept(k, v, &wg)
	}
	wg.Wait()
	log.Info("Completed DB load.")
	return nil
}

func (s *ServiceImpl) loadConcept(endpoint string, repo tmereader.Repository, wg *sync.WaitGroup) error {
	log.Infof("Loading %s", endpoint)

	s.setDataLoaded(endpoint, false)
	defer s.setDataLoaded(endpoint, true)

	if wg != nil {
		defer wg.Done()
	}

	concepts := []*BasicConcept{}

	responseCount := 0
	for {
		terms, err := repo.GetTmeTermsFromIndex(responseCount)
		if err != nil {
			return err
		}

		if len(terms) < 1 {
			log.Infof("Fetched %d %s from TME.", len(concepts), endpoint)
			break
		}
		log.Infof("Found %d %s.", len(terms), endpoint)
		responseCount += s.maxTmeRecords
		for _, term := range terms {
			cterm := term.(Term)
			concept := transformConcept(cterm, endpoint)
			concepts = append(concepts, concept)
		}
	}

	s.data.Store(endpoint, concepts)

	log.Infof("Finished processing %s", endpoint)
	return nil
}

// GetCount - returns record count for a specific endpoint
func (s *ServiceImpl) GetCount(endpoint string) (int, error) {
	if !s.IsDataLoaded(endpoint) {
		return 0, errors.New("data not loaded")
	}

	concepts, ok := s.data.Load(endpoint)
	if !ok {
		return 0, errors.New("data not loaded")
	}
	return len(concepts.([]*BasicConcept)), nil
}

// GetAllConcepts - returns a stream of all concepts for an endpoint
func (s *ServiceImpl) GetAllConcepts(endpoint string) (io.PipeReader, error) {
	pv, pw := io.Pipe()

	go func() {
		defer pw.Close()
		concepts, ok := s.data.Load(endpoint)
		if !ok {
			return
		}
		for _, v := range concepts.([]*BasicConcept) {
			j, err := json.Marshal(v)
			if err != nil {
				log.Error("could not marshal concept for pipe", err)
				return
			}
			if _, err := pw.Write(j); err != nil {
				log.Error("could not write concept to pipe", err)
				return
			}
			io.WriteString(pw, "\n")
		}
		return
	}()

	return *pv, nil
}

// GetConceptByUUID - return a specific concept from a specific endpoint.
func (s *ServiceImpl) GetConceptByUUID(endpoint, uuid string) (*BasicConcept, bool, error) {
	if !s.IsDataLoaded(endpoint) {
		return nil, false, errors.New("data not loaded")
	}

	concepts, ok := s.data.Load(endpoint)
	if !ok {
		return nil, false, errors.New("data not loaded")
	}

	for _, v := range concepts.([]*BasicConcept) {
		if v.UUID == uuid {
			return v, true, nil
		}
	}
	log.Debugf("No cached value for [%v].", uuid)
	return nil, false, nil
}

// RefreshConceptByUUID - updates and returns a specific concept.
func (s *ServiceImpl) RefreshConceptByUUID(endpoint, uuid string) (*BasicConcept, bool, error) {
	if !s.IsDataLoaded(endpoint) {
		return nil, false, errors.New("data not loaded")
	}

	term, err := s.repos[endpoint].GetTmeTermById(uuid)
	if err != nil {
		return nil, false, err
	}

	cterm := term.(Term)
	concept := transformConcept(cterm, endpoint)

	concepts, ok := s.data.Load(endpoint)
	if !ok {
		return nil, false, errors.New("data not loaded")
	}

	cconcepts := concepts.([]*BasicConcept)
	for i, cachedConcept := range cconcepts {
		if cachedConcept.UUID == concept.UUID {
			cconcepts[i] = concept
			break
		}
	}

	s.data.Store(endpoint, cconcepts)

	return concept, true, nil
}

func (s *ServiceImpl) GetConceptUUIDs(endpoint string) (io.PipeReader, error) {
	pv, pw := io.Pipe()

	if !s.IsDataLoaded(endpoint) {
		defer pw.Close()
		return *pv, errors.New("data not loaded")
	}

	go func() {
		defer pw.Close()
		encoder := json.NewEncoder(pw)
		concepts, ok := s.data.Load(endpoint)
		if !ok {
			return
		}
		for _, v := range concepts.([]*BasicConcept) {
			pl := ConceptUUID{
				UUID: v.UUID,
			}
			if err := encoder.Encode(pl); err != nil {
				return
			}
		}
		return
	}()
	return *pv, nil
}

type conceptResponse struct {
	uuid     string
	jobID    string
	response error
}

type conceptRequest struct {
	uuid    string
	jobID   string
	theType string
	payload *BasicConcept
}

func (s *ServiceImpl) SendConceptByUUID(txID, endpoint, uuid string, ignoreHash bool) error {
	concept, ok, err := s.RefreshConceptByUUID(endpoint, uuid)
	if err != nil {
		return err
	} else if !ok {
		return errors.New("concept not found")
	}

	return s.sendSingleConcept(endpoint, concept.UUID, concept, txID, ignoreHash)
}

func (s *ServiceImpl) SendConcepts(endpoint, jobID string, ignoreHash bool) error {
	if !s.IsDataLoaded(endpoint) {
		return errors.New("data not loaded")
	}

	responseChannel := make(chan conceptResponse)
	requestChannel := make(chan conceptRequest)

	wgResp := sync.WaitGroup{}

	defer close(requestChannel)

	// This just logs errors in sending a concept.  Successes are logged in the RW app.
	go func(responseChannel <-chan conceptResponse) {
		for r := range responseChannel {
			if r.response != nil {
				log.Errorf("Error sending concept %s [%s]: %s", r.uuid, r.jobID, r.response)
			}
		}
	}(responseChannel)

	// Creates the workers.
	wgResp.Add(s.writerWorkers)
	for i := 0; i < s.writerWorkers; i++ {
		go s.writeWorker(&wgResp, requestChannel, responseChannel, ignoreHash)
	}

	go func() {
		wgResp.Wait()
		close(responseChannel)
	}()

	// Loop through and create a request for each concept.
	concepts, ok := s.data.Load(endpoint)
	if !ok {
		return errors.New("data not loaded")
	}
	for k, v := range concepts.([]*BasicConcept) {
		log.Debugf("Sending concept to writer [%s]: %s", jobID, k)
		requestChannel <- conceptRequest{
			uuid:    v.UUID,
			jobID:   jobID,
			payload: v,
		}
	}

	return nil
}

func (s *ServiceImpl) writeWorker(wg *sync.WaitGroup, requestChannel <-chan conceptRequest, responseChannel chan<- conceptResponse, ignoreHash bool) {
	for req := range requestChannel {
		err := s.sendSingleConcept(req.theType, req.uuid, req.payload, req.jobID, ignoreHash)
		responseChannel <- conceptResponse{
			uuid:     req.uuid,
			jobID:    req.jobID,
			response: err,
		}
	}
	wg.Done()
}

func (s *ServiceImpl) sendSingleConcept(endpoint, uuid string, payload *BasicConcept, transactionID string, ignoreHash bool) error {
	fullURL, err := url.Parse(s.writerEndpoint + "/" + uuid)
	if err != nil {
		log.Errorf("Error parsing url %s: %s", s.writerEndpoint+"/"+uuid, err)
		return err
	}

	encoded, err := json.Marshal(payload)
	if err != nil {
		return err
	}

	req, err := http.NewRequest("PUT", fullURL.String(), bytes.NewReader(encoded))
	if err != nil {
		log.Errorf("Error creating request: %s", err)
		return err
	}

	if transactionID == "" {
		transactionID = transactionidutils.NewTransactionID()
	}
	req.Header.Set(transactionidutils.TransactionIDHeader, transactionID)
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-Ignore-Hash", strconv.FormatBool(ignoreHash))
	req.ContentLength = int64(len(encoded))

	resp, err := s.httpClient.Do(req)
	defer resp.Body.Close()

	if resp.StatusCode == 304 {
		log.WithFields(log.Fields{"transaction_id": transactionID, "UUID": uuid}).Debug("Concept has not been updated since last update, skipping")
		return nil
	}

	if int(resp.StatusCode/100) != 2 {
		return fmt.Errorf("bad response from writer [%d]: %s", resp.StatusCode, fullURL)
	}
	return err
}
