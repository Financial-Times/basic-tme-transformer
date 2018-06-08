package tme

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"

	"strconv"

	"github.com/Financial-Times/tme-reader/tmereader"
	"github.com/Financial-Times/transactionid-utils-go"
	"github.com/boltdb/bolt"
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
	GetConceptByUUID(endpoint, uuid string) (BasicConcept, bool, error)
	GetConceptUUIDs(endpoint string) (io.PipeReader, error)
	SendConcepts(endpoint, jobID string, ignoreHash bool) error
	RefreshConceptByUUID(endpoint, uuid string) (BasicConcept, bool, error)
	SendConceptByUUID(txID, endpoint, uuid string, ignoreHash bool) error
	Reload(endpoint string) error
	GetLoadedTypes() []string
}

// ServiceImpl - implements the interface above
type ServiceImpl struct {
	sync.RWMutex
	repos          map[string]tmereader.Repository
	cacheFileName  string
	httpClient     httpClient
	baseURL        string
	db             *bolt.DB
	dataLoaded     map[string]bool
	maxTmeRecords  int
	writerEndpoint string
	writerWorkers  int
}

// NewService - creates an instance of Service
func NewService(repos map[string]tmereader.Repository, cacheFilename string, httpClient httpClient, baseURL string, maxTmeRecords int, writerEndpoint string, writerWorkers int) Service {
	svc := &ServiceImpl{
		repos:          repos,
		cacheFileName:  cacheFilename,
		httpClient:     httpClient,
		baseURL:        baseURL,
		maxTmeRecords:  maxTmeRecords,
		writerEndpoint: writerEndpoint,
		writerWorkers:  writerWorkers,
		dataLoaded:     map[string]bool{},
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
	s.RLock()
	defer s.RUnlock()
	return s.dataLoaded[endpoint]
}

func (s *ServiceImpl) setDataLoaded(endpoint string, val bool) {
	s.Lock()
	s.dataLoaded[endpoint] = val
	s.Unlock()
}

func (s *ServiceImpl) openDB() error {
	s.Lock()
	defer s.Unlock()
	log.Infof("Opening database '%v'.", s.cacheFileName)
	if s.db == nil {
		os.Remove(s.cacheFileName)
		var err error
		if s.db, err = bolt.Open(s.cacheFileName, 0600, &bolt.Options{Timeout: 1 * time.Second}); err != nil {
			log.Errorf("Error opening cache file for init: %v.", err.Error())
			return err
		}
	}
	return nil
}

// Reload - Reloads the data for a specific endpoint
func (s *ServiceImpl) Reload(endpoint string) error {
	log.Infof("Reloading %s", endpoint)
	s.setDataLoaded(endpoint, false)
	if err := s.openDB(); err != nil {
		return err
	}
	r, ok := s.repos[endpoint]
	if !ok {
		return errors.New("endpoint invalid")
	}

	err := s.loadConcept(endpoint, r, nil)
	if err != nil {
		return err
	}
	s.setDataLoaded(endpoint, true)
	log.Infof("Completed %s load", endpoint)
	return nil
}

func (s *ServiceImpl) loadDB() error {
	log.Info("Loading DB...")

	if err := s.openDB(); err != nil {
		return err
	}

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
	s.setDataLoaded(endpoint, false)
	log.Infof("Loading %s", endpoint)
	err := s.createCacheBucket(endpoint)
	if err != nil {
		return err
	}

	var fullTerms []interface{}

	responseCount := 0
	for {
		terms, err := repo.GetTmeTermsFromIndex(responseCount)
		if err != nil {
			return err
		}

		if len(terms) < 1 {
			log.Infof("Fetched %d %s from TME.", len(fullTerms), endpoint)
			break
		}
		log.Infof("Found %d %s.", len(terms), endpoint)
		responseCount += s.maxTmeRecords
		fullTerms = append(fullTerms, terms...)
	}

	if err := s.db.Batch(func(tx *bolt.Tx) error {
		bucket := tx.Bucket([]byte(endpoint))
		if bucket == nil {
			return fmt.Errorf("cache bucket [%v] not found", endpoint)
		}
		for _, t := range fullTerms {
			concept := transformConcept(t.(Term), endpoint)
			marshalledConcept, err := json.Marshal(concept)
			if err != nil {
				return err
			}
			err = bucket.Put([]byte(concept.UUID), marshalledConcept)
			if err != nil {
				return err
			}
		}
		return nil
	}); err != nil {
		log.Errorf("Error storing to cache: %+v.", err)
	}
	log.Infof("Finished processing %s", endpoint)
	s.setDataLoaded(endpoint, true)
	if wg != nil {
		wg.Done()
	}
	return nil
}

func (s *ServiceImpl) createCacheBucket(taxonomy string) error {
	return s.db.Update(func(tx *bolt.Tx) error {
		if tx.Bucket([]byte(taxonomy)) != nil {
			log.Debugf("Deleting bucket '%v'.", taxonomy)
			if err := tx.DeleteBucket([]byte(taxonomy)); err != nil {
				log.Warnf("Cache bucket [%v] could not be deleted.", taxonomy)
			}
		}
		log.Infof("Creating bucket '%s'.", taxonomy)
		_, err := tx.CreateBucket([]byte(taxonomy))
		return err
	})
}

// GetCount - returns record count for a specific endpoint
func (s *ServiceImpl) GetCount(endpoint string) (int, error) {
	s.RLock()
	defer s.RUnlock()
	if !s.IsDataLoaded(endpoint) {
		return 0, errors.New("data not loaded")
	}

	var count int
	err := s.db.View(func(tx *bolt.Tx) error {
		bucket := tx.Bucket([]byte(endpoint))
		if bucket == nil {
			return fmt.Errorf("bucket %v not found", endpoint)
		}
		count = bucket.Stats().KeyN
		return nil
	})
	return count, err
}

// GetAllConcepts - returns a stream of all concepts for an endpoint
func (s *ServiceImpl) GetAllConcepts(endpoint string) (io.PipeReader, error) {
	s.RLock()
	pv, pw := io.Pipe()

	go func() {
		defer s.RUnlock()
		defer pw.Close()
		s.db.View(func(tx *bolt.Tx) error {
			b := tx.Bucket([]byte(endpoint))
			if b == nil {
				return fmt.Errorf("bucket %v not found", endpoint)
			}
			c := b.Cursor()
			for k, v := c.First(); k != nil; k, v = c.Next() {
				if _, err := pw.Write(v); err != nil {
					return err
				}
				io.WriteString(pw, "\n")
			}
			return nil
		})
	}()
	return *pv, nil
}

// GetConceptByUUID - return a specific concept from a specific endpoint.
func (s *ServiceImpl) GetConceptByUUID(endpoint, uuid string) (BasicConcept, bool, error) {
	s.RLock()
	defer s.RUnlock()
	var cachedValue []byte
	err := s.db.View(func(tx *bolt.Tx) error {
		bucket := tx.Bucket([]byte(endpoint))
		if bucket == nil {
			return fmt.Errorf("bucket %v not found", endpoint)
		}
		cachedValue = bucket.Get([]byte(uuid))
		return nil
	})

	if err != nil {
		log.Errorf("Error reading from cache file for [%v]: %v", uuid, err.Error())
		return BasicConcept{}, false, err
	}
	if len(cachedValue) == 0 {
		log.Infof("No cached value for [%v].", uuid)
		return BasicConcept{}, false, nil
	}

	var cachedConcept BasicConcept
	if err := json.Unmarshal(cachedValue, &cachedConcept); err != nil {
		log.Errorf("Error unmarshalling cached value for [%v]: %v.", uuid, err.Error())
		return BasicConcept{}, true, err
	}
	return cachedConcept, true, nil
}

// RefreshConceptByUUID - updates and returns a specific concept.
func (s *ServiceImpl) RefreshConceptByUUID(endpoint, uuid string) (BasicConcept, bool, error) {
	term, err := s.repos[endpoint].GetTmeTermById(uuid)
	if err != nil {
		return BasicConcept{}, false, err
	}

	concept := transformConcept(term.(Term), endpoint)
	marshalledConcept, err := s.marshal(concept)
	if err != nil {
		return BasicConcept{}, false, err
	}

	s.Lock()
	defer s.Unlock()

	if err := s.db.Batch(func(tx *bolt.Tx) error {
		bucket := tx.Bucket([]byte(endpoint))
		if bucket == nil {
			return fmt.Errorf("cache bucket [%v] not found", endpoint)
		}

		if err := bucket.Put([]byte(concept.UUID), marshalledConcept); err != nil {
			return err
		}

		return nil
	}); err != nil {
		log.Errorf("Error storing to cache: %+v.", err)
	}

	return concept, true, nil
}

func (s *ServiceImpl) GetConceptUUIDs(endpoint string) (io.PipeReader, error) {
	s.RLock()
	pv, pw := io.Pipe()
	go func() {
		defer s.RUnlock()
		defer pw.Close()
		s.db.View(func(tx *bolt.Tx) error {
			b := tx.Bucket([]byte(endpoint))
			c := b.Cursor()
			encoder := json.NewEncoder(pw)
			for k, _ := c.First(); k != nil; k, _ = c.Next() {
				if k == nil {
					break
				}
				pl := ConceptUUID{UUID: string(k[:])}
				if err := encoder.Encode(pl); err != nil {
					return err
				}
			}
			return nil
		})
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
	payload string
}

func (s *ServiceImpl) SendConceptByUUID(txID, endpoint, uuid string, ignoreHash bool) error {
	concept, ok, err := s.RefreshConceptByUUID(endpoint, uuid)
	if err != nil {
		return err
	} else if !ok {
		return errors.New("concept not found")
	}

	marshalledConcept, err := json.Marshal(concept)
	if err != nil {
		return err
	}

	return s.sendSingleConcept(endpoint, concept.UUID, string(marshalledConcept), txID, ignoreHash)
}

func (s *ServiceImpl) SendConcepts(endpoint, jobID string, ignoreHash bool) error {
	s.RLock()
	defer s.RUnlock()
	responseChannel := make(chan conceptResponse)
	requestChannel := make(chan conceptRequest)
	var wgReq sync.WaitGroup
	var wgResp sync.WaitGroup

	err := s.db.View(func(tx *bolt.Tx) error {
		bucket := tx.Bucket([]byte(endpoint))
		if bucket == nil {
			return fmt.Errorf("bucket %v not found", endpoint)
		}

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
		wgReq.Add(bucket.Stats().KeyN)
		err := bucket.ForEach(func(k, v []byte) error {
			log.Debugf("Sending concept to writer [%s]: %s", jobID, k)

			requestChannel <- conceptRequest{
				uuid:    string(k),
				jobID:   jobID,
				payload: string(v),
			}
			wgReq.Done()

			return nil
		})

		go func() {
			wgReq.Wait()
			close(requestChannel)
		}()

		return err
	})
	return err
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

func (s *ServiceImpl) sendSingleConcept(endpoint, uuid, payload, transactionID string, ignoreHash bool) error {
	fullURL, err := url.Parse(s.writerEndpoint + "/" + uuid)
	if err != nil {
		log.Errorf("Error parsing url %s: %s", s.writerEndpoint+"/"+uuid, err)
		return err
	}
	req, err := http.NewRequest("PUT", fullURL.String(), strings.NewReader(payload))
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
	req.ContentLength = int64(len(payload))

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
