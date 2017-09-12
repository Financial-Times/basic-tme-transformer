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

	"github.com/Financial-Times/tme-reader/tmereader"
	"github.com/Financial-Times/transactionid-utils-go"
	"github.com/boltdb/bolt"
	log "github.com/sirupsen/logrus"
)

type httpClient interface {
	Do(req *http.Request) (resp *http.Response, err error)
}

type Service interface {
	IsDataLoaded() bool
	GetCount(endpoint string) (int, error)
	GetAllConcepts(endpoint string) (io.PipeReader, error)
	GetConceptByUUID(endpoint, uuid string) (BasicConcept, bool, error)
	GetConceptUUIDs(endpoint string) (io.PipeReader, error)
	SendConcepts(endpoint, jobID string) error
	Reload(endpoint string) error
}

type ServiceImpl struct {
	sync.RWMutex
	repos          map[string]tmereader.Repository
	cacheFileName  string
	httpClient     httpClient
	baseURL        string
	db             *bolt.DB
	dataLoaded     bool
	maxTmeRecords  int
	writerEndpoint string
	writerWorkers  int
}

func NewService(repos map[string]tmereader.Repository, cacheFilename string, httpClient httpClient, baseURL string, maxTmeRecords int, writerEndpoint string, writerWorkers int) Service {
	svc := &ServiceImpl{
		repos:          repos,
		cacheFileName:  cacheFilename,
		httpClient:     httpClient,
		baseURL:        baseURL,
		maxTmeRecords:  maxTmeRecords,
		writerEndpoint: writerEndpoint,
		writerWorkers:  writerWorkers,
	}
	go func(service *ServiceImpl) {
		err := service.loadDB()
		if err != nil {
			log.Errorf("Error while creating service: [%v]", err.Error())
		}
	}(svc)
	return svc
}

func (s *ServiceImpl) IsDataLoaded() bool {
	s.RLock()
	defer s.RUnlock()
	return s.dataLoaded
}

func (s *ServiceImpl) setDataLoaded(val bool) {
	s.Lock()
	s.dataLoaded = val
	s.Unlock()
}

func (s *ServiceImpl) openDB() error {
	s.Lock()
	defer s.Unlock()
	log.Infof("Opening database '%v'.", s.cacheFileName)
	if s.db == nil {
		var err error
		if s.db, err = bolt.Open(s.cacheFileName, 0600, &bolt.Options{Timeout: 1 * time.Second}); err != nil {
			log.Errorf("Error opening cache file for init: %v.", err.Error())
			return err
		}
	}
	return nil
}

func (s *ServiceImpl) Reload(endpoint string) error {
	log.Infof("Reloading %s", endpoint)
	s.setDataLoaded(false)
	if err := s.openDB(); err != nil {
		return err
	}
	r, ok := s.repos[endpoint]
	if !ok {
		return errors.New("Endpoint invalid")
	}

	err := s.loadConcept(endpoint, r, nil)
	if err != nil {
		return err
	}
	s.setDataLoaded(true)
	log.Infof("Completed %s load", endpoint)
	return nil
}

func (s *ServiceImpl) loadDB() error {
	s.setDataLoaded(false)
	log.Info("Loading DB...")

	if err := s.openDB(); err != nil {
		return err
	}

	var wg sync.WaitGroup
	wg.Add(len(s.repos))
	for k := range s.repos {
		go s.loadConcept(k, s.repos[k], &wg)
	}
	wg.Wait()
	s.setDataLoaded(true)
	log.Info("Completed DB load.")
	return nil
}

func (s *ServiceImpl) loadConcept(endpoint string, repo tmereader.Repository, wg *sync.WaitGroup) error {
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
			return fmt.Errorf("Cache bucket [%v] not found!", endpoint)
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

func (s *ServiceImpl) GetCount(endpoint string) (int, error) {
	s.RLock()
	defer s.RUnlock()
	if !s.IsDataLoaded() {
		return 0, errors.New("Data not loaded")
	}

	var count int
	err := s.db.View(func(tx *bolt.Tx) error {
		bucket := tx.Bucket([]byte(endpoint))
		if bucket == nil {
			return fmt.Errorf("Bucket %v not found!", endpoint)
		}
		count = bucket.Stats().KeyN
		return nil
	})
	return count, err
}

func (s *ServiceImpl) GetAllConcepts(endpoint string) (io.PipeReader, error) {
	s.RLock()
	pv, pw := io.Pipe()

	go func() {
		defer s.RUnlock()
		defer pw.Close()
		s.db.View(func(tx *bolt.Tx) error {
			b := tx.Bucket([]byte(endpoint))
			if b == nil {
				return fmt.Errorf("Bucket %v not found!", endpoint)
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

func (s *ServiceImpl) GetConceptByUUID(endpoint, uuid string) (BasicConcept, bool, error) {
	s.RLock()
	defer s.RUnlock()
	var cachedValue []byte
	err := s.db.View(func(tx *bolt.Tx) error {
		bucket := tx.Bucket([]byte(endpoint))
		if bucket == nil {
			return fmt.Errorf("Bucket %v not found!", endpoint)
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

func (s *ServiceImpl) SendConcepts(endpoint, jobID string) error {
	s.RLock()
	defer s.RUnlock()
	responseChannel := make(chan conceptResponse)
	requestChannel := make(chan conceptRequest)
	var wgReq sync.WaitGroup
	var wgResp sync.WaitGroup

	err := s.db.View(func(tx *bolt.Tx) error {
		bucket := tx.Bucket([]byte(endpoint))
		if bucket == nil {
			return fmt.Errorf("Bucket %v not found!", endpoint)
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
			go s.writeWorker(wgResp, requestChannel, responseChannel)
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

func (s *ServiceImpl) writeWorker(wg sync.WaitGroup, requestChannel <-chan conceptRequest, responseChannel chan<- conceptResponse) {
	for req := range requestChannel {
		err := s.sendSingleConcept(req.theType, req.uuid, req.payload, req.jobID)
		responseChannel <- conceptResponse{
			uuid:     req.uuid,
			jobID:    req.jobID,
			response: err,
		}
	}
	wg.Done()
}

func (s *ServiceImpl) sendSingleConcept(endpoint, uuid, payload, transactionID string) error {
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
	req.ContentLength = int64(len(payload))

	resp, err := s.httpClient.Do(req)
	defer resp.Body.Close()
	if int(resp.StatusCode/100) != 2 {
		return fmt.Errorf("Bad response from writer [%d]: %s", resp.StatusCode, fullURL)
	}
	return err
}
