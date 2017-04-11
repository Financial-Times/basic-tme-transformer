package tme

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"

	"github.com/Financial-Times/tme-reader/tmereader"
	"github.com/Financial-Times/transactionid-utils-go"
	log "github.com/Sirupsen/logrus"
	"github.com/boltdb/bolt"
)

type httpClient interface {
	Do(req *http.Request) (resp *http.Response, err error)
}

type Service struct {
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

func NewService(repos map[string]tmereader.Repository, cacheFilename string, httpClient httpClient, baseURL string, maxTmeRecords int, writerEndpoint string, writerWorkers int) *Service {
	svc := &Service{
		repos:          repos,
		cacheFileName:  cacheFilename,
		httpClient:     httpClient,
		baseURL:        baseURL,
		maxTmeRecords:  maxTmeRecords,
		writerEndpoint: writerEndpoint,
		writerWorkers:  writerWorkers,
	}
	go func(service *Service) {
		err := service.loadDB()
		if err != nil {
			log.Errorf("Error while creating service: [%v]", err.Error())
		}
	}(svc)
	return svc
}

func (s *Service) isDataLoaded() bool {
	s.RLock()
	defer s.RUnlock()
	return s.dataLoaded
}

func (s *Service) setDataLoaded(val bool) {
	s.Lock()
	s.dataLoaded = val
	s.Unlock()
}

func (s *Service) openDB() error {
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

func (s *Service) loadDB() error {
	s.setDataLoaded(false)
	wgl := make(map[string]*sync.WaitGroup)
	processingChannels := make(map[string]chan []BasicConcept)
	log.Info("Loading DB...")

	if err := s.openDB(); err != nil {
		return err
	}

	for k := range EndpointTypeMappings {
		err := s.createCacheBucket(k)
		log.Infof("Loading %s", k)
		if err != nil {
			return err
		}

		processingChannels[k] = make(chan []BasicConcept)
		wgl[k] = new(sync.WaitGroup)
		go s.processConcepts(processingChannels[k], k, wgl[k])
		defer func(w *sync.WaitGroup, c chan []BasicConcept) {
			close(c)
			w.Wait()
		}(wgl[k], processingChannels[k])

		responseCount := 0
		for {
			terms, err := s.repos[k].GetTmeTermsFromIndex(responseCount)
			if err != nil {
				return err
			}
			if len(terms) < 1 {
				log.Infof("Finished fetching %s from TME. Waiting subroutines to terminate.", k)
				break
			}

			wgl[k].Add(1)
			s.processTerms(terms, k, processingChannels[k])
			responseCount += s.maxTmeRecords
		}
	}

	return nil
}

func (s *Service) checkAllLoaded() bool {
	for k := range EndpointTypeMappings {
		if i, _ := s.getCount(k); i <= 0 {
			return false
		}
	}
	return true
}

func (s *Service) processTerms(terms []interface{}, taxonomy string, c chan<- []BasicConcept) {
	log.Infof("Processing %s...", taxonomy)
	var cacheToBeWritten []BasicConcept
	for _, iTerm := range terms {
		t := iTerm.(Term)
		cacheToBeWritten = append(cacheToBeWritten, transformConcept(t, taxonomy))
	}
	c <- cacheToBeWritten
}

func (s *Service) processConcepts(c <-chan []BasicConcept, taxonomy string, wg *sync.WaitGroup) {
	for concepts := range c {
		log.Infof("Processing batch of %v %s.", len(concepts), taxonomy)
		if err := s.db.Batch(func(tx *bolt.Tx) error {
			bucket := tx.Bucket([]byte(taxonomy))
			if bucket == nil {
				return fmt.Errorf("Cache bucket [%v] not found!", taxonomy)
			}
			for _, aConcept := range concepts {
				marshalledConcept, err := json.Marshal(aConcept)
				if err != nil {
					return err
				}
				err = bucket.Put([]byte(aConcept.UUID), marshalledConcept)
				if err != nil {
					return err
				}
			}
			return nil
		}); err != nil {
			log.Errorf("Error storing to cache: %+v.", err)
		}
		wg.Done()
	}

	log.Infof("Finished processing all %s.", taxonomy)
	s.setDataLoaded(true)
}

func (s *Service) createCacheBucket(taxonomy string) error {
	return s.db.Update(func(tx *bolt.Tx) error {
		if tx.Bucket([]byte(taxonomy)) != nil {
			log.Infof("Deleting bucket '%v'.", taxonomy)
			if err := tx.DeleteBucket([]byte(taxonomy)); err != nil {
				log.Warnf("Cache bucket [%v] could not be deleted.", taxonomy)
			}
		}
		log.Infof("Creating bucket '%s'.", taxonomy)
		_, err := tx.CreateBucket([]byte(taxonomy))
		return err
	})
}

func (s *Service) getCount(endpoint string) (int, error) {
	s.RLock()
	defer s.RUnlock()
	if !s.isDataLoaded() {
		return 0, nil
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

func (s *Service) getAllConcepts(endpoint string) (io.PipeReader, error) {
	s.RLock()
	pv, pw := io.Pipe()
	go func() {
		defer s.RUnlock()
		defer pw.Close()
		s.db.View(func(tx *bolt.Tx) error {
			b := tx.Bucket([]byte(endpoint))
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

func (s *Service) getConceptByUUID(endpoint, uuid string) (BasicConcept, bool, error) {
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

func (s *Service) getConceptUUIDs(endpoint string) (io.PipeReader, error) {
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

func (s *Service) sendConcepts(endpoint, jobID string) error {

	responseChannel := make(chan conceptResponse)
	requestChannel := make(chan conceptRequest)
	var wgReq sync.WaitGroup
	var wgResp sync.WaitGroup

	err := s.db.View(func(tx *bolt.Tx) error {
		bucket := tx.Bucket([]byte(endpoint))
		if bucket == nil {
			return fmt.Errorf("Bucket %v not found!", endpoint)
		}

		go func(responseChannel <-chan conceptResponse) {
			for r := range responseChannel {
				if r.response != nil {
					log.Errorf("Error sending concept %s [%s]: %s", r.uuid, r.jobID, r.response)
				}
			}
		}(responseChannel)

		wgResp.Add(s.writerWorkers)
		for i := 0; i < s.writerWorkers; i++ {
			go s.writeWorker(wgResp, requestChannel, responseChannel)
		}

		go func() {
			wgResp.Wait()
			close(responseChannel)
		}()

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

func (s *Service) writeWorker(wg sync.WaitGroup, requestChannel <-chan conceptRequest, responseChannel chan<- conceptResponse) {
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

func (s *Service) sendSingleConcept(endpoint, uuid, payload, transactionID string) error {
	fullURL, err := url.Parse(s.writerEndpoint + "/" + uuid)
	if err != nil {
		log.Errorf("Error parsing url %s: %s", s.writerEndpoint+"/"+endpoint+"/"+uuid, err)
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
		log.Errorf("Bad response from writer [%d]: %s", resp.StatusCode, fullURL)
		return err
	}
	return err
}
