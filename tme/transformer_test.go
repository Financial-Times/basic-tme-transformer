package tme

import (
	"errors"
	"io/ioutil"
	"testing"

	log "github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
)

const (
	taxonomyName = "topics"
)

func TestTransformer_transformConcept(t *testing.T) {
	testTerm := Term{
		CanonicalName: "Bob",
		RawID:         "bob",
	}
	tfp := transformConcept(testTerm,
		taxonomyName)
	log.Infof("Got concept: %v", tfp)
	assert.NotNil(t, tfp)
	assert.Equal(t, "c0e2b109-2212-35c1-8645-2a13bc2cc3db", tfp.UUID)
	assert.Equal(t, "Bob", tfp.PrefLabel)
}

func TestTransformer_UnMarshallTaxonomy(t *testing.T) {

	t.Run("Test terms XML", func(t *testing.T) {
		content, err := ioutil.ReadFile("../test-data/terms.xml")
		if err != nil {
			log.Errorf("Failed to read test file: %s", err)
		}
		tr := Transformer{}
		iFace, err := tr.UnMarshallTaxonomy(content)
		assert.Equal(t, "A term", iFace[0].(Term).CanonicalName)
		assert.Equal(t, "Nstein_GL_AFTM_GL_123456", iFace[0].(Term).RawID)
	})

	t.Run("Test categories XML", func(t *testing.T) {
		content, err := ioutil.ReadFile("../test-data/categories.xml")
		if err != nil {
			log.Errorf("Failed to read test file: %s", err)
		}
		tr := Transformer{}
		iFace, err := tr.UnMarshallTaxonomy(content)
		assert.Equal(t, "A term", iFace[0].(Term).CanonicalName)
		assert.Equal(t, "Nstein_GL_AFTM_GL_123456", iFace[0].(Term).RawID)
	})

	t.Run("Test bad XML", func(t *testing.T) {
		content, err := ioutil.ReadFile("../test-data/bad-xml.xml")
		if err != nil {
			log.Errorf("Failed to read test file: %s", err)
		}
		tr := Transformer{}
		_, err = tr.UnMarshallTaxonomy(content)
		assert.Error(t, err)
	})
}

func TestTransformer_UnMarshallTerm(t *testing.T) {
	var content []byte
	tr := Transformer{}
	iFace, err := tr.UnMarshallTerm(content)
	assert.Nil(t, iFace)
	assert.Error(t, err, errors.New("Not Implemented"))
}
