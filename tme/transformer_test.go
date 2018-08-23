package tme

import (
	"errors"
	"io/ioutil"
	"testing"

	log "github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
)

func TestTransformer_transformConcept(t *testing.T) {
	type testStruct struct {
		testName             string
		testTerm             Term
		endpoint             string
		expectedUuid         string
		expectedPrefLabel    string
		expectedType         string
		expectedParentUuid   string
		expectedAuthority    string
		expectedAuthValue    string
		expectedAliases      []string
		expectedIsAuthor     bool
		expectedIsDeprecated *bool
	}

	//Tme terms
	exampleGenre := Term{CanonicalName: "NewGenre", RawID: "newGenre", Enabled: pTrueValue}
	examplePersonWithAliases := Term{CanonicalName: "John Smith", RawID: "johnSmith", Aliases: aliases{Alias: []alias{{Name: "Johnny Boy"}, {Name: "Smithy"}}}, Enabled: pTrueValue}
	examplePersonWithoutAliases := Term{CanonicalName: "Jane Doe", RawID: "janeDoe", Enabled: pTrueValue}
	exampleBrand := Term{CanonicalName: "NewBrand", RawID: "newBrand", Enabled: pTrueValue}
	exampleAuthor := Term{CanonicalName: "Author McAuthorface", RawID: "mcAuthorFace", Aliases: aliases{Alias: []alias{{Name: "John"}, {Name: "Bob"}}}, Enabled: pTrueValue}
	exampleDeprecatedGenre := Term{CanonicalName: "OldGenre", RawID: "oldGenre", Enabled: pFalseValue}

	//Scenarios
	genreTest := testStruct{testName: "genreTest", testTerm: exampleGenre, endpoint: "genres", expectedUuid: "7c80229b-3ad4-3bee-bb7a-45eaafe3f83a", expectedType: "Genre", expectedPrefLabel: "NewGenre", expectedAliases: []string{}, expectedParentUuid: "", expectedAuthority: "TME", expectedAuthValue: "bmV3R2VucmU=-R2VucmVz", expectedIsAuthor: false, expectedIsDeprecated: pFalseValue}
	personWithAliases := testStruct{testName: "personWithAliases", testTerm: examplePersonWithAliases, endpoint: "people", expectedUuid: "05d18aac-9d8e-35d6-9a50-a950fc10aa0e", expectedType: "Person", expectedPrefLabel: "John Smith", expectedAliases: []string{"Johnny Boy", "Smithy"}, expectedParentUuid: "", expectedAuthority: "TME", expectedAuthValue: "am9oblNtaXRo-UE4=", expectedIsAuthor: false, expectedIsDeprecated: pFalseValue}
	personNoAliases := testStruct{testName: "personNoAliases", testTerm: examplePersonWithoutAliases, endpoint: "people", expectedUuid: "ee34e2fd-f363-339b-aa25-191483cb909e", expectedType: "Person", expectedPrefLabel: "Jane Doe", expectedAliases: []string{}, expectedParentUuid: "", expectedAuthority: "TME", expectedAuthValue: "amFuZURvZQ==-UE4=", expectedIsAuthor: false, expectedIsDeprecated: pFalseValue}
	brandTest := testStruct{testName: "brandTest", testTerm: exampleBrand, endpoint: "brands", expectedUuid: "dcb6cc7c-0e5b-3537-8c98-5405a52484f3", expectedType: "Brand", expectedPrefLabel: "NewBrand", expectedAliases: []string{}, expectedParentUuid: financialTimesBrandUuid, expectedAuthority: "TME", expectedAuthValue: "bmV3QnJhbmQ=-QnJhbmRz", expectedIsAuthor: false, expectedIsDeprecated: pFalseValue}
	authorTest := testStruct{testName: "authorTest", testTerm: exampleAuthor, endpoint: "authors", expectedUuid: "829f073f-6666-338f-abd6-d69c37f2e5d0", expectedType: "Person", expectedPrefLabel: "Author McAuthorface", expectedAliases: []string{"John", "Bob"}, expectedParentUuid: "", expectedAuthority: "TME", expectedAuthValue: "bWNBdXRob3JGYWNl-QXV0aG9ycw==", expectedIsAuthor: true, expectedIsDeprecated: pFalseValue}
	deprecatedGenreTest := testStruct{testName: "deprecatedGenreTest", testTerm: exampleDeprecatedGenre, endpoint: "genres", expectedUuid: "0f2b2e49-74a2-3357-ba22-80a353922dab", expectedType: "Genre", expectedPrefLabel: "OldGenre", expectedAliases: []string{}, expectedParentUuid: "", expectedAuthority: "TME", expectedAuthValue: "b2xkR2VucmU=-R2VucmVz", expectedIsAuthor: false, expectedIsDeprecated: pTrueValue}

	testScenarios := []testStruct{genreTest, personWithAliases, personNoAliases, brandTest, authorTest, deprecatedGenreTest}

	for _, scenario := range testScenarios {
		result := transformConcept(scenario.testTerm, scenario.endpoint)
		assert.NotNil(t, result)
		assert.Equal(t, scenario.expectedUuid, result.UUID, "Scenario "+scenario.testName+" failed")
		assert.Equal(t, scenario.expectedPrefLabel, result.PrefLabel, "Scenario "+scenario.testName+" failed")
		assert.Equal(t, scenario.expectedType, result.Type, "Scenario "+scenario.testName+" failed")
		if scenario.expectedParentUuid != "" {
			assert.Equal(t, scenario.expectedParentUuid, result.ParentUUIDs[0], "Scenario "+scenario.testName+" failed")
		}
		assert.Equal(t, scenario.expectedAliases, result.Aliases, "Scenario "+scenario.testName+" failed")
		assert.Equal(t, scenario.expectedAuthority, result.Authority, "Scenario "+scenario.testName+" failed")
		assert.Equal(t, scenario.expectedAuthValue, result.AuthorityValue, "Scenario "+scenario.testName+" failed")
		assert.Equal(t, scenario.expectedIsAuthor, result.IsAuthor, "Scenario "+scenario.testName+" failed")
		assert.Equal(t, scenario.expectedIsDeprecated, result.IsDeprecated, "Scenario "+scenario.testName+" failed")
	}
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
		assert.Equal(t, pTrueValue, iFace[0].(Term).Enabled)
		assert.Equal(t, "Nstein_GL_AFTM_GL_111", iFace[1].(Term).RawID)
		assert.Equal(t, pTrueValue, iFace[1].(Term).Enabled)
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

func TestTransformer_UnMarshallTermEnabledDefault(t *testing.T) {
	t.Run("Test term XML", func(t *testing.T) {
		content, err := ioutil.ReadFile("../test-data/term.xml")
		if err != nil {
			log.Errorf("Failed to read test file: %s", err)
		}
		tr := Transformer{}
		term, err := tr.UnMarshallTerm(content)
		assert.Equal(t, "'Ar'ara", term.(Term).CanonicalName)
		assert.Equal(t, pTrueValue, term.(Term).Enabled)
	})
}
