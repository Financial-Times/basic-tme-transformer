package tme

import (
	"encoding/base64"
	"encoding/xml"

	"github.com/Financial-Times/tme-reader/tmereader"
	"github.com/pborman/uuid"
	"github.com/pkg/errors"
)

type Transformer struct {
}

func (*Transformer) UnMarshallTaxonomy(contents []byte) ([]interface{}, error) {
	taxonomy := Taxonomy{}
	err := xml.Unmarshal(contents, &taxonomy)
	if err != nil {
		return nil, err
	}
	var interfaces []interface{}
	if len(taxonomy.TermsC) > 0 {
		interfaces = make([]interface{}, len(taxonomy.TermsC))
		for i, d := range taxonomy.TermsC {
			interfaces[i] = d
		}
	} else {
		interfaces = make([]interface{}, len(taxonomy.TermsT))
		for i, d := range taxonomy.TermsT {
			interfaces[i] = d
		}
	}

	return interfaces, nil
}

func (*Transformer) UnMarshallTerm(content []byte) (interface{}, error) {
	return nil, errors.New("Not Implemented")
}

func transformConcept(tmeTerm Term, endpoint string) BasicConcept {
	identifier := buildTmeIdentifier(tmeTerm.RawID, EndpointTypeMappings[endpoint]["taxonomy"].(string))
	generatedUUID := uuid.NewMD5(uuid.UUID{}, []byte(identifier)).String()
	aliasList := buildAliasList(tmeTerm.Aliases)

	return BasicConcept{
		UUID:           generatedUUID,
		PrefLabel:      tmeTerm.CanonicalName,
		Type:           EndpointTypeMappings[endpoint]["type"].(string),
		Authority:      "TME",
		AuthorityValue: identifier,
		Aliases:        aliasList,
	}
}

func buildTmeIdentifier(rawID string, tmeTermTaxonomyName string) string {
	id := base64.StdEncoding.EncodeToString([]byte(rawID))
	taxonomyName := base64.StdEncoding.EncodeToString([]byte(tmeTermTaxonomyName))
	return id + "-" + taxonomyName
}

func buildAliasList(aList aliases) []string {
	aliasList := make([]string, len(aList.Alias))
	for k, v := range aList.Alias {
		aliasList[k] = v.Name
	}
	return aliasList
}

var EndpointTypeMappings = map[string]map[string]interface{}{
	"genres": {
		"taxonomy": "Genres",
		"source":   &tmereader.KnowledgeBases{},
		"type":     "Genre",
	},
	"locations": {
		"taxonomy": "GL",
		"source":   &tmereader.AuthorityFiles{},
		"type":     "Location",
	},
	"special-reports": {
		"taxonomy": "SpecialReports",
		"source":   &tmereader.AuthorityFiles{},
		"type":     "SpecialReport",
	},
	"topics": {
		"taxonomy": "Topics",
		"source":   &tmereader.KnowledgeBases{},
		"type":     "Topic",
	},
	"subjects": {
		"taxonomy": "Subjects",
		"source":   &tmereader.KnowledgeBases{},
		"type":     "Subject",
	},
	"sections": {
		"taxonomy": "Sections",
		"source":   &tmereader.KnowledgeBases{},
		"type":     "Section",
	},
	"alphaville-series": {
		"taxonomy": "AlphavilleSeriesClassification",
		"source":   &tmereader.KnowledgeBases{},
		"type":     "AlphavilleSeries",
	},
	"people": {
		"taxonomy": "PN",
		"source":   &tmereader.AuthorityFiles{},
		"type":     "Person",
	},
}
