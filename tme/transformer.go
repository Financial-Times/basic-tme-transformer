package tme

import (
	"encoding/base64"
	"encoding/xml"

	"github.com/Financial-Times/tme-reader/tmereader"
	"github.com/pborman/uuid"
)

const financialTimesBrandUuid = "dbb0bdae-1f0c-11e4-b0cb-b2227cce2b54"

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
			if d.Enabled == nil {
				d.Enabled = new(bool)
				*d.Enabled = true
			}
			interfaces[i] = d
		}
	} else {
		interfaces = make([]interface{}, len(taxonomy.TermsT))
		for i, d := range taxonomy.TermsT {
			if d.Enabled == nil {
				d.Enabled = new(bool)
				*d.Enabled = true
			}
			interfaces[i] = d
		}
	}

	return interfaces, nil
}

func (*Transformer) UnMarshallTerm(content []byte) (interface{}, error) {
	term := Term{}
	err := xml.Unmarshal(content, &term)
	if err != nil {
		return nil, err
	}
	if term.Enabled == nil {
		term.Enabled = new(bool)
		*term.Enabled = true
	}
	return term, nil
}

func transformConcept(tmeTerm Term, endpoint string) *BasicConcept {
	identifier := buildTmeIdentifier(tmeTerm.RawID, EndpointTypeMappings[endpoint]["taxonomy"].(string))
	generatedUUID := uuid.NewMD5(uuid.UUID{}, []byte(identifier)).String()
	aliasList := buildAliasList(tmeTerm.Aliases)
	basicConcept := &BasicConcept{
		UUID:           generatedUUID,
		PrefLabel:      tmeTerm.CanonicalName,
		Type:           EndpointTypeMappings[endpoint]["type"].(string),
		Authority:      "TME",
		AuthorityValue: identifier,
		Aliases:        aliasList,
		IsDeprecated:   false,
	}
	if tmeTerm.Enabled != nil && *tmeTerm.Enabled == false {
		basicConcept.IsDeprecated = true
	}
	if (EndpointTypeMappings[endpoint]["taxonomy"].(string)) == "Brands" {
		basicConcept.ParentUUIDs = []string{financialTimesBrandUuid}
	}
	return basicConcept
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
	"alphaville-series": {
		"taxonomy": "AlphavilleSeriesClassification",
		"source":   &tmereader.KnowledgeBases{},
		"type":     "AlphavilleSeries",
	},
	"authors": {
		"taxonomy": "Authors",
		"source":   &tmereader.AuthorityFiles{},
		"type":     "Person",
	},
	"brands": {
		"taxonomy": "Brands",
		"source":   &tmereader.AuthorityFiles{},
		"type":     "Brand",
	},
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
	"people": {
		"taxonomy": "PN",
		"source":   &tmereader.AuthorityFiles{},
		"type":     "Person",
	},
	"sections": {
		"taxonomy": "Sections",
		"source":   &tmereader.KnowledgeBases{},
		"type":     "Section",
	},
	"special-reports": {
		"taxonomy": "SpecialReports",
		"source":   &tmereader.AuthorityFiles{},
		"type":     "SpecialReport",
	},
	"subjects": {
		"taxonomy": "Subjects",
		"source":   &tmereader.KnowledgeBases{},
		"type":     "Subject",
	},
	"topics": {
		"taxonomy": "Topics",
		"source":   &tmereader.KnowledgeBases{},
		"type":     "Topic",
	},
	"organisations": {
		"taxonomy": "ON",
		"source":   &tmereader.AuthorityFiles{},
		"type":     "Organisation",
	},
}
