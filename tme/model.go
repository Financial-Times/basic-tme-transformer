package tme

type BasicConcept struct {
	UUID           string   `json:"uuid"`
	ParentUUIDs    []string `json:"parentUUIDs,omitempty"`
	PrefLabel      string   `json:"prefLabel,omitempty"`
	Type           string   `json:"type,omitempty"`
	Authority      string   `json:"authority,omitempty"`
	AuthorityValue string   `json:"authorityValue,omitempty"`
	Aliases        []string `json:"aliases,omitempty"`
	IsAuthor       bool     `json:"isAuthor,omitempty"`
	IsDeprecated   *bool    `json:"isDeprecated"`
}

type ConceptUUID struct {
	UUID string `json:"uuid"`
}

type Taxonomy struct {
	TermsT []Term `xml:"term"`
	TermsC []Term `xml:"category"`
}

type Term struct {
	CanonicalName string  `xml:"name"`
	RawID         string  `xml:"id"`
	Aliases       aliases `xml:"variations"`
	Enabled       *bool   `xml:"enabled,omitempty"`
}

type aliases struct {
	Alias []alias `xml:"variation"`
}

type alias struct {
	Name string `xml:"name"`
}
