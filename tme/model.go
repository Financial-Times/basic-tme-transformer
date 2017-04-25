package tme

type BasicConcept struct {
	UUID           string `json:"uuid"`
	PrefLabel      string `json:"prefLabel,omitempty"`
	Type           string `json:"type,omitempty"`
	Authority      string `json:"authority,omitempty"`
	AuthorityValue string `json:"authorityValue,omitempty"`
}

type ConceptUUID struct {
	UUID string `json:"uuid"`
}

type Taxonomy struct {
	TermsT []Term `xml:"term"`
	TermsC []Term `xml:"category"`
}

type Term struct {
	CanonicalName string `xml:"name"`
	RawID         string `xml:"id"`
}
