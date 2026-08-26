package dc

import "sort"

type keywordList struct {
	keywords       []*keyword
	keywordsByName map[string]*keyword
	flags          int32
}

func newKeywordList() *keywordList {
	return &keywordList{keywordsByName: make(map[string]*keyword)}
}

func (kl *keywordList) HasKeywordName(name string) bool {
	_, ok := kl.keywordsByName[name]
	return ok
}

func (kl *keywordList) HasKeyword(kw *keyword) bool {
	return kl.HasKeywordName(kw.Name())
}

func (kl *keywordList) GetNumKeywords() int {
	return len(kl.keywords)
}

func (kl *keywordList) GetKeyword(n int) *keyword {
	return kl.keywords[n]
}

func (kl *keywordList) GetKeywordByName(name string) *keyword {
	return kl.keywordsByName[name]
}

func (kl *keywordList) CompareKeywords(other *keywordList) bool {
	if len(kl.keywordsByName) != len(other.keywordsByName) {
		return false
	}
	for name, kw := range kl.keywordsByName {
		otherKw, ok := other.keywordsByName[name]
		if !ok || otherKw != kw {
			return false
		}
	}
	return true
}

func (kl *keywordList) CopyKeywords(other *keywordList) {
	kl.keywords = append([]*keyword(nil), other.keywords...)
	kl.keywordsByName = make(map[string]*keyword, len(other.keywordsByName))
	for k, v := range other.keywordsByName {
		kl.keywordsByName[k] = v
	}
	kl.flags = other.flags
}

func (kl *keywordList) AddKeyword(kw *keyword) bool {
	if _, exists := kl.keywordsByName[kw.Name()]; exists {
		return false
	}
	kl.keywordsByName[kw.Name()] = kw
	kl.keywords = append(kl.keywords, kw)
	kl.flags |= kw.HistoricalFlag()
	return true
}

func (kl *keywordList) ClearKeywords() {
	kl.keywords = nil
	kl.keywordsByName = make(map[string]*keyword)
	kl.flags = 0
}

func (kl *keywordList) generateHash(hashgen *hashGenerator) {
	if kl.flags != historicalFlagCustom {
		hashgen.addInt(kl.flags)
		return
	}

	hashgen.addInt(int32(len(kl.keywordsByName)))
	names := make([]string, 0, len(kl.keywordsByName))
	for name := range kl.keywordsByName {
		names = append(names, name)
	}
	sort.Strings(names)
	for _, name := range names {
		kl.keywordsByName[name].GenerateHash(hashgen)
	}
}
