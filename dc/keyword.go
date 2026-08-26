package dc

const historicalFlagCustom int32 = -1

type keyword struct {
	name           string
	historicalFlag int32
}

func newKeyword(name string, historicalFlag int32) *keyword {
	return &keyword{name: name, historicalFlag: historicalFlag}
}

func newCustomKeyword(name string) *keyword {
	return newKeyword(name, historicalFlagCustom)
}

func (k *keyword) Name() string { return k.name }

func (k *keyword) HistoricalFlag() int32 { return k.historicalFlag }

func (k *keyword) ClearHistoricalFlag() {
	k.historicalFlag = historicalFlagCustom
}

func (k *keyword) GenerateHash(hashgen *hashGenerator) {
	hashgen.addString(k.name)
}
