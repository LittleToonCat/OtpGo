package dc

const maxPrimeNumbers = 10000

type hashGenerator struct {
	hash   int64
	index  int
	primes *primeNumberGenerator
}

func newHashGenerator() *hashGenerator {
	return &hashGenerator{primes: newPrimeNumberGenerator()}
}

func (h *hashGenerator) addInt(num int32) {
	h.hash += int64(h.primes.at(h.index)) * int64(num)
	h.index = (h.index + 1) % maxPrimeNumbers
}

func (h *hashGenerator) addString(str string) {
	h.addInt(int32(len(str)))
	for i := 0; i < len(str); i++ {
		h.addInt(int32(int8(str[i])))
	}
}

func (h *hashGenerator) addBlob(bytes []byte) {
	h.addInt(int32(len(bytes)))
	for _, b := range bytes {
		h.addInt(int32(b))
	}
}

func (h *hashGenerator) getHash() uint32 {
	return uint32(h.hash & 0xffffffff)
}
