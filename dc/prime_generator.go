package dc

type primeNumberGenerator struct {
	primes []int
}

func newPrimeNumberGenerator() *primeNumberGenerator {
	return &primeNumberGenerator{primes: []int{2}}
}

func (p *primeNumberGenerator) at(n int) int {

	candidate := p.primes[len(p.primes)-1] + 1
	for len(p.primes) <= n {

		maybePrime := true
		j := 0
		for maybePrime && p.primes[j]*p.primes[j] <= candidate {
			if p.primes[j]*(candidate/p.primes[j]) == candidate {

				maybePrime = false
			}
			j++
		}
		if maybePrime {

			p.primes = append(p.primes, candidate)
		}
		candidate++
	}
	return p.primes[n]
}
