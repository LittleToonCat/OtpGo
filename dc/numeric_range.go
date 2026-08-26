package dc

type dcNumber interface {
	~int | ~uint | ~int64 | ~uint64 | ~float64
}

type minMax[N dcNumber] struct {
	min, max N
}

type numericRange[N dcNumber] struct {
	ranges []minMax[N]
}

func (r *numericRange[N]) isInRange(num N) bool {
	if len(r.ranges) == 0 {
		return true
	}
	for _, mm := range r.ranges {
		if num >= mm.min && num <= mm.max {
			return true
		}
	}
	return false
}

func (r *numericRange[N]) validate(num N, rangeError *bool) {
	if !r.isInRange(num) {
		*rangeError = true
	}
}

func (r *numericRange[N]) hasOneValue() bool {
	return len(r.ranges) == 1 && r.ranges[0].min == r.ranges[0].max
}

func (r *numericRange[N]) getOneValue() N {
	return r.ranges[0].min
}

func (r *numericRange[N]) generateHash(hashgen *hashGenerator) {
	if len(r.ranges) == 0 {
		return
	}
	hashgen.addInt(int32(len(r.ranges)))
	for _, mm := range r.ranges {

		hashgen.addInt(int32(mm.min))
		hashgen.addInt(int32(mm.max))
	}
}

func (r *numericRange[N]) clear() {
	r.ranges = nil
}

func (r *numericRange[N]) addRange(min, max N) bool {

	if max < min {
		return false
	}
	for _, mm := range r.ranges {
		if (min >= mm.min && min <= mm.max) ||
			(max >= mm.min && max <= mm.max) ||
			(min < mm.min && max > mm.max) {
			return false
		}
	}
	r.ranges = append(r.ranges, minMax[N]{min: min, max: max})
	return true
}

func (r *numericRange[N]) isEmpty() bool {
	return len(r.ranges) == 0
}

func (r *numericRange[N]) getNumRanges() int {
	return len(r.ranges)
}

func (r *numericRange[N]) getMin(n int) N {
	return r.ranges[n].min
}

func (r *numericRange[N]) getMax(n int) N {
	return r.ranges[n].max
}

type (
	dcIntRange           = numericRange[int]
	dcUnsignedIntRange   = numericRange[uint]
	dcInt64Range         = numericRange[int64]
	dcUnsignedInt64Range = numericRange[uint64]
	dcDoubleRange        = numericRange[float64]
)
