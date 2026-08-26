package dc

type Vector []byte

func NewVector() Vector { return Vector{} }

func DeleteVector(v Vector) {}

func (v *Vector) Add(b byte) { *v = append(*v, b) }

func (v Vector) Get(i int) byte { return v[i] }

func (v Vector) Size() int64 { return int64(len(v)) }
