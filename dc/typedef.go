package dc

type dcTypedef struct {
	parameter       packerInterface
	bogusTypedef    bool
	implicitTypedef bool
	number          int
}

func newDCTypedef(parameter packerInterface, implicit bool) *dcTypedef {
	return &dcTypedef{parameter: parameter, implicitTypedef: implicit, number: -1}
}

func newBogusDCTypedef(name string) *dcTypedef {
	sp := newSimpleParameter(STInvalid, 1)
	sp.SetName(name)
	return &dcTypedef{parameter: sp, bogusTypedef: true, number: -1}
}

func (t *dcTypedef) GetNumber() int { return t.number }

func (t *dcTypedef) GetName() string { return t.parameter.Name() }

func (t *dcTypedef) IsBogusTypedef() bool { return t.bogusTypedef }

func (t *dcTypedef) IsImplicitTypedef() bool { return t.implicitTypedef }

func (t *dcTypedef) SetNumber(n int) { t.number = n }

func (t *dcTypedef) MakeNewParameter() packerInterface {
	newParameter := t.parameter.MakeCopy()
	newParameter.SetName("")
	if fb, ok := newParameter.(interface{ SetFromTypedef(bool) }); ok {
		fb.SetFromTypedef(true)
	}
	return newParameter
}
