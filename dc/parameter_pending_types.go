package dc

var uint32Uint8Type *classParameter

func createUint32Uint8Type() packerInterface {
	if uint32Uint8Type == nil {
		dclass := newDCClass(nil, "", true, false)
		dclass.AddField(newSimpleParameter(STUint32, 1))
		dclass.AddField(newSimpleParameter(STUint8, 1))
		uint32Uint8Type = newClassParameter(dclass)
	}
	return uint32Uint8Type
}
