package dc

func SkipField(field DCField, data []byte, offset int) (newOffset int, ok bool) {
	return walkField(field, data, offset, false, nil)
}

func UnpackField(field DCField, data []byte, offset int, validate bool) (value []byte, newOffset int, ok bool) {
	var rangeErr bool
	end, wok := walkField(field, data, offset, validate, &rangeErr)
	if !wok || rangeErr {
		return nil, end, false
	}
	return append([]byte(nil), data[offset:end]...), end, true
}

func walkField(f packerInterface, data []byte, offset int, validate bool, rangeErr *bool) (end int, ok bool) {
	defer func() {
		if recover() != nil {
			ok = false
		}
	}()
	p := offset
	if !walkValue(f, data, &p, validate, rangeErr) {
		return p, false
	}
	return p, true
}

func walkValue(f packerInterface, data []byte, p *int, validate bool, rangeErr *bool) bool {
	if *p < 0 || *p > len(data) {
		return false
	}

	scopeStart := *p
	var packErr bool
	var single bool
	if validate {
		single = f.UnpackValidate(data, p, &packErr, rangeErr)
	} else {
		single = f.UnpackSkip(data, p, &packErr)
	}
	if single {
		return !packErr && *p >= scopeStart && *p <= len(data)
	}
	if packErr {
		return false
	}

	parent := f
	n := parent.NumNestedFields()
	popMarker := -1

	if lb := parent.NumLengthBytes(); lb != 0 {
		if *p+lb > len(data) {
			return false
		}
		var length int
		if lb == 4 {
			length = int(doUnpackUint32(data[*p : *p+4]))
			*p += 4
		} else {
			length = int(doUnpackUint16(data[*p : *p+2]))
			*p += 2
		}
		popMarker = *p + length
		if popMarker > len(data) {
			return false
		}
		if length == 0 {
			n = 0
		} else {
			n = parent.CalcNumNestedFields(length)
		}
	}

	for i := 0; ; i++ {
		if n >= 0 && i >= n {
			break
		}
		if popMarker >= 0 && *p >= popMarker {
			break
		}
		child := parent.GetNestedField(i)
		if child == nil {
			return false
		}
		if !walkValue(child, data, p, validate, rangeErr) {
			return false
		}
		if sw, isSwitch := parent.(*switchParameter); isSwitch {
			parent = sw.applySwitch(data[scopeStart:*p])
			if parent == nil {
				return false
			}
			n = parent.NumNestedFields()
		}
	}

	if popMarker >= 0 && *p != popMarker {
		return false
	}
	return *p <= len(data)
}
