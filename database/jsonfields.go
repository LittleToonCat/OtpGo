package database

import (
	"encoding/base64"
	"encoding/json"
	"fmt"

	"otpgo/dc"
	. "otpgo/util"

	"github.com/apex/log"
)

func UnpackDataToArray(unpacker *dc.DCPacker, array *[]interface{}, log log.Entry) {
	switch unpacker.GetPackType() {
	case dc.PTInvalid:
		log.Errorf("UnpackDataToArray: PTInvalid reached!\n%s", DumpUnpacker(unpacker))
		*array = append(*array, "invalid")
	case dc.PTDouble:
		*array = append(*array, unpacker.UnpackDouble())
	case dc.PTInt:
		*array = append(*array, unpacker.UnpackInt())
	case dc.PTUint:
		*array = append(*array, unpacker.UnpackUint())
	case dc.PTInt64:
		*array = append(*array, unpacker.UnpackInt64())
	case dc.PTUint64:
		*array = append(*array, unpacker.UnpackUint64())
	case dc.PTString:
		*array = append(*array, unpacker.UnpackString())
	case dc.PTBlob:
		data := unpacker.UnpackBlob()
		*array = append(*array, data)
	default:
		nestedArray := []interface{}{}
		unpacker.Push()
		for unpacker.MoreNestedFields() {
			UnpackDataToArray(unpacker, &nestedArray, log)
		}
		unpacker.Pop()
		*array = append(*array, nestedArray)
	}

	log.Debugf("Resulting Array: %v", *array)
}

func UnpackDataToDocument(unpacker *dc.DCPacker, name string, doc map[string]interface{}, log log.Entry) {
	switch unpacker.GetPackType() {
	case dc.PTInvalid:
		log.Errorf("UnpackDataToDocument: PTInvalid reached!\n%s", DumpUnpacker(unpacker))
	case dc.PTDouble:
		doc[name] = unpacker.UnpackDouble()
	case dc.PTInt:
		doc[name] = unpacker.UnpackInt()
	case dc.PTUint:
		doc[name] = unpacker.UnpackUint()
	case dc.PTInt64:
		doc[name] = unpacker.UnpackInt64()
	case dc.PTUint64:
		doc[name] = unpacker.UnpackUint64()
	case dc.PTString:
		doc[name] = unpacker.UnpackString()
	case dc.PTBlob:
		data := unpacker.UnpackBlob()
		doc[name] = data
	default:
		array := []interface{}{}
		unpacker.Push()
		for unpacker.MoreNestedFields() {
			UnpackDataToArray(unpacker, &array, log)
		}
		unpacker.Pop()
		doc[name] = array
	}

	log.Debugf("Resulting Document: %v", doc)
}

func PackValue(packer *dc.DCPacker, value interface{}, log log.Entry) {
	if array, ok := value.([]interface{}); ok && len(array) == 1 {
		switch packer.GetPackType() {
		case dc.PTDouble, dc.PTInt, dc.PTUint, dc.PTInt64, dc.PTUint64, dc.PTString, dc.PTBlob:
			value = array[0]
		}
	}

	if value == nil {
		switch packer.GetPackType() {
		case dc.PTString, dc.PTBlob:
			packer.PackString("")
			return
		case dc.PTDouble:
			packer.PackDouble(0)
			return
		case dc.PTInt:
			packer.PackInt(0)
			return
		case dc.PTUint:
			packer.PackUint(0)
			return
		case dc.PTInt64:
			packer.PackInt64(0)
			return
		case dc.PTUint64:
			packer.PackUint64(0)
			return
		}
	}

	switch packer.GetPackType() {
	case dc.PTInvalid:
		// TODO: Error out
	case dc.PTDouble:
		if double, ok := value.(float64); ok {
			packer.PackDouble(double)
		} else if intValue, ok := value.(int64); ok {
			packer.PackDouble(float64(intValue))
		} else if intValue, ok := value.(int); ok {
			packer.PackDouble(float64(intValue))
		} else if jsonNumber, ok := value.(json.Number); ok {
			if floatVal, err := jsonNumber.Float64(); err == nil {
				packer.PackDouble(floatVal)
			}
		}
	case dc.PTInt:
		if intValue, ok := value.(int64); ok {
			packer.PackInt(int(intValue))
		} else if intValue, ok := value.(int); ok {
			packer.PackInt(intValue)
		} else if floatValue, ok := value.(float64); ok {
			packer.PackInt(int(floatValue))
		} else if jsonNumber, ok := value.(json.Number); ok {
			if intVal, err := jsonNumber.Int64(); err == nil {
				packer.PackInt(int(intVal))
			}
		}
	case dc.PTUint:
		if intValue, ok := value.(int64); ok {
			packer.PackUint(uint(intValue))
		} else if intValue, ok := value.(int); ok {
			packer.PackUint(uint(intValue))
		} else if floatValue, ok := value.(float64); ok {
			packer.PackUint(uint(floatValue))
		} else if jsonNumber, ok := value.(json.Number); ok {
			if intVal, err := jsonNumber.Int64(); err == nil {
				packer.PackUint(uint(intVal))
			}
		}
	case dc.PTInt64:
		if intValue, ok := value.(int64); ok {
			packer.PackInt64(intValue)
		} else if intValue, ok := value.(int); ok {
			packer.PackInt64(int64(intValue))
		} else if floatValue, ok := value.(float64); ok {
			packer.PackInt64(int64(floatValue))
		} else if jsonNumber, ok := value.(json.Number); ok {
			if intVal, err := jsonNumber.Int64(); err == nil {
				packer.PackInt64(intVal)
			}
		}
	case dc.PTUint64:
		if intValue, ok := value.(int64); ok {
			packer.PackUint64(uint64(intValue))
		} else if intValue, ok := value.(int); ok {
			packer.PackUint64(uint64(intValue))
		} else if floatValue, ok := value.(float64); ok {
			packer.PackUint64(uint64(floatValue))
		} else if jsonNumber, ok := value.(json.Number); ok {
			if intVal, err := jsonNumber.Int64(); err == nil {
				packer.PackUint64(uint64(intVal))
			}
		}
	case dc.PTString:
		if stringValue, ok := value.(string); ok {
			packer.PackString(stringValue)
		} else if boolValue, ok := value.(bool); ok {
			if boolValue {
				packer.PackString("true")
			} else {
				packer.PackString("false")
			}
		} else if jsonNumber, ok := value.(json.Number); ok {
			packer.PackString(jsonNumber.String())
		}
	case dc.PTBlob:
		if binData, ok := value.([]byte); ok {
			packer.PackString(string(binData))
		} else if stringValue, ok := value.(string); ok {
			if decoded, err := base64.StdEncoding.DecodeString(stringValue); err == nil {
				packer.PackString(string(decoded))
			} else {
				packer.PackString(stringValue)
			}
		}
	default:
		if array, ok := value.([]interface{}); ok {
			packer.Push()
			for _, v := range array {
				PackValue(packer, v, log)
			}
			packer.Pop()
		} else if doc, ok := value.(map[string]interface{}); ok {
			packer.Push()
			numValues := len(doc)
			for i := 0; i < numValues; i++ {
				field := packer.GetCurrentField().AsField().(dc.DCField)
				name := field.GetName()
				if name == "" {
					name = fmt.Sprintf("_%d", i)
				}
				if v, ok := doc[name]; ok {
					PackValue(packer, v, log)
				} else {
					if name != "" {
						name = fmt.Sprintf("_%d", i)
						if v, ok := doc[name]; ok {
							PackValue(packer, v, log)
						}
					}
				}
			}
			packer.Pop()
		} else if value == nil {
			return
		} else {
			log.Warnf("Unknown value type in PackValue: %T with value: %v", value, value)
			if strValue := fmt.Sprintf("%v", value); strValue != "" {
				packer.PackString(strValue)
			}
		}
	}
}
