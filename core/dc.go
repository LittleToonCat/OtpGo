package core

import (
	"github.com/LittleToonCat/dcparser-go"
	"fmt"
)

var DC dcparser.DCFile = dcparser.NewDCFile()

func LoadDC() (err error) {
	DC.Clear()

	for _, conf := range Config.General.DC_Files {
		ok := DC.Read(conf)
		if !ok {
			return fmt.Errorf("failed to read DC file %s: %v", conf, err)
		}
	}
	return nil
}
