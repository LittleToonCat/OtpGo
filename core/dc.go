package core

import (
	dc "github.com/LittleToonCat/dcparser-go"
	"fmt"
)

var DC dc.DCFile = dc.NewDCFile()

func LoadDC() (err error) {
	for _, conf := range Config.General.DC_Files {
		ok := DC.Read(conf)
		if !ok {
			return fmt.Errorf("failed to read DC file %s: %v", conf, err)
		}
	}
	return nil
}
