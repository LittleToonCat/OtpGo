package core

import (
	"github.com/LittleToonCat/dcparser-go"
	"fmt"
)

var DC dcparser.DCFile = dcparser.NewDCFile()

func LoadDC() (err error) {
	if (len(Config.General.DC_Files) == 0) {
		return fmt.Errorf("missing DC file configuration")
	}
	for _, conf := range Config.General.DC_Files {
		ok := DC.Read(conf)
		if !ok {
			return fmt.Errorf("failed to read DC file %s: %v", conf, err)
		}
	}
	return nil
}
