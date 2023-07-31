package core

import (
	dc "github.com/LittleToonCat/dcparser-go"
	"otpgo/util"
	"fmt"
	"github.com/spf13/viper"
)

var Config *ServerConfig
var Hash uint32
var StopChan chan bool // For test purposes

type Uberdog struct {
	Id        util.Doid_t
	Class     dc.DCClass
}

var Uberdogs []Uberdog

type Role struct {
	Type string

	// CLIENT
	Bind    string
	Version string
	Tuning  struct {
		Interest_Timeout int
	}
	Client struct {
		Add_Interest      string
		Write_Buffer_Size int
		Heartbeat_Timeout int
		Keepalive         int
		Relocate          bool
	}
	Channels struct {
		Min int
		Max int
	}

	// STATESERVER
	Control int
	Objects []struct {
		ID        int
		Class     string
	}

	// DATABASE SERVER
	Generate struct {
		Min int
		Max int
	}
	Backend struct {
		Type      string

		// MONGO BACKEND
		Server    string
		Database  string
	}
}

type ServerConfig struct {
	Daemon struct {
		Name string
	}
	General struct {
		Eventlogger string
		DC_Files    []string
	}
	Uberdogs []struct {
		ID        int
		Class     string
	}
	MessageDirector struct {
		Bind    string
		Connect string
	}
	Eventlogger struct {
		Bind   string
		Output string
	}
	Roles []Role
}

func LoadConfig(path string, name string) (err error) {
	viper.SetConfigType("yaml")
	viper.AddConfigPath(".")
	viper.AddConfigPath("./config")
	viper.AddConfigPath(path)
	viper.SetConfigName(name)

	if err := viper.ReadInConfig(); err != nil {
		return fmt.Errorf("unable to load configuration file: %v", err)
	}

	conf := &ServerConfig{}
	if err := viper.Unmarshal(conf); err != nil {
		return fmt.Errorf("unable to decode configuration file: %v", err)
	}

	Config = conf
	return nil
}
