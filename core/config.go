package core

import (
	"fmt"
	"otpgo/util"

	"otpgo/dc"

	"github.com/spf13/viper"
)

var Config *ServerConfig
var Hash uint32
var StopChan chan bool // For test purposes

type Uberdog struct {
	Id    util.Doid_t
	Class dc.DCClass
}

var Uberdogs []Uberdog

type Role struct {
	Type string
	Name string

	// CLIENT
	Bind    string
	Proxy   bool
	Version string
	DC_Hash int
	Tuning  struct {
		Interest_Timeout int
	}
	Lua_File string
	Client   struct {
		Add_Interest         string
		Write_Buffer_Size    int
		Heartbeat_Timeout    int
		Keepalive            int
		Relocate             bool
		Legacy_Handle_Object bool
	}
	Channels struct {
		Min int
		Max int
	}

	// STATESERVER
	Control int
	Objects []struct {
		ID    int
		Class string
	}
	DO_Preallocation_Amount int

	// DBSS
	Ranges struct {
		Min util.Channel_t
		Max util.Channel_t
	}
	Database util.Channel_t

	// DATABASE SERVER
	Generate struct {
		Min int
		Max int
	}
	Backend struct {
		Type string

		// MONGO BACKEND
		Server   string
		Database string

		// YAML BACKEND
		Directory string
	}

	// EVENT LOGGER
	Output         string
	RotateInterval string
}

type ServerConfig struct {
	Daemon struct {
		Name string
	}
	General struct {
		Eventlogger                         string
		DC_Files                            []string
		DC_Disable_Multiple_Inheritance     bool
		DC_Disable_Virtual_Inheritance      bool
		DC_Disable_Sort_Inheritance_By_File bool
	}
	Uberdogs []struct {
		ID    int
		Class string
	}
	MessageDirector struct {
		Bind    string
		Connect string
	}
	Debug struct {
		Pprof bool
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
