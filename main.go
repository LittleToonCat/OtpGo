package main

import (
	dc "github.com/LittleToonCat/dcparser-go"
	"otpgo/clientagent"
	"otpgo/core"
	"otpgo/database"
	"otpgo/eventlogger"
	"otpgo/luarole"
	"otpgo/messagedirector"
	"otpgo/util"
	"otpgo/stateserver"
	"fmt"
	"github.com/apex/log"
	"github.com/carlmjohnson/versioninfo"
	"github.com/spf13/pflag"
	"os"
	"os/signal"
	"path"
	"path/filepath"
	"strings"
)

var mainLog *log.Entry

func init() {
	log.SetHandler(core.Log)
	log.SetLevel(log.DebugLevel)
	mainLog = log.WithFields(log.Fields{
		"name": "Main",
		"modName": "Main",
	})
}

func main() {
	pflag.Usage = func() {
		fmt.Printf(
			`Usage:    otpgo [options]... [CONFIG_FILE]

      OtpGo is an OTP (Online Theme Park) server written in Go.
      By default OtpGo looks for a configuration file in the current
      working directory as otp.yml.  A different config file path
      can be specified as a positional argument.

      -h, --help      Print this help dialog.
      -v, --version   Print version information.
      -L, --log       Specify a file to write log messages to.
      -l, --loglevel  Specify the minimum log level that should be logged;
                        Error and Fatal levels will always be logged.
`)
		os.Exit(1)
	}

	logfilePtr := pflag.StringP("log", "L", "", "Specify the file to write log messages to.")
	loglevelPtr := pflag.StringP("loglevel", "l", "debug", "Specify minimum log level that should be logged.")
	versionPtr := pflag.BoolP("version", "v", false, "Show the application version.")
	helpPtr := pflag.BoolP("help", "h", false, "Show the application usage.")

	pflag.Parse()

	if *helpPtr {
		pflag.Usage()
		os.Exit(1)
	}
	if *versionPtr {
		fmt.Printf(`
OTP (Online Theme Park) server written in Go.
https://github.com/LittleToonCat/OtpGo

(Based on the unfinished AstronGo project by nosyliam)
https://github.com/nosyliam/AstronGo

Revision: %s
`, versioninfo.Revision)
		os.Exit(1)
	}
	if *loglevelPtr != "" {
		loglevelChoices := map[string]log.Level{"info": log.InfoLevel, "warning": log.WarnLevel, "error": log.ErrorLevel, "fatal": log.FatalLevel, "debug": log.DebugLevel}
		if choice, validChoice := loglevelChoices[*loglevelPtr]; !validChoice {
			mainLog.Fatal(fmt.Sprintf("Unknown log-level \"%s\".", *loglevelPtr))
			pflag.Usage()
			os.Exit(1)
		} else {
			log.SetLevel(choice)
		}
	}
	if *logfilePtr != "" {
		logfile, err := os.OpenFile(*logfilePtr, os.O_RDWR|os.O_CREATE, 0600)
		if err != nil {
			mainLog.Fatal(fmt.Sprintf("Failed to open log file \"%s\".", *logfilePtr))
			os.Exit(1)
		}
		logfile.Truncate(0)
		logfile.Seek(0, 0)

		defer logfile.Sync()
		defer logfile.Close()

		handler := core.NewMultiHandler(core.Log, core.NewLogger(logfile))
		log.SetHandler(handler)
	}

	var configPath, configName string
	args := pflag.Args()
	if len(args) > 0 {
		configName = filepath.Base(args[0])
		configName = strings.TrimSuffix(configName, path.Ext(configName))
		configPath = filepath.Dir(args[0])
	} else {
		configName = "otp"
		configPath = "."
	}

	mainLog.Info("Loading configuration file...")

	if err := core.LoadConfig(configPath, configName); err != nil {
		mainLog.Fatal(err.Error())
	}

	if err := core.LoadDC(); err != nil {
		mainLog.Fatal(err.Error())
	}

	eventlogger.StartEventLogger()
	messagedirector.Start()

	// Configure UberDOG list
	for _, ud := range core.Config.Uberdogs {
		class := core.DC.Get_class_by_name(ud.Class)
		// Check if the method returns a NULL pointer
		if class == dc.SwigcptrDCClass(0) {
			mainLog.Fatalf("For UberDOG %d, class %s does not exist!", ud.ID, ud.Class)
			return
		}

		core.Uberdogs = append(core.Uberdogs, core.Uberdog{
			Id:        util.Doid_t(ud.ID),
			Class:     class,
		})
	}

	// Instantiate roles
	for _, role := range core.Config.Roles {
		switch role.Type {
		case "clientagent":
			clientagent.NewClientAgent(role)
		case "database":
			database.NewDatabaseServer(role)
		case "dbss":
			stateserver.NewDatabaseStateServer(role)
		case "lua":
			luarole.NewLuaRole(role)
		case "stateserver":
			stateserver.NewStateServer(role)
		}
	}

	c := make(chan os.Signal)
	signal.Notify(c, os.Interrupt)

	sig := <-c
	mainLog.Fatal(fmt.Sprintf("Got %s signal. Aborting...", sig))
	os.Exit(1)
}
