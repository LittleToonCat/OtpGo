module otpgo

go 1.22

toolchain go1.22.2

require (
	github.com/apex/log v1.9.0
	github.com/carlmjohnson/versioninfo v0.22.5
	github.com/cjoudrey/gluahttp v0.0.0-20201111170219-25003d9adfa9
	github.com/fatih/color v1.17.0
	github.com/jehiah/go-strftime v0.0.0-20171201141054-1d33003b3869
	github.com/mattn/go-colorable v0.1.13
	github.com/pires/go-proxyproto v0.7.0
	github.com/spf13/pflag v1.0.5
	github.com/spf13/viper v1.18.2
	github.com/stretchr/testify v1.9.0
	github.com/tengattack/gluacrypto v0.0.0-20240324200146-54b58c95c255
	github.com/tj/assert v0.0.3
	github.com/vadv/gopher-lua-libs v0.5.0
	github.com/yuin/gopher-lua v1.1.1
	go.mongodb.org/mongo-driver v1.15.0
	gopkg.in/yaml.v2 v2.4.0
)

require (
	filippo.io/edwards25519 v1.1.0 // indirect
	github.com/VividCortex/ewma v1.2.0 // indirect
	github.com/alessio/shellescape v1.4.2 // indirect
	github.com/aws/aws-sdk-go v1.53.10 // indirect
	github.com/beorn7/perks v1.0.1 // indirect
	github.com/cbroglie/mustache v1.4.0 // indirect
	github.com/cespare/xxhash/v2 v2.3.0 // indirect
	github.com/cheggaaa/pb/v3 v3.1.5 // indirect
	github.com/davecgh/go-spew v1.1.2-0.20180830191138-d8f796af33cc // indirect
	github.com/dustin/go-humanize v1.0.1 // indirect
	github.com/fsnotify/fsnotify v1.7.0 // indirect
	github.com/go-sql-driver/mysql v1.9.3 // indirect
	github.com/golang/snappy v0.0.4 // indirect
	github.com/hashicorp/hcl v1.0.0 // indirect
	github.com/jmespath/go-jmespath v0.4.0 // indirect
	github.com/klauspost/compress v1.17.8 // indirect
	github.com/lib/pq v1.10.9 // indirect
	github.com/magiconair/properties v1.8.7 // indirect
	github.com/mattn/go-isatty v0.0.20 // indirect
	github.com/mattn/go-runewidth v0.0.15 // indirect
	github.com/mattn/go-sqlite3 v1.14.22 // indirect
	github.com/mitchellh/mapstructure v1.5.0 // indirect
	github.com/montanaflynn/stats v0.7.1 // indirect
	github.com/pelletier/go-toml/v2 v2.2.2 // indirect
	github.com/pkg/errors v0.9.1 // indirect
	github.com/pmezard/go-difflib v1.0.1-0.20181226105442-5d4384ee4fb2 // indirect
	github.com/prometheus/client_golang v1.19.1 // indirect
	github.com/prometheus/client_model v0.6.1 // indirect
	github.com/prometheus/common v0.53.0 // indirect
	github.com/prometheus/procfs v0.15.0 // indirect
	github.com/rivo/uniseg v0.4.7 // indirect
	github.com/sagikazarmark/locafero v0.4.0 // indirect
	github.com/sagikazarmark/slog-shim v0.1.0 // indirect
	github.com/sourcegraph/conc v0.3.0 // indirect
	github.com/spf13/afero v1.11.0 // indirect
	github.com/spf13/cast v1.6.0 // indirect
	github.com/subosito/gotenv v1.6.0 // indirect
	github.com/technoweenie/multipartstreamer v1.0.1 // indirect
	github.com/xdg-go/pbkdf2 v1.0.0 // indirect
	github.com/xdg-go/scram v1.1.2 // indirect
	github.com/xdg-go/stringprep v1.0.4 // indirect
	github.com/youmark/pkcs8 v0.0.0-20240424034433-3c2c7870ae76 // indirect
	github.com/yuin/gluamapper v0.0.0-20150323120927-d836955830e7 // indirect
	go.uber.org/multierr v1.11.0 // indirect
	golang.org/x/crypto v0.23.0 // indirect
	golang.org/x/exp v0.0.0-20240525044651-4c93da0ed11d // indirect
	golang.org/x/net v0.25.0 // indirect
	golang.org/x/sync v0.7.0 // indirect
	golang.org/x/sys v0.20.0 // indirect
	golang.org/x/text v0.15.0 // indirect
	google.golang.org/protobuf v1.34.1 // indirect
	gopkg.in/ini.v1 v1.67.0 // indirect
	gopkg.in/xmlpath.v2 v2.0.0-20150820204837-860cbeca3ebc // indirect
	gopkg.in/yaml.v3 v3.0.1 // indirect
)

// This exists until https://github.com/yuin/gopher-lua/pull/465 and https://github.com/yuin/gopher-lua/pull/283 has been merged.
replace github.com/yuin/gopher-lua => github.com/rocketprogrammer/gopher-lua v0.0.0-20240527-8595878.0.20240531215802-bd95e3881f84
