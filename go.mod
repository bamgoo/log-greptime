module github.com/bamgoo/log-greptime

go 1.25.3

require (
	github.com/GreptimeTeam/greptimedb-ingester-go v0.4.0
	github.com/bamgoo/bamgoo v0.0.0
	github.com/bamgoo/base v0.0.0
	github.com/bamgoo/log v0.0.0
)

require (
	github.com/GreptimeTeam/greptime-proto v0.4.3 // indirect
	github.com/golang/protobuf v1.5.3 // indirect
	github.com/google/go-cmp v0.6.0 // indirect
	github.com/pelletier/go-toml/v2 v2.2.2 // indirect
	github.com/stoewer/go-strcase v1.3.0 // indirect
	github.com/stretchr/testify v1.10.0 // indirect
	golang.org/x/net v0.38.0 // indirect
	golang.org/x/sys v0.31.0 // indirect
	golang.org/x/text v0.23.0 // indirect
	google.golang.org/genproto/googleapis/rpc v0.0.0-20250227231956-55c901821b1e // indirect
	google.golang.org/grpc v1.70.0 // indirect
	google.golang.org/protobuf v1.36.5 // indirect
)

replace github.com/bamgoo/bamgoo => ../bamgoo

replace github.com/bamgoo/base => ../base

replace github.com/bamgoo/log => ../log

replace google.golang.org/grpc => google.golang.org/grpc v1.53.0

exclude google.golang.org/genproto v0.0.0-20230410155749-daa745c078e1
