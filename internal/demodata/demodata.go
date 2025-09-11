package demodata

import "embed"

//go:embed all:data
var DemoDataFS embed.FS
