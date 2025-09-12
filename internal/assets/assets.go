package assets

import "embed"

//go:embed all:banner.txt
var bannerFS embed.FS

var BannerString string

func init() {
	bytes, err := bannerFS.ReadFile("banner.txt")
	if err != nil {
		// we WANT to panic if we don't display ASCII art!
		panic(err)
	}

	BannerString = string(bytes)
}
