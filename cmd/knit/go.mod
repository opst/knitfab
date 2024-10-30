module github.com/opst/knitfab/cmd/knit

go 1.23.1

replace github.com/opst/knitfab => ../..

require (
	github.com/cheggaaa/pb/v3 v3.1.5
	github.com/google/go-containerregistry v0.20.2
	github.com/hectane/go-acl v0.0.0-20230122075934-ca0b05cb1adb
	github.com/opst/knitfab v1.5.0-beta
	github.com/opst/knitfab-api-types v1.4.0
	github.com/youta-t/flarc v0.0.3
	gopkg.in/yaml.v3 v3.0.1
	k8s.io/apimachinery v0.31.1
)

require (
	github.com/VividCortex/ewma v1.2.0 // indirect
	github.com/containerd/stargz-snapshotter/estargz v0.15.1 // indirect
	github.com/fatih/color v1.17.0 // indirect
	github.com/fxamacker/cbor/v2 v2.7.0 // indirect
	github.com/gogo/protobuf v1.3.2 // indirect
	github.com/klauspost/compress v1.17.10 // indirect
	github.com/mattn/go-colorable v0.1.13 // indirect
	github.com/mattn/go-isatty v0.0.20 // indirect
	github.com/mattn/go-runewidth v0.0.16 // indirect
	github.com/opencontainers/go-digest v1.0.0 // indirect
	github.com/opencontainers/image-spec v1.1.0 // indirect
	github.com/rivo/uniseg v0.4.7 // indirect
	github.com/vbatts/tar-split v0.11.6 // indirect
	github.com/x448/float16 v0.8.4 // indirect
	golang.org/x/sync v0.8.0 // indirect
	golang.org/x/sys v0.25.0 // indirect
	gopkg.in/inf.v0 v0.9.1 // indirect
)
