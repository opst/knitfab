//go:generate go run github.com/Songmu/gocredits/cmd/gocredits@v0.3.0 -w
//go:generate sh -c "git rev-parse HEAD > pkg/buildtime/revision"
package knitfab
