//go:generate go run github.com/Songmu/gocredits/cmd/gocredits@v0.3.0 -w
package main

import (
	_ "embed"
	"fmt"
	"log"
	"os"

	"github.com/opst/knitfab/cmd/empty/empty"
)

//go:embed CREDITS
var CREDITS string

// verify all of given filepathes are empty directory.
//
// if it holds up, exit with 0.
// otherwise, exit with non-zero.
func main() {
	args := os.Args[1:]
	for _, arg := range args {
		if arg == "--license" {
			fmt.Println(CREDITS)
			return
		}
	}

	for _, path := range args {
		if err := empty.Assert(path); err != nil {
			log.Fatalf(
				"%s : not an empty directory (%s)",
				path, err,
			)
		}
		log.Printf("%s : ok", path)
	}
	log.Printf("verified.")
}
