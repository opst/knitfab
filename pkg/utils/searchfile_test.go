package utils_test

import (
	"errors"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"path/filepath"
	"testing"

	"github.com/opst/knitfab/pkg/utils"
)

func TestSearchFilePathtoUpward(t *testing.T) {
	t.Run("the fila exist current directory. succsessfully search file.", func(t *testing.T) {

		name := "knit-env.yaml"

		tmp, err := ioutil.TempDir("", "knittest")
		if err != nil {
			t.Fatal(err.Error())
		}
		path := filepath.Join(tmp, name)
		os.Create(path)
		defer os.RemoveAll(tmp)

		if err != nil {
			fmt.Println("Error, can not create temp file.")
			log.Fatal(err)
		}

		ret, err := utils.SearchFilePathtoUpward(tmp, name)
		if err != nil {
			t.Error(err.Error())
		}
		if *ret != path {
			t.Errorf("unmatch fila path:%s, expected:%sn", path, *ret)
		}
	})

	t.Run("the file is one level up. succsessfully search file.", func(t *testing.T) {

		tmp, err := ioutil.TempDir("", "knittest")
		if err != nil {
			t.Fatal(err.Error())
		}
		upPath := filepath.Join(tmp, "data")
		err = os.Mkdir(upPath, 0777)
		if err != nil {
			t.Fatal(err.Error())
		}

		//upPath := currentPath[:strings.LastIndex(currentPath, string(filepath.Separator))]
		name := "knit-env.yaml"
		path := filepath.Join(tmp, name)
		os.Create(path)
		defer os.RemoveAll(path)

		if err != nil {
			fmt.Println("Error, can not create temp file.")
			log.Fatal(err)
		}

		ret, err := utils.SearchFilePathtoUpward(upPath, name)
		if err != nil {
			t.Error(err.Error())
		}
		if *ret != path {
			t.Errorf("unmatch fila path:%s, expected:%sn", path, *ret)
		}
	})

	t.Run("the file is not exist. error search file.", func(t *testing.T) {

		currentPath, err := os.Getwd()
		if err != nil {
			t.Fatal(err.Error())
		}

		name := "knit-env.yaml"
		if err != nil {
			log.Println("Error, can not create temp file.")
			log.Fatal(err)
		}

		_, err = utils.SearchFilePathtoUpward(currentPath, name)

		if !errors.Is(err, utils.ErrSearchFile) {
			t.Error(err.Error())
		}

	})

}
