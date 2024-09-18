package env_test

import (
	"testing"

	apitags "github.com/opst/knitfab-api-types/tags"
	kenv "github.com/opst/knitfab/cmd/knit/env"
	"github.com/opst/knitfab/pkg/cmp"
)

func TestLoadKnitEnv(t *testing.T) {

	t.Run("read knitenv. and it should be return Key and Value of Tags.", func(t *testing.T) {

		result, err := kenv.LoadKnitEnv("./testdata/knitenv_test.yaml")

		if err != nil {
			t.Errorf("failed to parse config.: %v", err)
		}
		expected := []apitags.Tag{
			{Key: "project", Value: "mnist"},
			{Key: "pase", Value: "test"},
			{Key: "param", Value: "param1"},
			{Key: "many", Value: "colon:in:tag"},
		}

		tags := result.Tags()

		if !cmp.SliceContentEq(tags, expected) {
			t.Errorf("unmatch host:%s, expected:%s", result, expected)
		}
	})

	t.Run("when incorrect filepath given empty KnitEnv should be created.", func(t *testing.T) {
		env, err := kenv.LoadKnitEnv("./testdata/env.yaml")

		if err != nil {
			t.Errorf("unexpected error occured:%v", err)
		}

		if len(env.Tags()) != 0 {
			t.Errorf("unexpected data:%v", env)
		}

	})

}
