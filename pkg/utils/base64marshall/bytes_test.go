package base64marshall_test

import (
	goBase64 "encoding/base64"
	"encoding/json"
	"fmt"
	"testing"

	"github.com/opst/knitfab/pkg/utils/base64marshall"
	"github.com/opst/knitfab/pkg/utils/try"
)

func TestBytes(t *testing.T) {
	t.Run("Bytes should be encoded to base64 string", func(t *testing.T) {
		testee := base64marshall.New([]byte("hello"))
		if got, want := testee.String(), goBase64.StdEncoding.EncodeToString([]byte("hello")); got != want {
			t.Errorf("got %s, want %s", got, want)
		}
	})

	t.Run("Bytes should return the underlying byte slice", func(t *testing.T) {
		testee := base64marshall.New([]byte("hello"))
		if got, want := testee.Bytes(), []byte("hello"); string(got) != string(want) {
			t.Errorf("got %s, want %s", got, want)
		}
	})

	t.Run("Bytes should be MarshalJSON to base64 string", func(t *testing.T) {
		testee := base64marshall.New([]byte("hello"))
		type S struct {
			Bytes base64marshall.Bytes `json:"bytes"`
		}
		got := string(try.To(json.Marshal(S{Bytes: testee})).OrFatal(t))

		want := fmt.Sprintf(
			`{"bytes":"%s"}`,
			goBase64.StdEncoding.EncodeToString([]byte("hello")),
		)

		if got != want {
			t.Errorf("got %s, want %s", got, want)
		}
	})

	t.Run("Bytes should be UnmarshalJSON from base64 string", func(t *testing.T) {
		type S struct {
			Bytes base64marshall.Bytes `json:"bytes"`
		}
		want := goBase64.StdEncoding.EncodeToString([]byte("hello"))
		j := fmt.Sprintf(`{"bytes":"%s"}`, want)
		got := S{}
		if err := json.Unmarshal([]byte(j), &got); err != nil {
			t.Fatal(err)
		}

		if got.Bytes.String() != want {
			t.Errorf("got %v, want %v", got.Bytes.String(), want)
		}
	})

}
