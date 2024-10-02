package yamler_test

import (
	"bytes"
	"testing"

	"github.com/opst/knitfab/pkg/cmp"
	"github.com/opst/knitfab/pkg/utils/yamler"
	"gopkg.in/yaml.v3"
)

type document struct {
	Key1   string            `yaml:"key1"`
	Key2   bool              `yaml:"key2"`
	Key3   bool              `yaml:"key3"`
	Key4   int               `yaml:"key4"`
	Key4_2 float32           `yaml:"key4.2"`
	Key5   map[string]string `yaml:"key5"`
	Key6   []string          `yaml:"key6"`
	Key7   any               `yaml:"key7"`
}

func TestYamler(t *testing.T) {

	testee := yamler.Map(
		yamler.Entry(yamler.Text("key1", yamler.WithHeadComment("comment1...\ncomment2...")), yamler.Text("value 1")),
		yamler.Entry(yamler.Text("key2"), yamler.Bool(true)),
		yamler.Entry(yamler.Text("key3"), yamler.Bool(false)),
		yamler.Entry(yamler.Text("key4"), yamler.Number(42)),
		yamler.Entry(yamler.Text("key4.2"), yamler.Number(4.2)),
		yamler.Entry(yamler.Text("key5", yamler.WithFootComment("foot comment")), yamler.Map(
			yamler.Entry(yamler.Text("child1", yamler.WithHeadComment("comment on child"), yamler.WithFootComment("foot comment on child")), yamler.Text("child value 1: with colon")),
		)),
		yamler.Entry(
			yamler.Text("key6"),
			yamler.Seq(
				yamler.Text("abc"),
				yamler.Bool(true),
				yamler.Bool(false),
				yamler.Number(123),
				yamler.Number(1.25),
			),
		),
		yamler.Entry(yamler.Text("key7"), yamler.Null()),
	)

	buf := bytes.NewBuffer(nil)
	enc := yaml.NewEncoder(buf)
	enc.SetIndent(2)
	defer enc.Close()

	if err := enc.Encode(testee); err != nil {
		t.Fatal(err)
	}
	enc.Close() // force close to flush

	expected := `# comment1...
# comment2...
key1: value 1
key2: true
key3: false
key4: 42
key4.2: 4.2
key5:
  # comment on child
  child1: 'child value 1: with colon'
  # foot comment on child
# foot comment

key6:
  - abc
  - true
  - false
  - 123
  - 1.25
key7: null
`

	actual := buf.String()
	if actual != expected {
		t.Errorf(
			"\n===actual===\n%s\n===expected===\n%s",
			actual, expected,
		)
	}

	d := new(document)
	d.Key7 = "not nil"

	if err := yaml.Unmarshal(buf.Bytes(), d); err != nil {
		t.Fatal(err)
	}

	if d.Key1 != "value 1" {
		t.Errorf("key1: actual = %s, expected = 'value 1'", d.Key1)
	}
	if !d.Key2 {
		t.Errorf("key2: actual = false, expected = true")
	}
	if d.Key3 {
		t.Errorf("key3: actual = true, expected = false")
	}
	if d.Key4 != 42 {
		t.Errorf("key4: actual = %d, expected = 42", d.Key4)
	}
	if d.Key4_2 != 4.2 {
		t.Errorf("key4.2: actual = %f, expected = 4.2", d.Key4_2)
	}
	{
		expected := map[string]string{
			"child1": "child value 1: with colon",
		}
		if !cmp.MapEq(d.Key5, expected) {
			t.Errorf("key5: actual = %+v, expected %+v", d.Key5, expected)
		}
	}
	{
		expected := []string{"abc", "true", "false", "123", "1.25"}
		if !cmp.SliceEq(d.Key6, expected) {
			t.Errorf("key6: actual = %+v, expected = %+v", actual, expected)
		}
	}
	if d.Key7 != nil {
		t.Errorf("key7 is not null. acutal = %+v", d.Key7)
	}
}
