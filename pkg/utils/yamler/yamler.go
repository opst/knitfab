package yamler

import (
	"fmt"

	"gopkg.in/yaml.v3"
)

func Text(value string, options ...Option) *yaml.Node {
	n := &yaml.Node{Kind: yaml.ScalarNode, Value: value}
	for _, opt := range options {
		n = opt(n)
	}
	return n
}

func Bool(b bool) *yaml.Node {
	value := "false"
	if b {
		value = "true"
	}
	return &yaml.Node{Kind: yaml.ScalarNode, Value: value}
}

type Numeric interface {
	~int | ~int8 | ~int16 | ~int32 | ~int64 | ~uint | ~uint8 | ~uint16 | ~uint32 | ~uint64 | ~float32 | ~float64
}

func Number[N Numeric](n N) *yaml.Node {
	return &yaml.Node{Kind: yaml.ScalarNode, Value: fmt.Sprint(n)}
}

type Option func(*yaml.Node) *yaml.Node

func WithStyle(s yaml.Style) Option {
	return func(n *yaml.Node) *yaml.Node {
		n.Style = s
		return n
	}
}

func WithHeadComment(comment string) Option {
	return func(n *yaml.Node) *yaml.Node {
		n.HeadComment = comment
		return n
	}
}

func Seq(s ...*yaml.Node) *yaml.Node {
	return &yaml.Node{Kind: yaml.SequenceNode, Content: s}
}

func Null() *yaml.Node {
	return &yaml.Node{Kind: yaml.ScalarNode, Value: "null"}
}

type MapEntry struct {
	Key   *yaml.Node
	Value *yaml.Node
}

func Entry(k *yaml.Node, v *yaml.Node) MapEntry {
	return MapEntry{Key: k, Value: v}
}

func Map(e ...MapEntry) *yaml.Node {
	content := []*yaml.Node{}

	for _, ee := range e {
		content = append(content, ee.Key)
		content = append(content, ee.Value)
	}

	return &yaml.Node{Kind: yaml.MappingNode, Content: content}
}
