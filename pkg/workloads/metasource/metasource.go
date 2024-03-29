package metasource

import (
	"fmt"

	"github.com/opst/knitfab/pkg/buildtime"
	kubeapimeta "k8s.io/apimachinery/pkg/apis/meta/v1"
)

type SpecBuilder[C any, D any] interface {
	// Build k8s resource descriptor(s)
	Build(conf C) D
}

// knit component metadata which is deploied or placed in k8s cluster.
//
// ToLabel function converts MetaSource (or, its subtype SubjectWithExtras) to k8s labels.
type MetaSource interface {
	// The name of application/resource.
	//
	// If there are many resources running a same app, they may have same `Name()`.
	//
	// For `ObjectMeta.Name`, USE `Instance()`, NOT THIS.
	//
	// This is set as a value of k8s label "app.kubernetes.io/name".
	//
	// see: https://kubernetes.io/docs/concepts/overview/working-with-objects/common-labels/
	Name() string

	// This is set as a value of k8s label "app.kubernetes.io/instance"
	// AND ALSO `ObjectMeta.Name` .
	//
	// This will identify an instance from others sharing Name() and Component().
	//
	// see: https://kubernetes.io/docs/concepts/overview/working-with-objects/common-labels/
	//
	// When you doubt what value should be set,
	// Name() + "-" + IdType() + "-" + "Id()" is recommended.
	Instance() string

	// Where is this positioned in system archetecture.
	//
	// example: database, cache, reverse-proxy, ...
	//
	// This is set as a value of k8s label "app.kubernetes.io/component".
	//
	// see: https://kubernetes.io/docs/concepts/overview/working-with-objects/common-labels/
	Component() string

	// Identifier of entity in knitfab object model.
	Id() string

	// type of "Id()"
	//
	// example: knit_id, run_id, ...
	IdType() string

	// convert to ObjectMeta
	ObjectMeta(namespace string) kubeapimeta.ObjectMeta
}

type Extraer interface {

	// Extra labels.
	//
	// See document of `ToLabels` for more details.
	Extras() map[string]string
}

type ResourceBuilder[C any, D any] interface {
	MetaSource
	SpecBuilder[C, D]
}

// convert from Subject to k8s labels, including "recomended labels".
//
// https://kubernetes.io/docs/concepts/overview/working-with-objects/common-labels/
//
// # Recomended Labels:
//
// Recomended labels are generated like below.
//
// - "app.kubernetes.io/version"    : build version of the knit.
//
// - "app.kubernetes.io/part-of"    : "knit"
//
// - "app.kubernetes.io/managed-by" : "knit"
//
// - "app.kubernetes.io/component"  : s.Component()
//
// - "app.kubernetes.io/name"       : s.Name()
//
// - "app.kubernetes.io/instance"   : s.Instance()
//
// Each `s`s are Subject passed to `ToLabel`.
//
// # Knit Labels:
//
// Knit specific labels are prefixed with "knit/" .
// They are generated like below.
//
// - "knit/${s.Name()}.${s.IdType()}" : s.Id()
//
// - "knit/${s.Name()}.KEY"           : s.Extras()[KEY] (if any)
//
// Each `s`s here are Subject passed to `ToLabel`.
//
// Expression `${...}` are placeholder, replaced with evaluation of its content.
// CAPITALIZED `KEY` is a key in `s.Extras()`,
// only if `s` implements `interface { Extras() map[string]string }`
// (otherwize, they are not appeared).
//
// #params:
//
// - Subject: knit object which is to be k8s resource.
//
// When `s` is `SubjectWithExtras`,
// `ToLabel` generates extra "knit/*" labels additionaly.
func ToLabels(s MetaSource) map[string]string {
	knitLabelPrefix := fmt.Sprintf("knit/%s.", s.Name())

	l := map[string]string{
		"app.kubernetes.io/version":    buildtime.VERSION(),
		"app.kubernetes.io/name":       s.Name(),
		"app.kubernetes.io/instance":   s.Instance(),
		"app.kubernetes.io/component":  s.Component(),
		"app.kubernetes.io/part-of":    "knit", // XXX(takaoka.youta) : shouldn't be the name in helm?
		"app.kubernetes.io/managed-by": "knit",

		// knit/NAME.ID_TYPE: ID  --  example: `knit/dataagt.knitid: SOMEUUID-VALU-E...`
		knitLabelPrefix + s.IdType(): s.Id(),
	}

	if withEx, ok := s.(Extraer); ok {
		for k, v := range withEx.Extras() {
			l[knitLabelPrefix+k] = v
		}
	}

	return l
}

// default (and reference) implimentation of Source.ObjectMeta.
//
// For users:
//
// This is a helper function for MetaSource implimenter, not for users.
//
// When you using specific MetaSource implimentations (for example: DataMetaSource),
// it is recommended that you use MetaSource.ObjectMeta methods, not this,
// to respect for each types.
func ToObjectMeta(m MetaSource, namespace string) kubeapimeta.ObjectMeta {
	labels := ToLabels(m)
	return kubeapimeta.ObjectMeta{
		Name:      m.Instance(),
		Namespace: namespace,
		Labels:    labels,
	}
}
