package tags

import (
	"github.com/opst/knitfab-api-types/tags"
	kdb "github.com/opst/knitfab/pkg/db"
)

func Compose(dbtag kdb.Tag) tags.Tag {
	return tags.Tag(dbtag)
}
