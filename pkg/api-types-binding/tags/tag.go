package tags

import (
	"github.com/opst/knitfab-api-types/tags"
	"github.com/opst/knitfab/pkg/domain"
)

func Compose(dbtag domain.Tag) tags.Tag {
	return tags.Tag(dbtag)
}
