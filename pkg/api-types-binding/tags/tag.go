package tags

import (
	"github.com/opst/knitfab-api-types/v2/tags"
	"github.com/opst/knitfab/v2/pkg/domain"
)

func Compose(dbtag domain.Tag) tags.Tag {
	return tags.Tag(dbtag)
}
