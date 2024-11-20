package db

import (
	"context"
	"time"

	"github.com/opst/knitfab/pkg/domain"
)

type DataInterface interface {
	// Retreive data identified by knitId
	//
	// args:
	//     - ctx: context
	//     - string: knitId
	//
	// returns:
	//     - map[string]KnitData : mapping from KnitId to KnitData
	//     - error
	//
	Get(context.Context, []string) (map[string]domain.KnitData, error)

	// Retrieve KnitId of the data that contains all specified tags and range of updated time.
	//
	// args:
	//     - ctx: context
	//     - []Tag: specified tags
	//     - *Time: start of the time range
	//     - *Time: end of the time range
	//
	// returns:
	//     - []string: Knitid of the data that meets the conditions
	//     - error
	//
	Find(context.Context, []domain.Tag, *time.Time, *time.Time) ([]string, error)

	// update tags on data.
	//
	// Args
	//
	// - context.Context
	//
	// - string : knitId of target data
	//
	// - TagDelta : tags adding/removing from data
	//
	// Return
	//
	// - error
	UpdateTag(context.Context, string, domain.TagDelta) error

	// Create and occupy a new DataAgent for the KnitData.
	//
	// Args
	//
	// - ctx context.Context
	//
	// - knitId string : knitId of target data
	//
	// - mode DataAgentMode: mode of the DataAgent
	//
	// - houskeepDelay: duration to wait before housekeeping.
	// This duration can be truncated at some resolution when stored into database.
	//
	// You should start DataAgent on kubernetes by the duration,
	// or the DataAgent record is removed by housekeeping.
	//
	// Return
	//
	// - DataAgent : DataAgent for the KnitData
	//
	// - error
	NewAgent(ctx context.Context, knitId string, mode domain.DataAgentMode, housekeepDelay time.Duration) (domain.DataAgent, error)

	// Remove a DataAgent from the Data Agent regisrty.
	//
	// Args
	//
	// - ctx context.Context
	//
	// - name string : name of the DataAgent
	//
	// Return
	//
	// - error
	RemoveAgent(ctx context.Context, name string) error

	// Pick and remove a DataAgent from the Data Agent regisrty.
	//
	// This method picks DataAgent exceeding "housekeep after" time limit,
	// and remove it if the function f returns true without error.
	//
	// Args
	//
	// - ctx context.Context
	//
	// - curosr DataAgentCursor : cursor where pick DataAgent last time
	//
	// - f func(DataAgent) (removeOk bool, err error) : function to determine whether to remove the DataAgent
	//
	// Return
	//
	// - DataAgentCursor : cursor where pick DataAgent this time.
	// If no DataAgent is picked, the cursor is not changed.
	//
	// - error
	PickAndRemoveAgent(ctx context.Context, curosr domain.DataAgentCursor, f func(domain.DataAgent) (removeOk bool, err error)) (domain.DataAgentCursor, error)

	// Get names of DataAgents for the KnitData.
	//
	// Args
	//
	// - ctx context.Context
	//
	// - knitId string : knitId of target data
	//
	// - modes []DataAgentMode : modes of the DataAgents to be queried
	//
	// Return
	//
	// - []string : names of DataAgents with the modes
	//
	GetAgentName(ctx context.Context, knitId string, modes []domain.DataAgentMode) ([]string, error)
}
