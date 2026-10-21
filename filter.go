package download

import (
	"fmt"
	"sort"
	"strings"
	"time"
)

// SortField selects the ordering used by List and Snapshot.
type SortField string

const (
	SortByCreatedAt SortField = "created_at"
	SortByUpdatedAt SortField = "updated_at"
	SortByPriority  SortField = "priority"
	SortByProgress  SortField = "progress"
	SortBySpeed     SortField = "speed"
	SortByStatus    SortField = "status"
)

// MatchMode selects how StringMatch compares a pattern.
type MatchMode string

const (
	MatchExact    MatchMode = "exact"
	MatchPrefix   MatchMode = "prefix"
	MatchContains MatchMode = "contains"
)

// StringMatch filters a string field. A MatchMode of "" is treated as
// MatchExact, and an empty Pattern disables the constraint.
type StringMatch struct {
	Mode    MatchMode
	Pattern string
}

// ListFilter narrows, orders and paginates tasks.
//
// The zero value matches everything. Different fields are ANDed; multiple
// values inside one field (IDs, Statuses, Priorities) are ORed. The execution
// order is: match, sort, paginate.
//
// Zero or unknown values in the speed column always sort last, regardless of
// direction.
type ListFilter struct {
	Statuses []TaskStatus
	IDs      []TaskID
	URL      StringMatch
	Path     StringMatch

	CreatedAfter  *time.Time
	CreatedBefore *time.Time
	UpdatedAfter  *time.Time
	UpdatedBefore *time.Time

	Priorities []int
	Tags       map[string]string
	OnlyFailed bool
	HasError   bool

	SortBy   SortField
	SortDesc bool
	Offset   int
	Limit    int // 0 means no limit
}

func validStatus(s TaskStatus) bool {
	switch s {
	case StatusPending, StatusProbing, StatusDownloading, StatusPaused,
		StatusCompleted, StatusFailed, StatusCanceled, StatusRemoved:
		return true
	}
	return false
}

func validSortField(f SortField) bool {
	switch f {
	case "", SortByCreatedAt, SortByUpdatedAt, SortByPriority, SortByProgress,
		SortBySpeed, SortByStatus:
		return true
	}
	return false
}

func (f ListFilter) validate() error {
	if f.Limit < 0 {
		return fmt.Errorf("%w: limit must not be negative", ErrInvalidFilter)
	}
	if f.Offset < 0 {
		return fmt.Errorf("%w: offset must not be negative", ErrInvalidFilter)
	}
	if !validSortField(f.SortBy) {
		return fmt.Errorf("%w: unknown sort field %q", ErrInvalidFilter, f.SortBy)
	}
	for _, s := range f.Statuses {
		if !validStatus(s) {
			return fmt.Errorf("%w: unknown status %q", ErrInvalidFilter, s)
		}
	}
	for _, m := range []StringMatch{f.URL, f.Path} {
		switch m.Mode {
		case "", MatchExact, MatchPrefix, MatchContains:
		default:
			return fmt.Errorf("%w: unknown match mode %q", ErrInvalidFilter, m.Mode)
		}
	}
	return nil
}

func matchString(value string, m StringMatch) bool {
	if m.Pattern == "" {
		return true
	}
	switch m.Mode {
	case MatchPrefix:
		return strings.HasPrefix(value, m.Pattern)
	case MatchContains:
		return strings.Contains(value, m.Pattern)
	default:
		return value == m.Pattern
	}
}

func containsStatus(list []TaskStatus, s TaskStatus) bool {
	for _, v := range list {
		if v == s {
			return true
		}
	}
	return false
}

func containsID(list []TaskID, id TaskID) bool {
	for _, v := range list {
		if v == id {
			return true
		}
	}
	return false
}

func containsInt(list []int, n int) bool {
	for _, v := range list {
		if v == n {
			return true
		}
	}
	return false
}

func matchesFilter(t TaskInfo, f ListFilter) bool {
	if len(f.Statuses) > 0 && !containsStatus(f.Statuses, t.Status) {
		return false
	}
	if len(f.IDs) > 0 && !containsID(f.IDs, t.ID) {
		return false
	}
	if !matchString(t.URL, f.URL) || !matchString(t.Path, f.Path) {
		return false
	}
	if f.CreatedAfter != nil && !t.CreatedAt.After(*f.CreatedAfter) {
		return false
	}
	if f.CreatedBefore != nil && !t.CreatedAt.Before(*f.CreatedBefore) {
		return false
	}
	if f.UpdatedAfter != nil && !t.UpdatedAt.After(*f.UpdatedAfter) {
		return false
	}
	if f.UpdatedBefore != nil && !t.UpdatedAt.Before(*f.UpdatedBefore) {
		return false
	}
	if len(f.Priorities) > 0 && !containsInt(f.Priorities, t.Priority) {
		return false
	}
	for k, v := range f.Tags {
		if t.Tags[k] != v {
			return false
		}
	}
	if f.OnlyFailed && t.Status != StatusFailed {
		return false
	}
	if f.HasError && t.Error == "" {
		return false
	}
	return true
}

func statusRank(s TaskStatus) int {
	switch s {
	case StatusDownloading:
		return 0
	case StatusProbing:
		return 1
	case StatusPending:
		return 2
	case StatusPaused:
		return 3
	case StatusFailed:
		return 4
	case StatusCompleted:
		return 5
	case StatusCanceled:
		return 6
	default:
		return 7
	}
}

// compareField returns -1, 0 or 1 for a and b. Speed uses a special rule:
// zero/unknown speeds always sort last, which is handled by the caller.
func compareField(a, b TaskInfo, f SortField) int {
	switch f {
	case "", SortByCreatedAt:
		return compareTime(a.CreatedAt, b.CreatedAt)
	case SortByUpdatedAt:
		return compareTime(a.UpdatedAt, b.UpdatedAt)
	case SortByPriority:
		return compareInt(a.Priority, b.Priority)
	case SortByProgress:
		return compareFloat(a.Progress, b.Progress)
	case SortBySpeed:
		return compareFloat(a.Speed, b.Speed)
	case SortByStatus:
		return compareInt(statusRank(a.Status), statusRank(b.Status))
	}
	return 0
}

func compareTime(a, b time.Time) int {
	switch {
	case a.Before(b):
		return -1
	case a.After(b):
		return 1
	}
	return 0
}

func compareInt(a, b int) int {
	switch {
	case a < b:
		return -1
	case a > b:
		return 1
	}
	return 0
}

func compareFloat(a, b float64) int {
	switch {
	case a < b:
		return -1
	case a > b:
		return 1
	}
	return 0
}

func sortInfos(list []TaskInfo, by SortField, desc bool) {
	if by == "" {
		by = SortByCreatedAt
	}
	sort.SliceStable(list, func(i, j int) bool {
		a, b := list[i], list[j]
		if by == SortBySpeed {
			az, bz := a.Speed == 0, b.Speed == 0
			if az != bz {
				// Whatever has a speed sorts before the unknown one.
				return bz
			}
			if az && bz {
				return a.ID < b.ID
			}
		}
		c := compareField(a, b, by)
		if c == 0 {
			return a.ID < b.ID
		}
		if desc {
			return c > 0
		}
		return c < 0
	})
}

// filterInfos applies match, sort and pagination. The returned slice is never
// nil.
func filterInfos(list []TaskInfo, f ListFilter) ([]TaskInfo, error) {
	if err := f.validate(); err != nil {
		return nil, err
	}
	out := make([]TaskInfo, 0, len(list))
	for _, t := range list {
		if matchesFilter(t, f) {
			out = append(out, t)
		}
	}
	sortInfos(out, f.SortBy, f.SortDesc)
	if f.Offset > 0 {
		if f.Offset >= len(out) {
			out = out[:0]
		} else {
			out = out[f.Offset:]
		}
	}
	if f.Limit > 0 && len(out) > f.Limit {
		out = out[:f.Limit]
	}
	return out, nil
}
