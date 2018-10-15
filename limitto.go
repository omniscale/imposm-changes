package changes

type LimitTo [4]float64

// Contains checks whether the given point is inside the LimitTo.
// Returns true if LimitTo is nil.
func (l *LimitTo) Contains(long, lat float64) bool {
	if l == nil {
		return true
	}
	return long >= l[0] && long <= l[2] && lat >= l[1] && lat <= l[3]
}
