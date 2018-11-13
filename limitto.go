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

// Intersects checks whether the bbox intersects with the LimitTo.
// Returns true if LimitTo is nil or if o is zero.
func (l *LimitTo) Intersects(o [4]float64) bool {
	if l == nil {
		return true
	}

	if o[0] == 0 && o[1] == 0 && o[2] == 0 && o[3] == 0 {
		return true
	}

	return l[2] >= o[0] &&
		l[3] >= o[1] &&
		l[0] <= o[2] &&
		l[1] <= o[3]
}
