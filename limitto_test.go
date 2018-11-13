package changes

import "testing"

func TestLimitTo_Contains(t *testing.T) {
	for _, tt := range []struct {
		l    *LimitTo
		long float64
		lat  float64
		want bool
	}{
		{l: nil, long: 0, lat: 0, want: true},
		{l: nil, long: 99999, lat: 99999, want: true},
		{l: &LimitTo{-20, 0, 10, 30}, long: 0, lat: -0.01, want: false},
		{l: &LimitTo{-20, 0, 10, 30}, long: 0, lat: 0, want: true},
		{l: &LimitTo{-20, 0, 10, 30}, long: 0, lat: 30.01, want: false},
		{l: &LimitTo{-20, 0, 10, 30}, long: 0, lat: 30, want: true},
		{l: &LimitTo{-20, 0, 10, 30}, long: -20, lat: 10, want: true},
		{l: &LimitTo{-20, 0, 10, 30}, long: -20.01, lat: 10, want: false},
		{l: &LimitTo{-20, 0, 10, 30}, long: 10.01, lat: 10, want: false},
		{l: &LimitTo{-20, 0, 10, 30}, long: 10, lat: 10, want: true},
		{l: &LimitTo{-20, 0, 10, 30}, long: -20, lat: 0, want: true},
		{l: &LimitTo{-20, 0, 10, 30}, long: -20, lat: 30, want: true},
		{l: &LimitTo{-20, 0, 10, 30}, long: 10, lat: 0, want: true},
		{l: &LimitTo{-20, 0, 10, 30}, long: 10, lat: 30, want: true},
	} {
		t.Run("", func(t *testing.T) {
			if got := tt.l.Contains(tt.long, tt.lat); tt.want != got {
				t.Errorf("check for contains of %f %f in %v was %v, want %v", tt.long, tt.lat, tt.l, got, tt.want)
			}
		})
	}
}

func TestLimitTo_Intersects(t *testing.T) {
	for _, tt := range []struct {
		l     *LimitTo
		check [4]float64
		want  bool
	}{
		{l: nil, check: [4]float64{-100, -100, 100, 100}, want: true},
		{l: nil, check: [4]float64{0, 0, 0, 0}, want: true},
		{l: &LimitTo{-20, 0, 10, 30}, check: [4]float64{0, 0, 0, 0}, want: true},
		{l: &LimitTo{-20, 0, 10, 30}, check: [4]float64{-100, -100, 100, 100}, want: true},
		{l: &LimitTo{-20, 0, 10, 30}, check: [4]float64{-10, 10, 5, 20}, want: true},
		{l: &LimitTo{-20, 0, 10, 30}, check: [4]float64{-30, 10, -20.01, 20}, want: false},
		{l: &LimitTo{-20, 0, 10, 30}, check: [4]float64{-30, 10, -20.0, 20}, want: true},
		{l: &LimitTo{-20, 0, 10, 30}, check: [4]float64{-30, 30.01, -10.0, 40}, want: false},
		{l: &LimitTo{-20, 0, 10, 30}, check: [4]float64{-30, 30, -10.0, 40}, want: true},
		{l: &LimitTo{-20, 0, 10, 30}, check: [4]float64{-30, -30, -10.0, 0}, want: true},
		{l: &LimitTo{-20, 0, 10, 30}, check: [4]float64{-30, -30, -10.0, -0.01}, want: false},
	} {
		t.Run("", func(t *testing.T) {
			if got := tt.l.Intersects(tt.check); tt.want != got {
				t.Errorf("check for intersection of %v and %v was %v, want %v", tt.check, tt.l, got, tt.want)
			}
		})
	}
}
