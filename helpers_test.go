package skaidb

import (
	"math"
	"time"
)

func timeFromMilli(ms int64) time.Time { return time.UnixMilli(ms).UTC() }
func nanFloat() float64                { return math.NaN() }
