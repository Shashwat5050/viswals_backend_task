package utils

import "time"


func ParseTimeStamp(timeStr string)(time.Duration,error){

	ttl,err:=time.ParseDuration(timeStr)
	if err != nil {
		return 0,err
	}
	return ttl,nil
}

// Helper function to convert Unix timestamp to *time.Time
func ConvertUnixToTime(unix int64) *time.Time {
	if unix == 0 {
		return nil
	}
	t := time.Unix(unix, 0)
	return &t
}

// Helper function to validate and fix invalid time.Time values
func FixInvalidTime(t *time.Time) *time.Time {
	if t != nil && (t.Year() < 0 || t.Year() > 9999) {
		// Replace invalid time with zero time
		validTime := time.Time{}
		return &validTime
	}
	return t
}