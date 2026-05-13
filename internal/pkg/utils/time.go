package utils

import "time"

func TaskSeqIdToTime(taskSeqId string) time.Time {
	if len(taskSeqId) != 14 {
		return time.Time{}
	}
	t, err := time.ParseInLocation("20060102150405", taskSeqId, GetTimeLocation())
	if err != nil {
		return time.Time{}
	}
	return t
}

func GetTimeLocation() *time.Location {
	loc, err := time.LoadLocation("Asia/Shanghai")
	if err != nil {
		return time.Local
	}
	return loc
}
