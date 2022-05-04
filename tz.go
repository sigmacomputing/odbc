package odbc

import (
	"regexp"
	"strings"
	"time"
)

// Regex to extract timezone for Databricks.
var tzRegex = regexp.MustCompile("SSP_timezone=(.*?);")

// Extracts the value of the SSP_timezone key from the given DSN. Returns nil
// if no timezone is specified, and an error if an invalid timezone is specified.
func extractTimezoneFromDsn(dsn string) (*time.Location, error) {
	matches := tzRegex.FindStringSubmatch(dsn)
	if len(matches) < 2 {
		return nil, nil
	}

	loc, err := time.LoadLocation(strings.TrimSpace(matches[1]))
	if err != nil {
		return nil, err
	}
	return loc, nil
}
