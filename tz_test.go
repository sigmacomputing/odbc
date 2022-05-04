package odbc

import (
	"testing"
	"time"
)

func loc(t *testing.T, name string) *time.Location {
	l, err := time.LoadLocation(name)
	if err != nil {
		t.Fatal(err)
	}
	return l
}

func TestExtractTimezoneFromDsn(t *testing.T) {
	type test struct {
		dsn         string
		loc         *time.Location
		shouldError bool
	}

	tests := []test{
		{dsn: "", loc: nil, shouldError: false},
		{
			dsn:         "Driver=something;SSP_timezone=America/Los_Angeles;",
			loc:         loc(t, "America/Los_Angeles"),
			shouldError: false,
		},
		{
			dsn:         "Driver=something;SSP_timezone=America/New_York;ApplySSPWithQueries=0;",
			loc:         loc(t, "America/New_York"),
			shouldError: false,
		},
		{
			dsn:         "Driver=something;SSP_timezone=;ApplySSPWithQueries=0;",
			loc:         loc(t, "UTC"),
			shouldError: false,
		},
		{
			dsn:         "Driver=something;ApplySSPWithQueries=0;",
			loc:         nil,
			shouldError: false,
		},
		{
			dsn:         "Driver=something;SSP_timezone=GMT-8;ApplySSPWithQueries=0;",
			loc:         nil,
			shouldError: true,
		},
	}

	for _, tc := range tests {
		res, err := extractTimezoneFromDsn(tc.dsn)
		if tc.shouldError && err == nil {
			t.Errorf("Expected error, but got none")
		} else if !tc.shouldError && err != nil {
			t.Error(err)
		}

		if res.String() != tc.loc.String() {
			t.Errorf("Expected %s but got %s", tc.loc, res)
		}
	}
}
