/*
Copyright 2026 The Kubernetes Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package utils

import (
	"testing"
	"time"

	workloadsv1alpha1 "sigs.k8s.io/workload-class/api/v1alpha1"
)

func TestWeekdaysValid(t *testing.T) {
	tests := []struct {
		name    string
		days    []string
		wantErr bool
	}{
		{
			name:    "empty days slice is valid",
			days:    nil,
			wantErr: false,
		},
		{
			name:    "valid weekdays",
			days:    []string{"Sunday", "Monday", "Saturday"},
			wantErr: false,
		},
		{
			name:    "invalid weekday name",
			days:    []string{"Monday", "Funday"},
			wantErr: true,
		},
		{
			name:    "lowercase weekday is rejected",
			days:    []string{"monday"},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := WeekdaysValid(tt.days)
			if (err != nil) != tt.wantErr {
				t.Errorf("WeekdaysValid(%v) error = %v, wantErr %v", tt.days, err, tt.wantErr)
			}
		})
	}
}

func TestWeekdaysValidMessage(t *testing.T) {
	err := WeekdaysValid([]string{"Monday", "Funday", "monday"})
	if err == nil {
		t.Fatal("WeekdaysValid() error = nil, want an error")
	}
	want := "`daysOfWeek` must only contain 'Sunday', 'Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', " +
		"got 'Funday', 'monday'"
	if err.Error() != want {
		t.Errorf("WeekdaysValid() error = %q, want %q", err.Error(), want)
	}
}

func TestTimeZoneValid(t *testing.T) {
	tests := []struct {
		name     string
		timeZone string
		want     bool
	}{
		{
			name:     "UTC is valid",
			timeZone: "UTC",
			want:     true,
		},
		{
			name:     "single-segment IANA zone EST is valid",
			timeZone: "EST",
			want:     true,
		},
		{
			name:     "single-segment IANA zone CET is valid",
			timeZone: "CET",
			want:     true,
		},
		{
			name:     "IANA region/city is valid",
			timeZone: "America/Los_Angeles",
			want:     true,
		},
		{
			name:     "three-segment IANA zone is valid",
			timeZone: "America/Argentina/Buenos_Aires",
			want:     true,
		},
		{
			name:     "invalid timezone returns false",
			timeZone: "Invalid/TimeZone",
			want:     false,
		},
		{
			name:     "Local timezone is rejected",
			timeZone: "Local",
			want:     false,
		},
		{
			name:     "lowercase local timezone is rejected",
			timeZone: "local",
			want:     false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := TimeZoneValid(tt.timeZone); got != tt.want {
				t.Errorf("TimeZoneValid(%q) = %v, want %v", tt.timeZone, got, tt.want)
			}
		})
	}
}

func TestIsTimeInWindows(t *testing.T) {
	tests := []struct {
		name    string
		now     time.Time
		windows []workloadsv1alpha1.DisruptionWindow
		wantIn  bool
	}{
		{
			name:    "No windows configured defaults to in window",
			now:     time.Date(2026, 4, 20, 23, 0, 0, 0, time.UTC),
			windows: nil,
			wantIn:  true,
		},
		{
			name: "Empty daysOfWeek on a window defaults to every day of the week",
			now:  time.Date(2026, 4, 20, 23, 0, 0, 0, time.UTC), // Monday 23:00
			windows: []workloadsv1alpha1.DisruptionWindow{
				{StartTime: "22:00", EndTime: "23:59"},
			},
			wantIn: true,
		},
		{
			name: "In window same day",
			now:  time.Date(2026, 4, 20, 23, 0, 0, 0, time.UTC), // Monday 23:00
			windows: []workloadsv1alpha1.DisruptionWindow{
				{DaysOfWeek: []string{"Monday"}, StartTime: "22:00", EndTime: "23:59"},
			},
			wantIn: true,
		},
		{
			name: "In window same day, start equals end",
			now:  time.Date(2026, 4, 20, 23, 0, 0, 0, time.UTC), // Monday 23:00
			windows: []workloadsv1alpha1.DisruptionWindow{
				{DaysOfWeek: []string{"Monday"}, StartTime: "12:00", EndTime: "12:00"},
			},
			wantIn: true,
		},
		{
			name: "In window same day, different time zone",
			now:  time.Date(2026, 4, 20, 23, 0, 0, 0, time.UTC), // Monday 23:00
			windows: []workloadsv1alpha1.DisruptionWindow{
				{DaysOfWeek: []string{"Monday"}, StartTime: "16:00", EndTime: "17:59", TimeZone: "America/Guatemala"},
			},
			wantIn: true,
		},
		{
			name: "Outside window same day",
			now:  time.Date(2026, 4, 20, 21, 0, 0, 0, time.UTC), // Monday 21:00
			windows: []workloadsv1alpha1.DisruptionWindow{
				{DaysOfWeek: []string{"Monday"}, StartTime: "22:00", EndTime: "23:59"},
			},
			wantIn: false,
		},
		{
			name: "Outside window same day, different time zone",
			now:  time.Date(2026, 4, 20, 21, 0, 0, 0, time.UTC), // Monday 21:00
			windows: []workloadsv1alpha1.DisruptionWindow{
				{DaysOfWeek: []string{"Monday"}, StartTime: "22:00", EndTime: "23:59", TimeZone: "Asia/Manila"},
			},
			wantIn: false,
		},
		{
			name: "In window same day, UTC and local dates differ (Tuesday UTC is Monday in America/Los_Angeles)",
			now:  time.Date(2026, 4, 21, 6, 0, 0, 0, time.UTC), // Tuesday 06:00 UTC == Monday 23:00 PDT
			windows: []workloadsv1alpha1.DisruptionWindow{
				{DaysOfWeek: []string{"Monday"}, StartTime: "22:00", EndTime: "23:59", TimeZone: "America/Los_Angeles"},
			},
			wantIn: true,
		},
		{
			name: "Outside window same day, UTC and local dates differ (Monday UTC is Tuesday in Asia/Manila)",
			now:  time.Date(2026, 4, 20, 23, 0, 0, 0, time.UTC), // Monday 23:00 UTC == Tuesday 07:00 Manila
			windows: []workloadsv1alpha1.DisruptionWindow{
				{DaysOfWeek: []string{"Monday"}, StartTime: "06:00", EndTime: "08:00", TimeZone: "Asia/Manila"},
			},
			wantIn: false,
		},
		{
			name: "Wraps around midnight - currently in (after start)",
			now:  time.Date(2026, 4, 20, 23, 0, 0, 0, time.UTC), // Monday 23:00
			windows: []workloadsv1alpha1.DisruptionWindow{
				{DaysOfWeek: []string{"Monday"}, StartTime: "22:00", EndTime: "04:00"},
			},
			wantIn: true,
		},
		{
			name: "Wraps around midnight - currently in (after start), different time zone",
			now:  time.Date(2026, 4, 20, 15, 0, 0, 0, time.UTC), // Monday 15:00 UTC == Monday 23:00 Manila
			windows: []workloadsv1alpha1.DisruptionWindow{
				{DaysOfWeek: []string{"Monday"}, StartTime: "22:00", EndTime: "04:00", TimeZone: "Asia/Manila"},
			},
			wantIn: true,
		},
		{
			name: "Wraps around midnight - currently in (before end on next day)",
			now:  time.Date(2026, 4, 21, 2, 0, 0, 0, time.UTC), // Tuesday 02:00 UTC (spillover from Monday night)
			windows: []workloadsv1alpha1.DisruptionWindow{
				{DaysOfWeek: []string{"Monday"}, StartTime: "22:00", EndTime: "04:00"},
			},
			wantIn: true,
		},
		{
			name: "Wraps around midnight - currently outside (before end on start day itself)",
			now:  time.Date(2026, 4, 20, 2, 0, 0, 0, time.UTC), // Monday 02:00 UTC (Sunday night was not in DaysOfWeek)
			windows: []workloadsv1alpha1.DisruptionWindow{
				{DaysOfWeek: []string{"Monday"}, StartTime: "22:00", EndTime: "04:00"},
			},
			wantIn: false,
		},
		{
			name: "Wraps around midnight - currently in (before end on next day), different time zone",
			now:  time.Date(2026, 4, 21, 8, 0, 0, 0, time.UTC), // Tuesday 08:00 UTC == Tuesday 02:00 Guatemala (spillover from Monday night)
			windows: []workloadsv1alpha1.DisruptionWindow{
				{DaysOfWeek: []string{"Monday"}, StartTime: "22:00", EndTime: "04:00", TimeZone: "America/Guatemala"},
			},
			wantIn: true,
		},
		{
			name: "Wraps around midnight - currently outside",
			now:  time.Date(2026, 4, 20, 10, 0, 0, 0, time.UTC), // Monday 10:00
			windows: []workloadsv1alpha1.DisruptionWindow{
				{DaysOfWeek: []string{"Monday"}, StartTime: "22:00", EndTime: "04:00"},
			},
			wantIn: false,
		},
		{
			name: "Wraps around midnight - currently outside, different time zone",
			now:  time.Date(2026, 4, 20, 10, 0, 0, 0, time.UTC), // Monday 10:00
			windows: []workloadsv1alpha1.DisruptionWindow{
				{DaysOfWeek: []string{"Monday"}, StartTime: "22:00", EndTime: "04:00", TimeZone: "America/Toronto"},
			},
			wantIn: false,
		},
	}

	ctx := t.Context()
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotIn, _ := IsTimeInWindows(ctx, tt.now, tt.windows)
			if gotIn != tt.wantIn {
				t.Errorf("IsTimeInWindows() gotIn = %v, want %v", gotIn, tt.wantIn)
			}
		})
	}
}

func TestIsTimeInWindowsNextTransition(t *testing.T) {
	// 2026-04-20 is a Monday.
	monday := func(hour, minute int) time.Time { return time.Date(2026, 4, 20, hour, minute, 0, 0, time.UTC) }

	tests := []struct {
		name     string
		now      time.Time
		windows  []workloadsv1alpha1.DisruptionWindow
		wantIn   bool
		wantWait time.Duration
	}{
		{
			name:     "No windows configured never changes",
			now:      monday(23, 0),
			windows:  nil,
			wantIn:   true,
			wantWait: 0,
		},
		{
			name: "Before a same-day window opens",
			now:  monday(21, 0),
			windows: []workloadsv1alpha1.DisruptionWindow{
				{DaysOfWeek: []string{"Monday"}, StartTime: "22:00", EndTime: "23:59"},
			},
			wantIn:   false,
			wantWait: time.Hour,
		},
		{
			name: "In a same-day window until it closes",
			now:  monday(23, 0),
			windows: []workloadsv1alpha1.DisruptionWindow{
				{DaysOfWeek: []string{"Monday"}, StartTime: "22:00", EndTime: "23:59"},
			},
			wantIn:   true,
			wantWait: 59 * time.Minute,
		},
		{
			name: "Exactly at the start of a window, which opens just after",
			now:  monday(22, 0),
			windows: []workloadsv1alpha1.DisruptionWindow{
				{DaysOfWeek: []string{"Monday"}, StartTime: "22:00", EndTime: "23:59"},
			},
			wantIn:   false,
			wantWait: time.Nanosecond,
		},
		{
			name: "In a window that wraps around midnight until it closes the next day",
			now:  monday(23, 0),
			windows: []workloadsv1alpha1.DisruptionWindow{
				{DaysOfWeek: []string{"Monday"}, StartTime: "22:00", EndTime: "04:00"},
			},
			wantIn:   true,
			wantWait: 5 * time.Hour,
		},
		{
			name: "In the spillover of a window that wraps around midnight",
			now:  time.Date(2026, 4, 21, 2, 0, 0, 0, time.UTC), // Tuesday 02:00
			windows: []workloadsv1alpha1.DisruptionWindow{
				{DaysOfWeek: []string{"Monday"}, StartTime: "22:00", EndTime: "04:00"},
			},
			wantIn:   true,
			wantWait: 2 * time.Hour,
		},
		{
			name: "Before a window that wraps around midnight opens",
			now:  monday(10, 0),
			windows: []workloadsv1alpha1.DisruptionWindow{
				{DaysOfWeek: []string{"Monday"}, StartTime: "22:00", EndTime: "04:00"},
			},
			wantIn:   false,
			wantWait: 12 * time.Hour,
		},
		{
			name: "Window on a later day of the week",
			now:  time.Date(2026, 4, 22, 9, 0, 0, 0, time.UTC), // Wednesday 09:00
			windows: []workloadsv1alpha1.DisruptionWindow{
				{DaysOfWeek: []string{"Friday"}, StartTime: "10:00", EndTime: "12:00"},
			},
			wantIn:   false,
			wantWait: 49 * time.Hour,
		},
		{
			name: "Window already closed today opens again next week",
			now:  time.Date(2026, 4, 24, 13, 0, 0, 0, time.UTC), // Friday 13:00
			windows: []workloadsv1alpha1.DisruptionWindow{
				{DaysOfWeek: []string{"Friday"}, StartTime: "10:00", EndTime: "12:00"},
			},
			wantIn:   false,
			wantWait: 7*24*time.Hour - 3*time.Hour,
		},
		{
			name: "Window in a different time zone",
			now:  monday(23, 0), // Monday 16:00 PDT
			windows: []workloadsv1alpha1.DisruptionWindow{
				{DaysOfWeek: []string{"Monday"}, StartTime: "22:00", EndTime: "23:59", TimeZone: "America/Los_Angeles"},
			},
			wantIn:   false,
			wantWait: 6 * time.Hour,
		},
		{
			name: "Overlapping windows close when the last one closes",
			now:  monday(22, 15),
			windows: []workloadsv1alpha1.DisruptionWindow{
				{Name: "first", StartTime: "22:00", EndTime: "23:00"},
				{Name: "second", StartTime: "22:30", EndTime: "23:59"},
			},
			wantIn:   true,
			wantWait: time.Hour + 44*time.Minute,
		},
		{
			name: "Adjacent windows close when the last one closes",
			now:  monday(21, 0),
			windows: []workloadsv1alpha1.DisruptionWindow{
				{Name: "first", StartTime: "20:00", EndTime: "22:00"},
				{Name: "second", StartTime: "22:00", EndTime: "23:00"},
			},
			wantIn:   true,
			wantWait: 2 * time.Hour,
		},
		{
			name: "Start equal to end covers the whole day",
			now:  monday(23, 0),
			windows: []workloadsv1alpha1.DisruptionWindow{
				{DaysOfWeek: []string{"Monday"}, StartTime: "12:00", EndTime: "12:00"},
			},
			wantIn:   true,
			wantWait: time.Hour,
		},
		{
			name: "Windows covering every day never change",
			now:  monday(23, 0),
			windows: []workloadsv1alpha1.DisruptionWindow{
				{StartTime: "00:00", EndTime: "00:00"},
			},
			wantIn:   true,
			wantWait: 0,
		},
		{
			name: "Invalid windows are ignored and never change",
			now:  monday(23, 0),
			windows: []workloadsv1alpha1.DisruptionWindow{
				{StartTime: "22:00", EndTime: "23:00", TimeZone: "Mars/Phobos"},
			},
			wantIn:   false,
			wantWait: 0,
		},
		{
			name: "Daylight saving time starts during the window",
			// 2026-03-08 09:30 UTC is 01:30 PST; clocks jump from 02:00 PST to 03:00 PDT (10:00 UTC).
			now: time.Date(2026, 3, 8, 9, 30, 0, 0, time.UTC),
			windows: []workloadsv1alpha1.DisruptionWindow{
				{StartTime: "01:00", EndTime: "03:00", TimeZone: "America/Los_Angeles"},
			},
			wantIn:   true,
			wantWait: 30 * time.Minute,
		},
		{
			name: "Daylight saving time started before the window opens",
			// 2026-03-08 08:30 UTC is 00:30 PST, and the window opens at 01:00 PST (09:00 UTC).
			now: time.Date(2026, 3, 8, 8, 30, 0, 0, time.UTC),
			windows: []workloadsv1alpha1.DisruptionWindow{
				{StartTime: "01:00", EndTime: "03:00", TimeZone: "America/Los_Angeles"},
			},
			wantIn:   false,
			wantWait: 30 * time.Minute,
		},
	}

	ctx := t.Context()
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotIn, gotWait := IsTimeInWindows(ctx, tt.now, tt.windows)
			if gotIn != tt.wantIn {
				t.Errorf("IsTimeInWindows() gotIn = %v, want %v", gotIn, tt.wantIn)
			}
			if gotWait != tt.wantWait {
				t.Errorf("IsTimeInWindows() gotWait = %v, want %v", gotWait, tt.wantWait)
			}
			if gotWait > 0 {
				// The state must be unchanged just before the transition, and changed just after it.
				if in, _ := IsTimeInWindows(ctx, tt.now.Add(gotWait-time.Nanosecond), tt.windows); gotWait > time.Nanosecond && in != gotIn {
					t.Errorf("IsTimeInWindows() just before the transition = %v, want %v", in, gotIn)
				}
				if in, _ := IsTimeInWindows(ctx, tt.now.Add(gotWait+time.Nanosecond), tt.windows); in == gotIn {
					t.Errorf("IsTimeInWindows() just after the transition = %v, want %v", in, !gotIn)
				}
			}
		})
	}
}
