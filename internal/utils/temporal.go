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
	"context"
	"errors"
	"fmt"
	"slices"
	"time"

	logf "sigs.k8s.io/controller-runtime/pkg/log"

	workloadsv1alpha1 "sigs.k8s.io/workload-class/api/v1alpha1"
)

// WeekdaysValid checks if a given slice of strings contains only valid,
// fully-capitalized days of the week (e.g., "Monday", "Tuesday").
// It returns an error if any unrecognized or incorrectly formatted days are found.
func WeekdaysValid(days []string) error {
	validWeekdays := AllWeekdays()

	var invalid []string
	for _, d := range days {
		if !slices.Contains(validWeekdays, d) {
			invalid = append(invalid, d)
		}
	}
	if len(invalid) > 0 {
		return fmt.Errorf("`daysOfWeek` must only contain %s, got %s", QuotedList(validWeekdays), QuotedList(invalid))
	}

	return nil
}

// TimeZoneValid checks if the provided string represents a valid IANA Time Zone
// identifier (e.g., "America/New_York", "UTC"). It returns true if the time zone
// is recognized and can be loaded by the system, and false otherwise.
// "Local" is explicitly rejected because it resolves to the controller container's
// local timezone.
func TimeZoneValid(timeZone string) bool {
	if isLocalTimeZone(timeZone) {
		return false
	}
	_, err := time.LoadLocation(timeZone)
	return err == nil
}

// isLocalTimeZone returns true if timeZone is exactly "Local" or "local", matching the CRD CEL rule on
// DisruptionWindow.timeZone. The comparison is deliberately case-sensitive, because the Kubernetes API
// conventions forbid case-insensitive comparisons in validation.
func isLocalTimeZone(timeZone string) bool {
	return timeZone == "Local" || timeZone == "local"
}

// windowTransitionSearchDays bounds the search for the next disruption window transition. Windows repeat weekly, so
// the next transition, if there is one, is within a week plus the spillover of a window that wraps around midnight.
const windowTransitionSearchDays = 8

// IsTimeInWindows checks if the given time is within any of the defined disruption windows.
// Returns true if in window, and the duration until the next state change (window opens or closes).
// The duration is 0 if the state never changes, e.g. when no windows are defined, no window is valid, or the windows
// cover every day of the week.
//
// A window includes its start time and excludes its end time. Window times are interpreted in the window's time zone
// on each calendar day: a time that does not exist on a day because clocks are turned forward is moved to the instant
// of the change, and a time that exists twice because clocks are turned back is its first occurrence.
func IsTimeInWindows(ctx context.Context, nowUTC time.Time, windows []workloadsv1alpha1.DisruptionWindow) (bool, time.Duration) {
	log := logf.FromContext(ctx)
	if len(windows) == 0 {
		log.V(1).Info("WorkloadClass does not contain AllowedDisruptionWindows")
		return true, 0
	}

	parsed := make([]parsedWindow, 0, len(windows))
	for _, w := range windows {
		p, err := parseWindow(w)
		if err != nil {
			log.Error(err, "Skipping invalid DisruptionWindow", "window", w.Name)
			continue
		}
		parsed = append(parsed, p)
	}

	inWindow := inAnyWindow(nowUTC, parsed)
	next, ok := nextWindowTransition(nowUTC, parsed, inWindow)
	if !ok {
		return inWindow, 0
	}
	return inWindow, max(next.Sub(nowUTC), time.Nanosecond)
}

// timeOfDay is a wall-clock time with minute precision.
type timeOfDay struct {
	hour, minute int
}

// before returns true if t is earlier in the day than other.
func (t timeOfDay) before(other timeOfDay) bool {
	return t.hour*60+t.minute < other.hour*60+other.minute
}

// parsedWindow is a DisruptionWindow whose time zone and times have been parsed.
type parsedWindow struct {
	daysOfWeek []string // Empty means every day of the week.
	location   *time.Location
	start, end timeOfDay
}

// parseWindow parses the DisruptionWindow's time zone (UTC if unset), and its start and end times of day.
func parseWindow(w workloadsv1alpha1.DisruptionWindow) (parsedWindow, error) {
	var err error
	location := "Etc/UTC"
	if w.TimeZone != "" {
		location = w.TimeZone
	}
	if isLocalTimeZone(location) {
		err = errors.Join(err, fmt.Errorf("`timeZone` must not be %s, got '%s'", QuotedList([]string{"Local", "local"}), w.TimeZone))
	}
	timeZone, timeZoneErr := time.LoadLocation(location)
	if timeZoneErr != nil {
		err = errors.Join(err, timeZoneErr)
	}

	// hoursMinutesOnly is the target layout, using Go's standard reference timestamp
	// Mon Jan 2 15:04:05 MST 2006
	const hoursMinutesOnly = "15:04"

	start, startErr := time.Parse(hoursMinutesOnly, w.StartTime)
	if startErr != nil {
		err = errors.Join(err, startErr)
	}
	end, endErr := time.Parse(hoursMinutesOnly, w.EndTime)
	if endErr != nil {
		err = errors.Join(err, endErr)
	}
	if err != nil {
		return parsedWindow{}, err
	}

	return parsedWindow{
		daysOfWeek: w.DaysOfWeek,
		location:   timeZone,
		start:      timeOfDay{start.Hour(), start.Minute()},
		end:        timeOfDay{end.Hour(), end.Minute()},
	}, nil
}

// activeOn returns true if the window starts on the given calendar date in its time zone.
func (w parsedWindow) activeOn(year int, month time.Month, day int) bool {
	if len(w.daysOfWeek) == 0 {
		return true
	}
	// Civil arithmetic in UTC normalizes day overflow and underflow without time zone effects.
	weekday := time.Date(year, month, day, 12, 0, 0, 0, time.UTC).Weekday()
	return slices.Contains(w.daysOfWeek, weekday.String())
}

// contains returns true if t is within the window.
//
// DaysOfWeek refers to the window's start day in its time zone. For a window that wraps around midnight
// (e.g. 22:00 to 04:00), [start, 24:00) is active when today is in DaysOfWeek, and [00:00, end) is the spillover from
// yesterday night's window, so it is active when yesterday was in DaysOfWeek. A window whose start equals its end
// covers the whole day.
func (w parsedWindow) contains(t time.Time) bool {
	local := t.In(w.location)
	year, month, day := local.Date()
	todayActive := w.activeOn(year, month, day)
	yesterdayActive := w.activeOn(year, month, day-1)
	if !todayActive && !yesterdayActive {
		return false
	}

	start := localTime(year, month, day, w.start, w.location)
	end := localTime(year, month, day, w.end, w.location)
	switch {
	case w.start == w.end:
		return todayActive
	case w.end.before(w.start):
		return (todayActive && !t.Before(start)) || (yesterdayActive && t.Before(end))
	default:
		return todayActive && !t.Before(start) && t.Before(end)
	}
}

// boundaries returns the instants, from the start of the current calendar day in the window's time zone through the
// next windowTransitionSearchDays days, at which the window starts or ends, or the calendar day changes.
func (w parsedWindow) boundaries(nowUTC time.Time) []time.Time {
	year, month, day := nowUTC.In(w.location).Date()
	midnight := timeOfDay{}
	boundaries := make([]time.Time, 0, 3*(windowTransitionSearchDays+1))
	for days := 0; days <= windowTransitionSearchDays; days++ {
		boundaries = append(boundaries,
			localTime(year, month, day+days, midnight, w.location),
			localTime(year, month, day+days, w.start, w.location),
			localTime(year, month, day+days, w.end, w.location),
		)
	}
	return boundaries
}

// inAnyWindow returns true if t is within any of the windows.
func inAnyWindow(t time.Time, windows []parsedWindow) bool {
	for _, w := range windows {
		if w.contains(t) {
			return true
		}
	}
	return false
}

// nextWindowTransition returns the earliest instant, not before nowUTC, at which inAnyWindow changes from inWindow.
// It returns false if inAnyWindow does not change within windowTransitionSearchDays, e.g. because there are no valid
// windows or the windows cover every day of the week.
//
// Whether an instant is within a window can only change when the window starts or ends, or when the calendar day
// changes in the window's time zone, so only those candidate instants are evaluated. Windows include their start and
// exclude their end, so each candidate is evaluated just after it, which is the state that holds from the candidate on.
func nextWindowTransition(nowUTC time.Time, windows []parsedWindow, inWindow bool) (time.Time, bool) {
	candidates := make([]time.Time, 0, len(windows)*3*(windowTransitionSearchDays+1))
	for _, w := range windows {
		candidates = append(candidates, w.boundaries(nowUTC)...)
	}
	slices.SortFunc(candidates, time.Time.Compare)
	candidates = slices.CompactFunc(candidates, time.Time.Equal)

	for _, candidate := range candidates {
		if candidate.Before(nowUTC) {
			continue
		}
		if inAnyWindow(candidate.Add(time.Nanosecond), windows) != inWindow {
			return candidate, true
		}
	}
	return time.Time{}, false
}

// localTime returns the instant at which the wall-clock time tod occurs on the calendar date (year, month, day) in
// loc. The day may be outside the month's range and is normalized like time.Date.
//
// Unlike time.Date, the result is well defined across daylight saving time changes: if the wall-clock time occurs
// twice because clocks are turned back, the first occurrence is returned; if it does not occur because clocks are
// turned forward past it, the instant of the change is returned. In particular, localTime with a midnight tod is the
// first instant of the calendar day even in time zones that change clocks at midnight.
func localTime(year int, month time.Month, day int, tod timeOfDay, loc *time.Location) time.Time {
	// civil is the wall-clock time as if it were UTC. Subtracting a zone offset from it yields the instant at which
	// that wall-clock time is shown in a zone with that offset.
	civil := time.Date(year, month, day, tod.hour, tod.minute, 0, 0, time.UTC)
	guess := time.Date(year, month, day, tod.hour, tod.minute, 0, 0, loc)
	_, offsetBefore := guess.Add(-24 * time.Hour).Zone()
	_, offsetGuess := guess.Zone()
	_, offsetAfter := guess.Add(24 * time.Hour).Zone()

	var earliest time.Time
	for _, offset := range []int{offsetBefore, offsetGuess, offsetAfter} {
		instant := civil.Add(-time.Duration(offset) * time.Second)
		if showsWallClock(instant, loc, civil) && (earliest.IsZero() || instant.Before(earliest)) {
			earliest = instant
		}
	}
	if !earliest.IsZero() {
		return earliest
	}

	// The wall-clock time falls in a gap: clocks were turned forward past it. Find the instant of the change by
	// binary search between the instants the time would have in the zone before and after the change.
	lo := civil.Add(-time.Duration(max(offsetBefore, offsetAfter)) * time.Second).In(loc)
	hi := civil.Add(-time.Duration(min(offsetBefore, offsetAfter)) * time.Second).In(loc)
	_, loOffset := lo.Zone()
	for hi.Sub(lo) > time.Second {
		mid := lo.Add((hi.Sub(lo) / 2).Truncate(time.Second))
		if _, offset := mid.Zone(); offset == loOffset {
			lo = mid
		} else {
			hi = mid
		}
	}
	return hi.UTC()
}

// showsWallClock returns true if instant, shown in loc, has the calendar date, hour, and minute of civil.
func showsWallClock(instant time.Time, loc *time.Location, civil time.Time) bool {
	local := instant.In(loc)
	return local.Year() == civil.Year() && local.Month() == civil.Month() && local.Day() == civil.Day() &&
		local.Hour() == civil.Hour() && local.Minute() == civil.Minute()
}
