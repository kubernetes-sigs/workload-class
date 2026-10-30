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
// The duration is 0 if the state never changes, e.g. when no windows are defined or no window is valid.
func IsTimeInWindows(ctx context.Context, nowUTC time.Time, windows []workloadsv1alpha1.DisruptionWindow) (bool, time.Duration) {
	log := logf.FromContext(ctx)
	if len(windows) == 0 {
		log.Info("WorkloadClass does not contain AllowedDisruptionWindows")
		return true, 0
	}

	inWindow := false
	for _, w := range windows {
		inCurrentWindow, err := windowContains(nowUTC, w)
		if err != nil {
			log.Error(err, fmt.Sprintf("Error getting start and end times from DisruptionWindow %s", w.Name))
			continue
		}
		inWindow = inWindow || inCurrentWindow
	}

	next, ok := nextWindowTransition(nowUTC, windows, inWindow)
	if !ok {
		return inWindow, 0
	}
	return inWindow, max(next.Sub(nowUTC), time.Nanosecond)
}

// windowContains returns true if nowUTC is within the DisruptionWindow.
func windowContains(nowUTC time.Time, w workloadsv1alpha1.DisruptionWindow) (bool, error) {
	start, end, timeZone, err := windowInfo(nowUTC, w)
	if err != nil {
		return false, err
	}

	localNow := nowUTC.In(timeZone)
	todayActive := len(w.DaysOfWeek) == 0 || slices.Contains(w.DaysOfWeek, localNow.Weekday().String())
	yesterdayActive := len(w.DaysOfWeek) == 0 || slices.Contains(w.DaysOfWeek, localNow.AddDate(0, 0, -1).Weekday().String())
	if !todayActive && !yesterdayActive {
		return false, nil
	}

	return evaluateWindow(start, end, nowUTC, todayActive, yesterdayActive), nil
}

// inAnyWindow returns true if nowUTC is within any valid DisruptionWindow.
func inAnyWindow(nowUTC time.Time, windows []workloadsv1alpha1.DisruptionWindow) bool {
	for _, w := range windows {
		if in, err := windowContains(nowUTC, w); err == nil && in {
			return true
		}
	}
	return false
}

// nextWindowTransition returns the earliest time, not before nowUTC, at which inAnyWindow changes from inWindow.
// It returns false if inAnyWindow does not change within windowTransitionSearchDays, e.g. because no window is valid
// or the windows cover every day of the week.
//
// Whether a time is within a window can only change at the window's start or end time, or at midnight (when the day
// of the week changes) in the window's time zone, so only those candidate times are evaluated. Windows exclude their
// start time, so each candidate is evaluated just after it.
func nextWindowTransition(nowUTC time.Time, windows []workloadsv1alpha1.DisruptionWindow, inWindow bool) (time.Time, bool) {
	candidates := make([]time.Time, 0, len(windows)*3*(windowTransitionSearchDays+1))
	for _, w := range windows {
		candidates = append(candidates, windowBoundaries(nowUTC, w)...)
	}
	slices.SortFunc(candidates, time.Time.Compare)

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

// windowBoundaries returns the times, from the start of the current day in the window's time zone through the next
// windowTransitionSearchDays days, at which the window starts or ends, or the day of the week changes.
// It returns nil if the window is invalid.
func windowBoundaries(nowUTC time.Time, w workloadsv1alpha1.DisruptionWindow) []time.Time {
	timeZone, start, end, err := parseWindow(w)
	if err != nil {
		return nil
	}

	localNow := nowUTC.In(timeZone)
	boundaries := make([]time.Time, 0, 3*(windowTransitionSearchDays+1))
	for days := 0; days <= windowTransitionSearchDays; days++ {
		day := time.Date(localNow.Year(), localNow.Month(), localNow.Day()+days, 0, 0, 0, 0, timeZone)
		boundaries = append(boundaries, day, atTimeOfDay(day, start), atTimeOfDay(day, end))
	}
	return boundaries
}

// windowInfo calculates the DisruptionWindow's start and end times in UTC for the current local day.
// It anchors the window's time to the date in the specified TimeZone before converting back to UTC.
// On error, it returns (nowUTC, nowUTC, nil, err).
func windowInfo(nowUTC time.Time, w workloadsv1alpha1.DisruptionWindow) (start, end time.Time, timeZone *time.Location, err error) {
	timeZone, startTime, endTime, err := parseWindow(w)
	if err != nil {
		return nowUTC, nowUTC, nil, err
	}

	localTime := nowUTC.In(timeZone)
	startUTC := atTimeOfDay(localTime, startTime).UTC()
	endUTC := atTimeOfDay(localTime, endTime).UTC()

	return startUTC, endUTC, timeZone, nil
}

// parseWindow parses the DisruptionWindow's time zone (UTC if unset), and its start and end times of day.
func parseWindow(w workloadsv1alpha1.DisruptionWindow) (timeZone *time.Location, start, end time.Time, err error) {
	location := "Etc/UTC"
	if w.TimeZone != "" {
		location = w.TimeZone
	}

	if isLocalTimeZone(location) {
		err = errors.Join(err, fmt.Errorf("`timeZone` must not be %s, got '%s'", QuotedList([]string{"Local", "local"}), w.TimeZone))
	}

	var timeZoneErr error
	timeZone, timeZoneErr = time.LoadLocation(location)
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

	return timeZone, start, end, err
}

// atTimeOfDay returns the time on day's date, in day's location, with the hour and minute of timeOfDay.
func atTimeOfDay(day, timeOfDay time.Time) time.Time {
	return time.Date(day.Year(), day.Month(), day.Day(), timeOfDay.Hour(), timeOfDay.Minute(), 0, 0, day.Location())
}

func evaluateWindow(start, end, nowUTC time.Time, todayActive, yesterdayActive bool) bool {
	// Handle windows that wrap around midnight (e.g. 22:00 to 04:00)
	if end.Before(start) {
		return evaluateCrossMidnightWindow(start, end, nowUTC, todayActive, yesterdayActive)
	}

	if !todayActive {
		return false
	}

	return evaluateSameDayWindow(start, end, nowUTC)
}

func evaluateCrossMidnightWindow(start, end, now time.Time, todayActive, yesterdayActive bool) bool {
	// DaysOfWeek refers to the window's start day in its local timezone:
	// - [start, 24:00) is active when today is in DaysOfWeek.
	// - [00:00, end) is the spillover from yesterday night's window, so it is active when yesterday was in DaysOfWeek.
	return (todayActive && now.After(start)) || (yesterdayActive && now.Before(end))
}

func evaluateSameDayWindow(start, end, now time.Time) bool {
	strictlyBetween := now.After(start) && now.Before(end)
	startEqualsEnd := start.Compare(end) == 0

	return strictlyBetween || startEqualsEnd
}
