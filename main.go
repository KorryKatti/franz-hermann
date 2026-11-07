package main

import (
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/go-fluxer/gofluxer"
	"github.com/joho/godotenv"
	_ "github.com/mattn/go-sqlite3"
	"io"
	"log"
	"net/http"
	"os"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

var DB *sql.DB

const year = 2026

type Session struct {
	SessionKey       int       `json:"session_key"`
	SessionType      string    `json:"session_type"`
	SessionName      string    `json:"session_name"`
	DateStart        time.Time `json:"date_start"`
	DateEnd          time.Time `json:"date_end"`
	MeetingKey       int       `json:"meeting_key"`
	CircuitKey       int       `json:"circuit_key"`
	CircuitShortName string    `json:"circuit_short_name"`
	CountryKey       int       `json:"country_key"`
	CountryCode      string    `json:"country_code"`
	CountryName      string    `json:"country_name"`
	Location         string    `json:"location"`
	GMTOffset        string    `json:"gmt_offset"`
	Year             int       `json:"year"`
	IsCancelled      bool      `json:"is_cancelled"`
}

type RaceCache struct {
	UpdatedAt time.Time `json:"updated_at"`
	Races     []Session `json:"races"`
}

// httpClient bounds every OpenF1 call. The default http.Get has no
// timeout, so one hung request would freeze the bot indefinitely.
var httpClient = &http.Client{Timeout: 10 * time.Second}

// errDriverNotFound lets handlers tell "unknown number" apart from a
// real failure, so the user gets "I don't know that driver" instead
// of a raw error (or, worse, silence).
var errDriverNotFound = errors.New("driver not found")

// asyncHandler runs a command off the gateway read loop. gofluxer
// dispatches commands synchronously, so a slow OpenF1 call inside a
// handler would block all other messages. Also turns a panic into a
// visible reply instead of a dead process / empty channel.
func asyncHandler(bot *gofluxer.Bot, fn func(m *gofluxer.Message, args []string)) func(m *gofluxer.Message, args []string) {
	return func(m *gofluxer.Message, args []string) {
		go func() {
			defer func() {
				if r := recover(); r != nil {
					log.Printf("panic in command handler: %v", r)
					bot.SendMessage(m.ChannelID, "Something went wrong handling that command.")
				}
			}()
			fn(m, args)
		}()
	}
}

// startTime is captured at package init for the `ping` uptime report.
var startTime = time.Now()

// openF1PingTTL bounds OpenF1 ping probes: at most one real request
// every 2 hours, everybody else gets the cached latency.
const openF1PingTTL = 2 * time.Hour

// siteURL is the bot's page, linked from every embed's title.
const siteURL = "https://korrykatti.github.io/franz-hermann"

type pingCache struct {
	mu        sync.Mutex
	latency   time.Duration
	err       error
	checkedAt time.Time
}

var openF1Ping pingCache

// measureLatency does one timed GET and drains the body (connection
// reuse). The status code is reported via err, but the latency is
// still meaningful — that's what the ping measures.
func measureLatency(url string, authToken string) (time.Duration, error) {
	req, err := http.NewRequest(http.MethodGet, url, nil)
	if err != nil {
		return 0, err
	}
	if authToken != "" {
		req.Header.Set("Authorization", "Bot "+authToken)
	}
	start := time.Now()
	resp, err := httpClient.Do(req)
	latency := time.Since(start)
	if err != nil {
		return latency, err
	}
	defer resp.Body.Close()
	io.Copy(io.Discard, io.LimitReader(resp.Body, 64*1024))
	if resp.StatusCode != http.StatusOK {
		return latency, fmt.Errorf("status %s", resp.Status)
	}
	return latency, nil
}

// getOpenF1Ping returns the OpenF1 latency, probing at most once per
// openF1PingTTL. fresh reports whether a new probe was just made.
func getOpenF1Ping() (latency time.Duration, err error, checkedAt time.Time, fresh bool) {
	openF1Ping.mu.Lock()
	defer openF1Ping.mu.Unlock()
	if !openF1Ping.checkedAt.IsZero() && time.Since(openF1Ping.checkedAt) < openF1PingTTL {
		return openF1Ping.latency, openF1Ping.err, openF1Ping.checkedAt, false
	}
	// Tiny real API call (one driver row, ~400 bytes) — a representative
	// OpenF1 round-trip. session_key 11404 is a finished historical
	// session, so it stays valid as a probe forever.
	latency, err = measureLatency("https://api.openf1.org/v1/drivers?driver_number=1&session_key=11404", "")
	openF1Ping.latency, openF1Ping.err, openF1Ping.checkedAt = latency, err, time.Now()
	return latency, err, openF1Ping.checkedAt, true
}

// --- system resource usage, read from /proc (Linux; the container is
// Alpine). Everything degrades to "n/a" instead of failing `ping`. ---

const linuxClockTicksPerSec = 100 // USER_HZ on Linux

func processCPUSeconds() (float64, error) {
	data, err := os.ReadFile("/proc/self/stat")
	if err != nil {
		return 0, err
	}
	// Field 2 (comm) is parenthesised and may contain spaces, so split
	// after the last ')' — fields then start at field 3 (state).
	idx := strings.LastIndex(string(data), ")")
	if idx < 0 {
		return 0, fmt.Errorf("bad /proc/self/stat")
	}
	f := strings.Fields(string(data)[idx+1:])
	// utime/stime are fields 14/15 overall => indexes 11/12 here.
	utime, err1 := strconv.ParseFloat(f[11], 64)
	stime, err2 := strconv.ParseFloat(f[12], 64)
	if err1 != nil || err2 != nil {
		return 0, fmt.Errorf("bad /proc/self/stat cpu fields")
	}
	return (utime + stime) / linuxClockTicksPerSec, nil
}

func systemCPUCounters() (busy, total uint64, err error) {
	data, err := os.ReadFile("/proc/stat")
	if err != nil {
		return 0, 0, err
	}
	line := strings.SplitN(string(data), "\n", 2)[0]
	f := strings.Fields(line)
	if len(f) < 8 || f[0] != "cpu" {
		return 0, 0, fmt.Errorf("bad /proc/stat")
	}
	vals := make([]uint64, len(f)-1)
	for i, s := range f[1:] {
		vals[i], err = strconv.ParseUint(s, 10, 64)
		if err != nil {
			return 0, 0, err
		}
		total += vals[i]
	}
	idle := vals[3] + vals[4] // idle + iowait
	return total - idle, total, nil
}

var cpuSampler = struct {
	sync.Mutex
	lastProc  float64
	lastBusy  uint64
	lastTotal uint64
	lastTime  time.Time
}{}

// Seed the sampler at process start so the very first `ping` already
// reports CPU averages since startup, not a meaningless 0%.
func init() {
	proc, _ := processCPUSeconds()
	busy, total, _ := systemCPUCounters()
	cpuSampler.lastProc, cpuSampler.lastBusy, cpuSampler.lastTotal, cpuSampler.lastTime = proc, busy, total, startTime
}

// sampleCPU returns (process %, system %) of total CPU capacity.
// The first call reports averages since process start.
func sampleCPU() (procPct, sysPct float64, ok bool) {
	procSec, err1 := processCPUSeconds()
	busy, total, err2 := systemCPUCounters()
	if err1 != nil && err2 != nil {
		return 0, 0, false
	}
	now := time.Now()
	cpuSampler.Lock()
	defer cpuSampler.Unlock()
	if cpuSampler.lastTime.IsZero() {
		wall := now.Sub(startTime).Seconds()
		if wall > 0 && err1 == nil {
			procPct = procSec / wall * 100 / float64(runtime.NumCPU())
		}
	} else {
		wall := now.Sub(cpuSampler.lastTime).Seconds()
		if wall > 0 {
			if err1 == nil {
				procPct = (procSec - cpuSampler.lastProc) / wall * 100 / float64(runtime.NumCPU())
			}
			if err2 == nil && total > cpuSampler.lastTotal {
				sysPct = float64(busy-cpuSampler.lastBusy) / float64(total-cpuSampler.lastTotal) * 100
			}
		}
	}
	cpuSampler.lastProc, cpuSampler.lastBusy, cpuSampler.lastTotal, cpuSampler.lastTime = procSec, busy, total, now
	return procPct, sysPct, true
}

func systemMemUsage() (usedPct float64, totalMB, availMB float64, ok bool) {
	data, err := os.ReadFile("/proc/meminfo")
	if err != nil {
		return 0, 0, 0, false
	}
	for _, line := range strings.Split(string(data), "\n") {
		f := strings.Fields(line)
		if len(f) < 2 {
			continue
		}
		v, err := strconv.ParseFloat(f[1], 64)
		if err != nil {
			continue
		}
		switch f[0] {
		case "MemTotal:":
			totalMB = v / 1024
		case "MemAvailable:":
			availMB = v / 1024
		}
	}
	if totalMB <= 0 {
		return 0, 0, 0, false
	}
	return (totalMB - availMB) / totalMB * 100, totalMB, availMB, true
}

func loadAvg() (string, bool) {
	data, err := os.ReadFile("/proc/loadavg")
	if err != nil {
		return "", false
	}
	f := strings.Fields(string(data))
	if len(f) < 1 {
		return "", false
	}
	return f[0], true // 1-minute load average
}

func rssMB() (float64, bool) {
	data, err := os.ReadFile("/proc/self/status")
	if err != nil {
		return 0, false
	}
	for _, line := range strings.Split(string(data), "\n") {
		if strings.HasPrefix(line, "VmRSS:") {
			f := strings.Fields(line)
			if len(f) >= 2 {
				v, err := strconv.ParseFloat(f[1], 64)
				if err == nil {
					return v / 1024, true
				}
			}
		}
	}
	return 0, false
}

func fmtMs(d time.Duration) string {
	return fmt.Sprintf("%.1f ms", float64(d)/float64(time.Millisecond))
}

const (
	PREFIX = ">"

	raceDayLead  = time.Hour
	raceDayGrace = time.Hour
)

// raceUnderway gates the short cache ttl while a race/sprint is on
var raceUnderway atomic.Bool
var lastRaceFetch time.Time
var statusIndex int
var rotatingStatuses []string
var lastStatusRotation time.Time

func getRaces() ([]Session, error) {
	raceTTL := 72 * time.Hour
	if raceUnderway.Load() {
		raceTTL = 35 * time.Minute
	}

	var stale []Session

	data, err := os.ReadFile("data/race.json")

	if err == nil {
		var cache RaceCache
		if json.Unmarshal(data, &cache) == nil &&
			!cache.UpdatedAt.IsZero() {
			stale = cache.Races
			if time.Since(cache.UpdatedAt) < raceTTL {
				return cache.Races, nil
			}
		}
	}

	// stale cache on api failure so we dont hammer a 429
	if len(stale) > 0 && !lastRaceFetch.IsZero() && time.Since(lastRaceFetch) < raceTTL {
		return stale, nil
	}
	lastRaceFetch = time.Now()

	url := fmt.Sprintf(
		"https://api.openf1.org/v1/sessions?year=%d&session_type=Race",
		year,
	)

	resp, err := httpClient.Get(url)

	if err != nil {
		if len(stale) > 0 {
			return stale, nil
		}
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		if len(stale) > 0 {
			return stale, nil
		}
		return nil, fmt.Errorf("OpenF1 returned status: %s", resp.Status)
	}

	var races []Session
	if err := json.NewDecoder(resp.Body).Decode(&races); err != nil {
		if len(stale) > 0 {
			return stale, nil
		}
		return nil, err
	}

	os.MkdirAll("data", 0755)

	cache := RaceCache{
		UpdatedAt: time.Now().UTC(),
		Races:     races,
	}

	data, _ = json.MarshalIndent(cache, "", "  ")
	if err := os.WriteFile("data/race.json", data, 0644); err != nil {
		return nil, err
	}

	return races, nil
}

func getStatus() string {
	races, err := getRaces()
	if err != nil {
		log.Println("Race API error:", err)
		return " F1 | Data unavailable"
	}

	now := time.Now().UTC()

	var raceDay Session
	var raceDayStart time.Time
	foundRaceDay := false

	var next Session
	var nextDate time.Time
	foundNext := false

	for i := range races {
		if races[i].IsCancelled {
			continue
		}

		start := races[i].DateStart

		remaining := start.Sub(now)

		if remaining <= raceDayLead && now.Sub(races[i].DateEnd) <= raceDayGrace {
			if foundRaceDay == false || start.Before(raceDayStart) {
				raceDay = races[i]
				raceDayStart = start
				foundRaceDay = true
			}
			continue
		}

		// Already over (past the window) or started and grace expired.
		if remaining <= 0 {
			continue
		}

		if foundNext == false || start.Before(nextDate) {
			next = races[i]
			nextDate = start
			foundNext = true
		}
	}

	raceUnderway.Store(foundRaceDay)

	if foundRaceDay {
		return fmt.Sprintf("🏁 RACE DAY at %s", raceDay.Location)
	}

	countdown := "🏁 No upcoming races"

	if foundNext {
		remaining := nextDate.Sub(now)

		if remaining < 24*time.Hour {
			hours := int(remaining.Hours())
			unit := "hours"
			if hours == 1 {
				unit = "hour"
			}
			countdown = fmt.Sprintf(" %s | %d %s until race", next.Location, hours, unit)
		} else {
			days := int(remaining / (24 * time.Hour))
			unit := "days"
			if days == 1 {
				unit = "day"
			}
			countdown = fmt.Sprintf(" %s | %d %s until race", next.Location, days, unit)
		}
	}

	if len(rotatingStatuses) == 0 || time.Since(lastStatusRotation) >= 15*time.Minute {
		if len(rotatingStatuses) > 0 {
			statusIndex = (statusIndex + 1) % len(rotatingStatuses)
		}

		rotatingStatuses = []string{countdown}

		if nextSession, err := getNextSession(); err == nil && nextSession != nil {
			place := nextSession.Location
			if place == "" {
				place = nextSession.CircuitShortName
			}
			if place == "" {
				rotatingStatuses = append(rotatingStatuses, fmt.Sprintf("Next: %s", nextSession.SessionName))
			} else {
				rotatingStatuses = append(rotatingStatuses, fmt.Sprintf("Next: %s · %s", nextSession.SessionName, place))
			}
		}

		if standings, err := getDriverStandings(); err == nil && len(standings) > 0 {
			name := fmt.Sprintf("#%d", standings[0].DriverNumber)
			if driver, err := getDriverName(standings[0].DriverNumber); err == nil && driver.NameAcronym != "" {
				name = driver.NameAcronym
			}
			rotatingStatuses = append(rotatingStatuses, fmt.Sprintf("Leader: %s · %.1f pts", name, standings[0].PointsCurrent))
		}

		if standings, err := getLastStandings(); err == nil && len(standings) > 0 {
			name := fmt.Sprintf("#%d", standings[0].DriverNumber)
			if driver, err := getDriverName(standings[0].DriverNumber); err == nil && driver.NameAcronym != "" {
				name = driver.NameAcronym
			}
			rotatingStatuses = append(rotatingStatuses, fmt.Sprintf("Last race P1: %s", name))
		}

		rotatingStatuses = append(rotatingStatuses, ">help for commands")

		if statusIndex >= len(rotatingStatuses) {
			statusIndex = 0
		}
		lastStatusRotation = time.Now()
	}

	return rotatingStatuses[statusIndex]
}

type driverStandings struct {
	MeetingKey      int     `json:"meeting_key"`
	SessionKey      int     `json:"session_key"`
	DriverNumber    int     `json:"driver_number"`
	PositionStart   int     `json:"position_start"`
	PositionCurrent int     `json:"position_current"`
	PointsStart     float64 `json:"points_start"`
	PointsCurrent   float64 `json:"points_current"`
}

type driverStandingCache struct {
	UpdatedAt time.Time         `json:"updated_at"`
	Standings []driverStandings `json:"standings"`
}

func getSessionKey(races []Session, which string) int {
	// possible thigs in which is prev and curr , if prev return prev session id and if curr return today's session id and if not found default to prev
	now := time.Now().UTC()
	var previous Session
	var current Session
	foundPrevious := false
	foundCurrent := false

	for _, race := range races {

		if race.IsCancelled || race.SessionName != "Race" {
			continue
		}

		// Find most recently completed race
		if race.DateEnd.Before(now) {
			if !foundPrevious || race.DateEnd.After(previous.DateEnd) {
				previous = race
				foundPrevious = true
			}
		}

		// Find today's race
		if race.DateStart.Year() == now.Year() &&
			race.DateStart.YearDay() == now.YearDay() &&
			!race.DateStart.After(now) {

			current = race
			foundCurrent = true
		}
	}

	if which == "curr" && foundCurrent {
		return current.SessionKey
	}

	if foundPrevious {
		return previous.SessionKey
	}

	return 0
}

type driverNumberType struct {
	MeetingKey    int    `json:"meeting_key"`
	SessionKey    int    `json:"session_key"`
	DriverNumber  int    `json:"driver_number"`
	BroadcastName string `json:"broadcast_name"`
	FullName      string `json:"full_name"`
	NameAcronym   string `json:"name_acronym"`
	TeamName      string `json:"team_name"`
	TeamColour    string `json:"team_colour"`
	FirstName     string `json:"first_name"`
	LastName      string `json:"last_name"`
	HeadshotURL   string `json:"headshot_url"`
	CountryCode   string `json:"country_code"`
	CountryName   string `json:"country_name"`
}

// Exported fields: encoding/json silently skips unexported ones, which would
// leave the cache empty both when writing and when reading it back.
type driverNumberCache struct {
	LastUpdated time.Time          `json:"last_updated"`
	Drivers     []driverNumberType `json:"drivers"`
}

func getDriverName(driverNumber int) (driverNumberType, error) {
	data, err := os.ReadFile("data/drivers.json")

	if err == nil {
		var cache driverNumberCache
		if json.Unmarshal(data, &cache) == nil &&
			!cache.LastUpdated.IsZero() &&
			time.Since(cache.LastUpdated) < 480*time.Hour && len(cache.Drivers) > 0 {
			for _, d := range cache.Drivers {
				if d.DriverNumber == driverNumber {
					return d, nil
				}
			}
			// A fresh cache is authoritative: the driver isn't in this
			// race's driver list, so say so now instead of refetching
			// the same list from the API only to miss again.
			return driverNumberType{}, fmt.Errorf("%w: #%d", errDriverNotFound, driverNumber)
		}
	}

	races, err := getRaces()
	if err != nil {
		return driverNumberType{}, fmt.Errorf("loading races: %w", err)
	}

	session_key := getSessionKey(races, "prev")
	if session_key == 0 {
		return driverNumberType{}, fmt.Errorf("no completed race found (session_key=0)")
	}

	resp, err := httpClient.Get(
		fmt.Sprintf(
			"https://api.openf1.org/v1/drivers?session_key=%d",
			session_key,
		),
	)
	if err != nil {
		return driverNumberType{}, fmt.Errorf("couldn't get driver names: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return driverNumberType{}, fmt.Errorf("OpenF1 returned status: %s", resp.Status)
	}

	var driverNumbers []driverNumberType
	if err := json.NewDecoder(resp.Body).Decode(&driverNumbers); err != nil {
		return driverNumberType{}, fmt.Errorf("decoding driver names: %w", err)
	}

	if err := os.MkdirAll("data", 0755); err != nil {
		return driverNumberType{}, err
	}

	var cache driverNumberCache
	cache = driverNumberCache{
		LastUpdated: time.Now().UTC(),
		Drivers:     driverNumbers,
	}

	cacheData, err := json.MarshalIndent(cache, "", "  ")
	if err != nil {
		return driverNumberType{}, err
	}
	if err := os.WriteFile("data/drivers.json", cacheData, 0644); err != nil {
		return driverNumberType{}, err
	}

	for _, d := range driverNumbers {
		if d.DriverNumber == driverNumber {
			return d, nil
		}
	}

	return driverNumberType{}, fmt.Errorf("%w: #%d", errDriverNotFound, driverNumber)
}

// driverCountry prefers the full country name when the API sends one and falls
// back to the code, since country_code is null for some drivers.
func driverCountry(d driverNumberType) string {
	if d.CountryName != "" {
		return d.CountryName
	}
	if d.CountryCode != "" {
		return d.CountryCode
	}
	return "Unknown"
}

func getDriverStandings() ([]driverStandings, error) {
	// so the thing here is it will take session id of last race conducted and give top 10
	data, err := os.ReadFile("data/standings.json")

	if err == nil {
		var cache driverStandingCache
		if json.Unmarshal(data, &cache) == nil &&
			!cache.UpdatedAt.IsZero() &&
			time.Since(cache.UpdatedAt) < 72*time.Hour {
			if len(cache.Standings) > 10 {
				return cache.Standings[:10], nil
			}
			return cache.Standings, nil
		}
	}

	races, err := getRaces()
	if err != nil {
		return nil, fmt.Errorf("loading races: %w", err)
	}

	session_key := getSessionKey(races, "prev")
	if session_key == 0 {
		return nil, fmt.Errorf("no completed race found (session_key=0)")
	}

	resp, err := httpClient.Get(
		fmt.Sprintf(
			"https://api.openf1.org/v1/championship_drivers?session_key=%d",
			session_key,
		),
	)
	if err != nil {
		return nil, fmt.Errorf("requesting driver standings: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("OpenF1 returned status: %s", resp.Status)
	}

	var standings []driverStandings
	if err := json.NewDecoder(resp.Body).Decode(&standings); err != nil {
		return nil, fmt.Errorf("decoding driver standings: %w", err)
	}

	if err := os.MkdirAll("data", 0755); err != nil {
		return nil, err
	}

	cache := driverStandingCache{
		UpdatedAt: time.Now().UTC(),
		Standings: standings,
	}

	data, err = json.MarshalIndent(cache, "", "  ")
	if err != nil {
		return nil, err
	}

	if err := os.WriteFile("data/standings.json", data, 0644); err != nil {
		return nil, err
	}

	// return top 10
	if len(standings) > 10 {
		standings = standings[:10]
	}

	return standings, nil

}

func getSessionFromKey(sessions []Session, key int) (Session, error) {
	// NOTE: data/race.json on disk is NOT a bare []Session. getRaces()
	// writes it wrapped as RaceCache{UpdatedAt, Races} ({"updated_at":...,"races":[...]}),
	// so json.Unmarshal(data, &[]Session) fails with:
	// "cannot unmarshal object into Go value of type []main.Session".
	// Reuse the slice getRaces() already returned instead of re-reading the file.
	for _, session := range sessions {
		if session.SessionKey == key {
			return session, nil
		}
	}

	return Session{}, fmt.Errorf("session with key %d not found", key)
}

func getLastRace() (string, error) {
	// so i think get session key should return the last conducted race right
	// but uhm
	// its sessions
	// so it may return fp1
	// i haven't made a way to iterate backwards i think
	// getsession can only get previous , not certain type
	// man
	// should i make it an optional param in session key getter
	// ah
	// man
	// ok lol , it does return only races
	// so i will have id
	// with that i can look inside cached json
	// or should i check with api
	// hmmm

	sessions, err := getRaces()
	if err != nil {
		log.Println("Error getting races:", err)
		return "", fmt.Errorf("you done fucked up %v", err)
	}

	sessionkey := getSessionKey(sessions, "prev")
	fmt.Println(sessionkey)
	session_we_want, err := getSessionFromKey(sessions, sessionkey)

	if err != nil {
		return "", fmt.Errorf("you done fucked up #2 %v", err)
	}

	// SessionName is always "Race" for these sessions, so it tells the
	// user nothing. Location/CircuitShortName is the useful part
	// (e.g. "Mexico City" for session_key 11404).
	race_name := session_we_want.Location
	if race_name == "" {
		race_name = session_we_want.CircuitShortName
	}
	if race_name == "" {
		race_name = session_we_want.SessionName
	}
	//if err != nil {
	//	return "error", fmt.Errorf("session we want is not session we get ,how sad")
	//}

	return race_name, nil
}

type sessionResult struct {
	MeetingKey   int     `json:"meeting_key"`
	SessionKey   int     `json:"session_key"`
	DriverNumber int     `json:"driver_number"`
	Position     int     `json:"position"`
	Points       float64 `json:"points"`
	DNF          bool    `json:"dnf"`
	DNS          bool    `json:"dns"`
	DSQ          bool    `json:"dsq"`
	GapToLeader  float64 `json:"gap_to_leader"`
	NumberOfLaps int     `json:"number_of_laps"`
	Duration     float64 `json:"duration"`
}

type lastStandingsCache struct {
	SessionKey int             `json:"session_key"`
	UpdatedAt  time.Time       `json:"updated_at"`
	Standings  []sessionResult `json:"standings"`
}

func getLastStandings() ([]driverStandings, error) {
	sessions, err := getRaces()
	if err != nil {
		return nil, fmt.Errorf("loading races: %w", err)
	}

	sessionKey := getSessionKey(sessions, "prev")
	if sessionKey == 0 {
		return nil, fmt.Errorf("no completed race found (session_key=0)")
	}

	// Results of a finished race don't change (unless a DSQ lands), so
	// serve from cache whenever it belongs to this same session
	if data, err := os.ReadFile("data/last-standings.json"); err == nil {
		var cache lastStandingsCache
		if json.Unmarshal(data, &cache) == nil && cache.SessionKey == sessionKey && len(cache.Standings) > 0 {
			return mapSessionResults(cache.Standings), nil
		}
	}

	// https://api.openf1.org/v1/session_result?session_key={sessionkey}&position%3C=10
	resp, err := httpClient.Get(
		fmt.Sprintf(
			"https://api.openf1.org/v1/session_result?session_key=%d&position%%3C=10",
			sessionKey,
		),
	)
	if err != nil {
		return nil, fmt.Errorf("requesting last race standings: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("OpenF1 returned status: %s", resp.Status)
	}

	var results []sessionResult
	if err := json.NewDecoder(resp.Body).Decode(&results); err != nil {
		return nil, fmt.Errorf("decoding last race standings: %w", err)
	}
	if len(results) == 0 {
		return nil, fmt.Errorf("no results found for session_key=%d", sessionKey)
	}

	sort.Slice(results, func(i, j int) bool { return results[i].Position < results[j].Position })

	if err := os.MkdirAll("data", 0755); err != nil {
		return nil, err
	}

	cache := lastStandingsCache{
		SessionKey: sessionKey,
		UpdatedAt:  time.Now().UTC(),
		Standings:  results,
	}

	data, err := json.MarshalIndent(cache, "", "  ")
	if err != nil {
		return nil, err
	}
	if err := os.WriteFile("data/last-standings.json", data, 0644); err != nil {
		return nil, err
	}

	return mapSessionResults(results), nil
}

func mapSessionResults(results []sessionResult) []driverStandings {
	standings := make([]driverStandings, 0, len(results))
	for _, r := range results {
		standings = append(standings, driverStandings{
			MeetingKey:      r.MeetingKey,
			SessionKey:      r.SessionKey,
			DriverNumber:    r.DriverNumber,
			PositionCurrent: r.Position,
			PointsCurrent:   r.Points,
		})
	}
	return standings
}

type allSession struct {
	SessionKey       int       `json:"session_key"`
	SessionType      string    `json:"session_type"`
	SessionName      string    `json:"session_name"`
	DateStart        time.Time `json:"date_start"`
	DateEnd          time.Time `json:"date_end"`
	MeetingKey       int       `json:"meeting_key"`
	CircuitKey       int       `json:"circuit_key"`
	CircuitShortName string    `json:"circuit_short_name"`
	CountryKey       int       `json:"country_key"`
	CountryCode      string    `json:"country_code"`
	CountryName      string    `json:"country_name"`
	Location         string    `json:"location"`
	GMTOffset        string    `json:"gmt_offset"` // should this have been time.Time too ?
	Year             int       `json:"year"`
	IsCancelled      bool      `json:"is_cancelled"`
}

type allSessionCache struct {
	UpdatedAt   time.Time    `json:"updated_at"`
	AllSessions []allSession `json:"all_sessions"`
}

func findNextSession(sessions []allSession) *allSession {
	now := time.Now().UTC()
	var next *allSession

	for i := range sessions {
		session := &sessions[i]

		if session.IsCancelled {
			continue
		}

		if session.DateEnd.Before(now) {
			continue
		}

		if next == nil || session.DateStart.Before(next.DateStart) {
			next = session
		}
	}

	return next
}

func getNextSession() (*allSession, error) {
	// i am also thinking , cant i get the last modified data by some os params anyways ?
	// that would meke json parsing for me easier , not having to store the time
	// will think
	// made a constant year at top
	data, err := os.ReadFile("data/sessions.json")

	if err == nil {
		var cache allSessionCache

		if json.Unmarshal(data, &cache) == nil &&
			!cache.UpdatedAt.IsZero() &&
			time.Since(cache.UpdatedAt) < 120*time.Hour {

			next := findNextSession(cache.AllSessions)

			return next, nil

		}
	}

	url := fmt.Sprintf(
		"https://api.openf1.org/v1/sessions?year=%d",
		year,
	)

	resp, err := httpClient.Get(url)

	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("OpenF1 returned status: %s", resp.Status)
	}

	var all_sessions []allSession
	if err := json.NewDecoder(resp.Body).Decode(&all_sessions); err != nil {
		return nil, fmt.Errorf("decoding driver names: %w", err)
	}

	if err := os.MkdirAll("data", 0755); err != nil {
		return nil, err
	}

	var cache allSessionCache
	cache = allSessionCache{
		UpdatedAt:   time.Now().UTC(),
		AllSessions: all_sessions,
	}

	cacheData, err := json.MarshalIndent(cache, "", "  ")
	if err != nil {
		return nil, err
	}

	if err := os.WriteFile("data/sessions.json", cacheData, 0644); err != nil {
		return nil, err
	}

	next := findNextSession(all_sessions)

	return next, nil

}

func initDB() {
	var err error
	// DB lives in data/, not the workdir: in Docker the process runs as
	// USER app, which can only write /app/data (root owns /app), and
	// compose persists /app/data via the franz-data volume — so this
	// both starts in the container and keeps subscriptions across rebuilds.
	if err := os.MkdirAll("data", 0755); err != nil {
		log.Fatal("Failed to create data dir for database:", err)
	}
	// i find it very hard to get docs for mttn sqlite3 for some reason
	DB, err = sql.Open("sqlite3", "./data/f1bot.db")
	if err != nil {
		log.Fatal(err)
	}
	query := `
	CREATE TABLE IF NOT EXISTS subscriptions (
		server_id TEXT PRIMARY KEY,
		channel_id TEXT NOT NULL,
		invoker_user_id TEXT NOT NULL,
		last_session_id TEXT,
		sent BOOLEAN DEFAULT FALSE,
		last_updated DATETIME DEFAULT CURRENT_TIMESTAMP
	);
	`
	_, err = DB.Exec(query)
	if err != nil {
		log.Fatal("Faled to create subscription table:", err)
	}
	log.Println("database initzlised sccuesfuly")
}

func addSubscription(serverID, channelID, userID string) error {
	query := `
	INSERT INTO subscriptions (
		server_id,
		channel_id,
		invoker_user_id
	)
	VALUES (?,?,?)
	ON CONFLICT (server_id)
	DO UPDATE SET
	channel_id = excluded.channel_id,
	invoker_user_id = excluded.invoker_user_id;
	`
	_, err := DB.Exec(query, serverID, channelID, userID)
	return err
}

type Subscription struct {
	ServerID      string
	ChannelID     string
	InvokerUserID string
	LastSessionID string
	Sent          bool
}

func getSubscriptions() ([]Subscription, error) {
	// COALESCE: fresh rows have NULL last_session_id, and scanning NULL
	// into a string/bool fails, which would break every notification check.
	rows, err := DB.Query(`
	SELECT
	server_id,
	channel_id,
	invoker_user_id,
	COALESCE(last_session_id, ''),
	COALESCE(sent, 0)
	FROM subscriptions
	`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var subscriptions []Subscription
	for rows.Next() {
		var sub Subscription
		err := rows.Scan(
			&sub.ServerID,
			&sub.ChannelID,
			&sub.InvokerUserID,
			&sub.LastSessionID,
			&sub.Sent,
		)
		if err != nil {
			return nil, err
		}
		subscriptions = append(subscriptions, sub)
		// getting a hang of this language
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return subscriptions, nil
}

func removeSubscription(serverID string) error {
	_, err := DB.Exec(
		"DELETE FROM subscriptions WHERE server_id = ?",
		serverID,
	)

	return err
}

func shouldNotify(session Session) bool {
	if session.IsCancelled {
		return false
	}

	if session.SessionName != "Race" &&
		session.SessionName != "Sprint" {
		return false
	}

	now := time.Now()
	notificationTime := session.DateStart.Add(-30 * time.Minute)

	return !now.Before(notificationTime) &&
		now.Before(session.DateStart)
}

func findNextRaceOrSprint(sessions []Session) *Session {
	now := time.Now()

	var next *Session

	for i := range sessions {
		s := &sessions[i]

		if s.IsCancelled {
			continue
		}

		if s.SessionName != "Race" && s.SessionName != "Sprint" {
			continue
		}

		if !s.DateStart.After(now) {
			continue
		}

		if next == nil || s.DateStart.Before(next.DateStart) {
			next = s
		}
	}

	return next
}

func shouldSendNotification(sub Subscription, session Session) bool {
	if sub.LastSessionID != fmt.Sprint(session.SessionKey) {
		return true
	}

	return !sub.Sent
}

func updateNotification(server_id string, session_id string) error {
	query := `
    UPDATE subscriptions
    SET last_session_id = ?,
        sent = TRUE,
        last_updated = CURRENT_TIMESTAMP
    WHERE server_id = ?
    `

	_, err := DB.Exec(query, session_id, server_id)
	return err
}

func createRaceEmbed(session Session) Embed {
	// Fall back so the embed never shows a blank field when OpenF1
	// omits one of these for a session — an empty field value makes
	// Fluxer reject the whole embed and the message is never sent.
	orUnknown := func(s string) string {
		if s == "" {
			return "—"
		}
		return s
	}
	place := session.Location
	if place == "" {
		place = session.CircuitShortName
	}
	timestamp := session.DateStart.Unix()
	return Embed{
		Title: fmt.Sprintf("🏎️ %s Starting Soon!", orUnknown(session.SessionName)),
		URL:   siteURL,
		Description: fmt.Sprintf(
			"**%s Grand Prix**\n📍 %s\nStarts: <t:%d:F>\nCountdown: <t:%d:R>",
			orUnknown(session.CountryName),
			orUnknown(place),
			timestamp,
			timestamp,
		),
		Color: 0xE10600,
		Fields: []EmbedField{
			{
				Name:   "Session",
				Value:  orUnknown(session.SessionName),
				Inline: true,
			},
			{
				Name:   "Circuit",
				Value:  orUnknown(session.CircuitShortName),
				Inline: true,
			},
			{
				Name:   "Starts At",
				Value:  session.DateStart.Format("02 Jan 2006, 15:04 UTC"),
				Inline: false,
			},
		},
		Footer: &EmbedFooter{
			Text: "franz-hermann",
		},
	}
}

func checkNotifications(bot *gofluxer.Bot, sessions []Session) {
	session := findNextRaceOrSprint(sessions)

	if session == nil {
		return
	}

	if !shouldNotify(*session) {
		return
	}

	subscriptions, err := getSubscriptions()
	if err != nil {
		log.Printf("Failed to get subscriptions: %v", err)
		return
	}

	embed := createRaceEmbed(*session)

	for _, sub := range subscriptions {
		if !shouldSendNotification(sub, *session) {
			continue
		}

		SendEmbed(bot, sub.ChannelID, embed)

		err := updateNotification(
			sub.ServerID,
			fmt.Sprint(session.SessionKey),
		)

		if err != nil {
			log.Printf("Failed to update notification: %v", err)
		}
	}
}

func main() {
	godotenv.Load()
	initDB()

	bot := gofluxer.NewBot(os.Getenv("BOT_TOKEN"), PREFIX)

	bot.OnReady(func() {
		updateStatus := func() {
			status := getStatus()

			log.Println("Status:", status)

			err := bot.Conn.WriteJSON(map[string]any{
				"op": 3,
				"d": map[string]any{
					"status": "online",
					"afk":    false,
					"custom_status": map[string]any{
						"text":     status,
						"emoji_id": 1567114760195936256,
					},
				},
			})

			if err != nil {
				log.Println("Presence error:", err)
			}
		}

		updateStatus()

		go func() {
			ticker := time.NewTicker(5 * time.Minute)
			defer ticker.Stop()

			for range ticker.C {
				updateStatus()
			}
		}()
	})

	bot.AddCommand("ping", asyncHandler(bot, func(m *gofluxer.Message, args []string) {
		// Fluxer: real API round-trip, measured fresh every ping.
		fluxerLatency, fluxerErr := measureLatency(bot.BaseURL+"/users/@me", bot.Token)
		fluxerVal := fmtMs(fluxerLatency)
		if fluxerErr != nil {
			fluxerVal += " (error: " + fluxerErr.Error() + ")"
		}

		// OpenF1: probed at most once per 2 hours, cached in between.
		ofLatency, ofErr, ofCheckedAt, ofFresh := getOpenF1Ping()
		ofVal := fmtMs(ofLatency)
		if ofErr != nil {
			ofVal += " (error: " + ofErr.Error() + ")"
		}
		if ofFresh {
			ofVal += " (fresh)"
		} else {
			ofVal += fmt.Sprintf(" (cached %s ago — rechecked every 2h)", time.Since(ofCheckedAt).Round(time.Minute))
		}

		procPct, sysPct, cpuOK := sampleCPU()
		cpuVal := "n/a"
		if cpuOK {
			cpuVal = fmt.Sprintf("Process %.1f%% · System %.1f%% (%d cores)", procPct, sysPct, runtime.NumCPU())
			if load, ok := loadAvg(); ok {
				cpuVal += " · Load " + load
			}
		}

		var mem runtime.MemStats
		runtime.ReadMemStats(&mem)
		memVal := fmt.Sprintf("Heap %.1f MB", float64(mem.HeapAlloc)/1024/1024)
		if rss, ok := rssMB(); ok {
			memVal += fmt.Sprintf(" · RSS %.1f MB", rss)
		}
		if usedPct, totalMB, availMB, ok := systemMemUsage(); ok {
			memVal += fmt.Sprintf(" · System %.0f%% used (%.0f/%.0f MB free)", usedPct, availMB, totalMB)
		}

		SendEmbed(bot, m.ChannelID, Embed{
			Title:       "Pong!",
			URL:         siteURL,
			Description: "Live latency and resource usage",
			Color:       0x5865F2,
			Fields: []EmbedField{
				{Name: "Fluxer API", Value: fluxerVal, Inline: true},
				{Name: "OpenF1 API", Value: ofVal, Inline: true},
				{Name: "CPU", Value: cpuVal, Inline: false},
				{Name: "Memory", Value: memVal, Inline: false},
				{Name: "Uptime", Value: fmt.Sprintf("%s · %d goroutines", time.Since(startTime).Round(time.Second), runtime.NumGoroutine()), Inline: false},
			},
			Footer: &EmbedFooter{Text: "franz-hermann bot"},
		})
	}))

	bot.AddCommand("help", asyncHandler(bot, func(m *gofluxer.Message, args []string) {
		title := "Help command"
		if len(args) > 0 {
			title = strings.Join(args, " ")
		}
		SendEmbed(bot, m.ChannelID, Embed{
			Title:       title,
			URL:         siteURL,
			Description: "Commands for the franz-hermann bot. All commands start with " + PREFIX,
			Color:       0x5865F2,
			Fields: []EmbedField{
				{Name: "ping", Value: "Live latency to Fluxer + OpenF1 (OpenF1 probed at most once per 2h), plus CPU/memory usage", Inline: false},
				{Name: "ds [title]", Value: "Top 10 driver standings from the latest race session", Inline: false},
				{Name: "who <driver number>", Value: "Driver name, team and country by car number (e.g. `>who 1`)", Inline: false},
				{Name: "last [title]", Value: "Top 10 results from the last race", Inline: false},
				{Name: "next [title]", Value: "Next session with location, circuit, country and countdown", Inline: false},
				{Name: "follow", Value: "Subscribe this channel to race/sprint reminders 30 minutes before start", Inline: false},
				{Name: "unfollow", Value: "Unsubscribe this server from race/sprint reminders", Inline: false},
				{Name: "help [title]", Value: "Shows this help message", Inline: false},
			},
			Footer: &EmbedFooter{Text: "franz-hermann bot"},
		})
	}))

	//bot.AddCommand("ds",func(m *gofluxer.))
	//i doubt the existence of embeds
	bot.AddCommand("ds", asyncHandler(bot, func(m *gofluxer.Message, args []string) {
		title := "Top 10 Driver Leaderboard"
		if len(args) > 0 {
			title = strings.Join(args, " ")
		}

		standings, err := getDriverStandings()
		if err != nil {
			bot.SendMessage(m.ChannelID, "Failed to fetch driver standings: "+err.Error())
			return
		}
		if len(standings) == 0 {
			bot.SendMessage(m.ChannelID, "No driver standings available right now.")
			return
		}

		fields := make([]EmbedField, 0, len(standings))
		for _, d := range standings {
			name := fmt.Sprintf("#%d", d.DriverNumber)
			if driver, err := getDriverName(d.DriverNumber); err == nil && driver.NameAcronym != "" {
				name = fmt.Sprintf("#%d %s", d.DriverNumber, driver.NameAcronym)
			}

			fields = append(fields, EmbedField{
				Name:   name,
				Value:  fmt.Sprintf("%.1f pts (P%d)", d.PointsCurrent, d.PositionCurrent),
				Inline: true,
			})
		}

		SendEmbed(bot, m.ChannelID, Embed{
			Title:       title,
			URL:         siteURL,
			Description: "Driver standings from latest race session",
			Color:       0x584F99,
			Fields:      fields,
		})
	}))

	bot.AddCommand("who", asyncHandler(bot, func(m *gofluxer.Message, args []string) {
		// to return driver name and embed info when number of driver is passed , session_key will be assumed to be current by default
		if len(args) == 0 {
			bot.SendMessage(m.ChannelID, "You didn't specify a driver number.")
			return
		}
		number, err := strconv.Atoi(args[0])
		if err != nil {
			bot.SendMessage(m.ChannelID, fmt.Sprintf("%q is not a driver number.", args[0]))
			return
		}

		driver, err := getDriverName(number)
		if err != nil {
			if errors.Is(err, errDriverNotFound) {
				bot.SendMessage(m.ChannelID, fmt.Sprintf("I don't know driver #%d — there's no such driver in the latest race.", number))
			} else {
				bot.SendMessage(m.ChannelID, "Error fetching driver: "+err.Error())
			}
			return
		}

		name := driver.FullName
		if name == "" {
			name = driver.BroadcastName
		}
		if name == "" {
			name = driver.NameAcronym
		}
		if name == "" {
			bot.SendMessage(m.ChannelID, fmt.Sprintf("I don't know driver #%d — there's no such driver in the latest race.", number))
			return
		}

		team := driver.TeamName
		if team == "" {
			team = "Unknown"
		}

		fields := make([]EmbedField, 0, 1)
		fields = append(fields, EmbedField{
			Name:   fmt.Sprintf("#%d %s", driver.DriverNumber, name),
			Value:  fmt.Sprintf("Team %s | Country %s", team, driverCountry(driver)),
			Inline: true,
		})

		// team_colour comes from OpenF1 as a bare hex string such as "FF8000".
		colInt, err := strconv.ParseInt(strings.TrimPrefix(driver.TeamColour, "#"), 16, 32)
		if err != nil {
			colInt = 0
		}
		embed := Embed{
			Title:  "Your driver is " + name,
			URL:    siteURL,
			Color:  int(colInt),
			Fields: fields,
		}
		// Only attach a thumbnail when OpenF1 actually gave us one.
		// A thumbnail with an empty URL (e.g. reserve driver #50, whose
		// headshot_url is null) makes the API reject the whole embed,
		// and gofluxer's SendEmbed swallows that error — the user just
		// sees silence.
		if driver.HeadshotURL != "" {
			embed.Thumbnail = &EmbedImage{URL: driver.HeadshotURL}
		}
		SendEmbed(bot, m.ChannelID, embed)

	}))

	bot.AddCommand("last", asyncHandler(bot, func(m *gofluxer.Message, args []string) {
		title := "last race results"
		if len(args) > 0 {
			title = strings.Join(args, " ")
		}
		last_race, err := getLastRace()
		if err != nil {
			// Was: fmt.Printf("... mate", err) with no %v -> prints %!(EXTRA *errors.errorString=...)
			// and then fell through to send an embed with an empty last_race.
			fmt.Printf("some issue with getting last race mate: %v\n", err)
			bot.SendMessage(m.ChannelID, "Failed to get last race: "+err.Error())
			return
		}
		fmt.Println(last_race)
		description := fmt.Sprintf(" Last race was : %s", last_race)

		last_standings, err := getLastStandings()
		if err != nil {
			bot.SendMessage(m.ChannelID, "Failed to fetch driver standings: "+err.Error())
			return
		}
		if len(last_standings) == 0 {
			bot.SendMessage(m.ChannelID, "No results available for the last race yet.")
			return
		}

		// Embed colour = the team colour of the race winner (P1).
		color := 0x5865F2
		if len(last_standings) > 0 {
			if winner, err := getDriverName(last_standings[0].DriverNumber); err == nil {
				if col, err := strconv.ParseInt(strings.TrimPrefix(winner.TeamColour, "#"), 16, 32); err == nil && col != 0 {
					color = int(col)
				}
			}
		}

		fields := make([]EmbedField, 0, len(last_standings))
		for _, d := range last_standings {
			name := fmt.Sprintf("#%d", d.DriverNumber)
			if driver, err := getDriverName(d.DriverNumber); err == nil && driver.NameAcronym != "" {
				name = fmt.Sprintf("#%d %s", d.DriverNumber, driver.NameAcronym)
			}

			fields = append(fields, EmbedField{
				Name:   name,
				Value:  fmt.Sprintf("%.1f pts (P%d)", d.PointsCurrent, d.PositionCurrent),
				Inline: true,
			})
		}

		SendEmbed(bot, m.ChannelID, Embed{
			Title:       title,
			URL:         siteURL,
			Description: description,
			Color:       color,
			Fields:      fields,
			Footer:      &EmbedFooter{Text: "franz-hermann bot"},
		})
	}))

	bot.AddCommand("next", asyncHandler(bot, func(m *gofluxer.Message, args []string) {
		title := "Next session"
		if len(args) > 0 {
			title = strings.Join(args, " ")
		}

		next, err := getNextSession()
		if err != nil {
			log.Printf("getNextSession: %v", err)
			return
		}

		if next == nil {
			SendEmbed(bot, m.ChannelID, Embed{
				Title:       title,
				Description: "No upcoming sessions found.",
				Color:       0x5865F2,
			})
			return
		}

		timestamp := next.DateStart.Unix()

		// next.Location / CircuitShortName / CountryName / SessionType are
		// already in the OpenF1 payload (see allSession) — the old embed
		// just never printed them, only SessionName + start time.
		sessionLabel := next.SessionName
		if next.SessionType != "" {
			sessionLabel = fmt.Sprintf("%s (%s)", next.SessionName, next.SessionType)
		}
		if sessionLabel == "" {
			sessionLabel = "Next session"
		}

		description := fmt.Sprintf(
			"**%s**\nStarts: <t:%d:F>\nCountdown: <t:%d:R>",
			sessionLabel,
			timestamp,
			timestamp,
		)

		// Fall back so the embed never shows a blank field when OpenF1
		// omits one of these for a session.
		orUnknown := func(s string) string {
			if s == "" {
				return "—"
			}
			return s
		}
		country := orUnknown(next.CountryName)
		if next.CountryCode != "" {
			country = fmt.Sprintf("%s (%s)", country, next.CountryCode)
		}

		fields := []EmbedField{
			{Name: "Location", Value: orUnknown(next.Location), Inline: true},
			{Name: "Circuit", Value: orUnknown(next.CircuitShortName), Inline: true},
			{Name: "Country", Value: country, Inline: true},
		}

		SendEmbed(bot, m.ChannelID, Embed{
			Title:       title,
			URL:         siteURL,
			Description: description,
			Color:       0x5865F2,
			Fields:      fields,
			Footer:      &EmbedFooter{Text: "franz-hermann bot"},
		})
	}))

	bot.AddCommand("follow", asyncHandler(bot, func(m *gofluxer.Message, args []string) {
		serverID := m.GuildID
		channelID := m.ChannelID
		invokerID := m.Author.ID

		err := addSubscription(serverID, channelID, invokerID)
		if err != nil {
			log.Printf("Failed to add subscription: %v", err)
			bot.SendMessage(channelID, "Something went wrong while subscribing!")
			return
		}

		msg := fmt.Sprintf(
			"This channel has been added to subscriptions by <@%s>",
			invokerID,
		)

		bot.SendMessage(channelID, msg)
	}))

	bot.AddCommand("unfollow", asyncHandler(bot, func(m *gofluxer.Message, args []string) {
		server_id := m.GuildID

		err := removeSubscription(server_id)
		if err != nil {
			log.Printf("%v", err)
			bot.SendMessage(m.ChannelID, "Something went wrong while unsubscribing!")
			return
		}

		msg := fmt.Sprintf("This server has been removed from subscriptions by <@%v>", m.Author.ID)
		bot.SendMessage(m.ChannelID, msg)
	}))

	// Race reminder sender: check on startup and then every 5 minutes.
	// getRaces() is cached (72h, or 35m while a race is underway), so
	// this is mostly a DB read plus a time comparison.
	go func() {
		check := func() {
			// A panic here must not kill the bot (unlike command handlers,
			// this goroutine has no asyncHandler recover) — log it and
			// let the next tick try again.
			defer func() {
				if r := recover(); r != nil {
					log.Printf("panic in race reminder check: %v", r)
				}
			}()
			sessions, err := getRaces()
			if err != nil {
				log.Printf("Race reminder check failed: %v", err)
				return
			}
			checkNotifications(bot, sessions)
		}
		check()
		ticker := time.NewTicker(5 * time.Minute)
		defer ticker.Stop()
		for range ticker.C {
			check()
		}
	}()

	bot.Run()
}
