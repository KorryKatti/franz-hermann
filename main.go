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
	Round            int       `json:"round"`
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

var commandSem = make(chan struct{}, 16)

// asyncHandler runs a command off the gateway read loop. gofluxer
// dispatches commands synchronously, so a slow OpenF1 call inside a
// handler would block all other messages. Also turns a panic into a
// visible reply instead of a dead process / empty channel.
func asyncHandler(bot *gofluxer.Bot, fn func(m *gofluxer.Message, args []string)) func(m *gofluxer.Message, args []string) {
	return func(m *gofluxer.Message, args []string) {
		go func() {
			commandSem <- struct{}{}
			defer func() { <-commandSem }()
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

// pingTTL bounds Jolpica ping probes.
const pingTTL = 30 * time.Minute

// siteURL is the bot's page, linked from every embed's title.
const siteURL = "https://korrykatti.github.io/projects/franz-hermann/"

type pingCache struct {
	mu        sync.Mutex
	latency   time.Duration
	err       error
	checkedAt time.Time
}

var jolpiPing pingCache

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
	req.Header.Set("User-Agent", "FranzHermann/1.0")
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

// getJolpiPing returns the Jolpica latency.
func getJolpiPing() (latency time.Duration, err error, checkedAt time.Time, fresh bool) {
	jolpiPing.mu.Lock()
	defer jolpiPing.mu.Unlock()
	if !jolpiPing.checkedAt.IsZero() && time.Since(jolpiPing.checkedAt) < pingTTL {
		return jolpiPing.latency, jolpiPing.err, jolpiPing.checkedAt, false
	}
	latency, err = measureLatency("https://api.jolpi.ca/ergast/f1/current.json", "")
	jolpiPing.latency, jolpiPing.err, jolpiPing.checkedAt = latency, err, time.Now()
	return latency, err, jolpiPing.checkedAt, true
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
	if len(f) < 13 {
		return 0, fmt.Errorf("bad /proc/self/stat")
	}
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

var lastRaceFetchMu sync.Mutex
var lastRaceFetch time.Time
var lastStandingsFetch time.Time
var lastConstructorStandingsFetch time.Time
var lastDriversFetch time.Time
var lastNextFetch time.Time

var cacheWriteMu sync.Mutex

func writeCacheFile(path string, data []byte) error {
	cacheWriteMu.Lock()
	defer cacheWriteMu.Unlock()
	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, data, 0644); err != nil {
		return err
	}
	return os.Rename(tmp, path)
}

var statusIndex int
var rotatingStatuses []string
var lastStatusRotation time.Time

func jolpiGet(url string) (*http.Response, error) {
	req, err := http.NewRequest(http.MethodGet, url, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("User-Agent", "FranzHermann/1.0")
	return httpClient.Do(req)
}

type jolpiSessionTime struct {
	Date string `json:"date"`
	Time string `json:"time"`
}

type jolpiRace struct {
	Season  string `json:"season"`
	Round   string `json:"round"`
	Circuit struct {
		CircuitName string `json:"circuitName"`
		Location    struct {
			Locality string `json:"locality"`
			Country  string `json:"country"`
		} `json:"Location"`
	} `json:"Circuit"`
	Date             string            `json:"date"`
	Time             string            `json:"time"`
	FirstPractice    *jolpiSessionTime `json:"FirstPractice"`
	SecondPractice   *jolpiSessionTime `json:"SecondPractice"`
	ThirdPractice    *jolpiSessionTime `json:"ThirdPractice"`
	Qualifying       *jolpiSessionTime `json:"Qualifying"`
	Sprint           *jolpiSessionTime `json:"Sprint"`
	SprintQualifying *jolpiSessionTime `json:"SprintQualifying"`
}

type jolpiScheduleResp struct {
	MRData struct {
		RaceTable struct {
			Races []jolpiRace `json:"Races"`
		} `json:"RaceTable"`
	} `json:"MRData"`
}

func parseJolpiTime(date, tm string) time.Time {
	t, err := time.Parse(time.RFC3339, date+"T"+tm)
	if err != nil {
		return time.Time{}
	}
	return t
}

func jolpiSessionKey(round, code int) int { return year*100000 + round*100 + code }

type jolpiSessionSpec struct {
	name  string
	typ   string
	code  int
	start time.Time
	dur   time.Duration
}

func jolpiSpecs(r jolpiRace) []jolpiSessionSpec {
	var specs []jolpiSessionSpec
	add := func(name, typ string, code int, t *jolpiSessionTime, dur time.Duration) {
		if t == nil {
			return
		}
		start := parseJolpiTime(t.Date, t.Time)
		if start.IsZero() {
			return
		}
		specs = append(specs, jolpiSessionSpec{name, typ, code, start, dur})
	}
	add("Practice 1", "Practice", 1, r.FirstPractice, time.Hour)
	add("Practice 2", "Practice", 2, r.SecondPractice, time.Hour)
	add("Practice 3", "Practice", 3, r.ThirdPractice, time.Hour)
	add("Qualifying", "Qualifying", 4, r.Qualifying, time.Hour)
	add("Sprint Qualifying", "Qualifying", 5, r.SprintQualifying, 45*time.Minute)
	add("Sprint", "Race", 6, r.Sprint, time.Hour)
	add("Race", "Race", 7, &jolpiSessionTime{r.Date, r.Time}, 2*time.Hour)
	return specs
}

func fetchJolpiSchedule() ([]jolpiRace, error) {
	resp, err := jolpiGet("https://api.jolpi.ca/ergast/f1/" + strconv.Itoa(year) + ".json?limit=100")
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("Jolpica returned status: %s", resp.Status)
	}
	var out jolpiScheduleResp
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		return nil, err
	}
	if len(out.MRData.RaceTable.Races) == 0 {
		return nil, fmt.Errorf("Jolpica returned no races")
	}
	return out.MRData.RaceTable.Races, nil
}

func jolpiSessions(r jolpiRace) []Session {
	round, _ := strconv.Atoi(r.Round)
	var out []Session
	for _, s := range jolpiSpecs(r) {
		if s.name != "Race" && s.name != "Sprint" {
			continue
		}
		out = append(out, Session{
			SessionKey: jolpiSessionKey(round, s.code), SessionType: s.typ, SessionName: s.name,
			DateStart: s.start, DateEnd: s.start.Add(s.dur), CircuitShortName: r.Circuit.CircuitName,
			CountryName: r.Circuit.Location.Country, Location: r.Circuit.Location.Locality, Year: year, Round: round,
		})
	}
	return out
}

func jolpiAllSessions(r jolpiRace) []allSession {
	round, _ := strconv.Atoi(r.Round)
	var out []allSession
	for _, s := range jolpiSpecs(r) {
		out = append(out, allSession{
			SessionKey: jolpiSessionKey(round, s.code), SessionType: s.typ, SessionName: s.name,
			DateStart: s.start, DateEnd: s.start.Add(s.dur), CircuitShortName: r.Circuit.CircuitName,
			CountryName: r.Circuit.Location.Country, Location: r.Circuit.Location.Locality, Year: year, Round: round,
		})
	}
	return out
}

func getRaces() ([]Session, error) {
	raceTTL := 12 * time.Hour
	cooldown := 30 * time.Minute
	if raceUnderway.Load() {
		raceTTL = 30 * time.Minute
		cooldown = 15 * time.Minute
	}

	var stale []Session

	data, err := os.ReadFile("data/race.json")
	if err == nil {
		var cache RaceCache
		if json.Unmarshal(data, &cache) == nil && !cache.UpdatedAt.IsZero() {
			stale = cache.Races
			if time.Since(cache.UpdatedAt) < raceTTL {
				return cache.Races, nil
			}
		}
	}

	lastRaceFetchMu.Lock()
	if len(stale) > 0 && !lastRaceFetch.IsZero() && time.Since(lastRaceFetch) < cooldown {
		lastRaceFetchMu.Unlock()
		return stale, nil
	}
	lastRaceFetch = time.Now()
	lastRaceFetchMu.Unlock()

	if schedule, ferr := fetchJolpiSchedule(); ferr == nil {
		var races []Session
		for _, r := range schedule {
			races = append(races, jolpiSessions(r)...)
		}
		if len(races) > 0 {
			os.MkdirAll("data", 0755)
			cache := RaceCache{UpdatedAt: time.Now().UTC(), Races: races}
			data, _ = json.MarshalIndent(cache, "", "  ")
			if err := writeCacheFile("data/race.json", data); err != nil {
				return nil, err
			}
			return races, nil
		}
	} else if len(stale) > 0 {
		return stale, nil
	}

	if raceUnderway.Load() {
		return nil, fmt.Errorf("Jolpica schedule unavailable during live session")
	}

	url := fmt.Sprintf("https://api.openf1.org/v1/sessions?year=%d&session_type=Race", year)
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
	cache := RaceCache{UpdatedAt: time.Now().UTC(), Races: races}
	data, _ = json.MarshalIndent(cache, "", "  ")
	if err := writeCacheFile("data/race.json", data); err != nil {
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
	Round     int               `json:"round"`
	Standings []driverStandings `json:"standings"`
}

type constructorStanding struct {
	Position int     `json:"position"`
	Points   float64 `json:"points"`
	Name     string  `json:"name"`
}

type constructorStandingCache struct {
	UpdatedAt time.Time             `json:"updated_at"`
	Round     int                   `json:"round"`
	Standings []constructorStanding `json:"standings"`
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
	WikipediaURL  string `json:"wikipedia_url"`
	DateOfBirth   string `json:"date_of_birth"`
}

// Exported fields: encoding/json silently skips unexported ones, which would
// leave the cache empty both when writing and when reading it back.
type driverNumberCache struct {
	LastUpdated time.Time          `json:"last_updated"`
	Drivers     []driverNumberType `json:"drivers"`
}

type jolpiDriversResp struct {
	MRData struct {
		DriverTable struct {
			Drivers []struct {
				PermanentNumber string `json:"permanentNumber"`
				Code            string `json:"code"`
				GivenName       string `json:"givenName"`
				FamilyName      string `json:"familyName"`
				Nationality     string `json:"nationality"`
				URL             string `json:"url"`
				DateOfBirth     string `json:"dateOfBirth"`
			} `json:"Drivers"`
		} `json:"DriverTable"`
	} `json:"MRData"`
}

func fetchJolpiDrivers(round int, sessionKey int) ([]driverNumberType, error) {
	url := "https://api.jolpi.ca/ergast/f1/" + strconv.Itoa(year) + "/" + strconv.Itoa(round) + "/drivers.json?limit=100"
	resp, err := jolpiGet(url)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("Jolpica returned status: %s", resp.Status)
	}
	var out jolpiDriversResp
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		return nil, err
	}
	_, teams, _ := fetchJolpiStandings(round, sessionKey)
	var drivers []driverNumberType
	for _, d := range out.MRData.DriverTable.Drivers {
		num := atoi(d.PermanentNumber)
		if len(teams) > 0 {
			if _, ok := teams[num]; !ok {
				continue
			}
		}
		name := d.GivenName
		if d.FamilyName != "" {
			if name != "" {
				name = name + " " + d.FamilyName
			} else {
				name = d.FamilyName
			}
		}
		drivers = append(drivers, driverNumberType{SessionKey: sessionKey, DriverNumber: num, FullName: name, BroadcastName: name, NameAcronym: d.Code, TeamName: teams[num], FirstName: d.GivenName, LastName: d.FamilyName, CountryName: d.Nationality, WikipediaURL: d.URL, DateOfBirth: d.DateOfBirth})
	}
	if len(drivers) == 0 {
		return nil, fmt.Errorf("Jolpica returned no drivers")
	}
	return drivers, nil
}

func getDriverName(driverNumber int) (driverNumberType, error) {
	var stale []driverNumberType
	findStale := func() (driverNumberType, bool) {
		for _, d := range stale {
			if d.DriverNumber == driverNumber {
				return d, true
			}
		}
		return driverNumberType{}, false
	}

	data, err := os.ReadFile("data/drivers.json")

	if err == nil {
		var cache driverNumberCache
		if json.Unmarshal(data, &cache) == nil && len(cache.Drivers) > 0 {
			stale = cache.Drivers
			if !cache.LastUpdated.IsZero() && time.Since(cache.LastUpdated) < 72*time.Hour {
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
	}

	races, err := getRaces()
	if err != nil {
		if d, ok := findStale(); ok {
			return d, nil
		}
		return driverNumberType{}, fmt.Errorf("loading races: %w", err)
	}

	session_key := getSessionKey(races, "prev")
	if session_key == 0 {
		if d, ok := findStale(); ok {
			return d, nil
		}
		return driverNumberType{}, fmt.Errorf("no completed race found (session_key=0)")
	}
	prev, _ := getSessionFromKey(races, session_key)
	if len(stale) > 0 && !lastDriversFetch.IsZero() && time.Since(lastDriversFetch) < 30*time.Minute {
		if d, ok := findStale(); ok {
			return d, nil
		}
		return driverNumberType{}, fmt.Errorf("%w: #%d", errDriverNotFound, driverNumber)
	}
	lastDriversFetch = time.Now()
	if prev.Round > 0 {
		drivers, ferr := fetchJolpiDrivers(prev.Round, session_key)
		if ferr == nil && len(drivers) > 0 {
			os.MkdirAll("data", 0755)
			cache := driverNumberCache{LastUpdated: time.Now().UTC(), Drivers: drivers}
			cacheData, _ := json.MarshalIndent(cache, "", "  ")
			if err := writeCacheFile("data/drivers.json", cacheData); err != nil {
				return driverNumberType{}, err
			}
			for _, d := range drivers {
				if d.DriverNumber == driverNumber {
					return d, nil
				}
			}
			return driverNumberType{}, fmt.Errorf("%w: #%d", errDriverNotFound, driverNumber)
		}
		if d, ok := findStale(); ok {
			return d, nil
		}
		if raceUnderway.Load() {
			return driverNumberType{}, fmt.Errorf("Jolpica drivers unavailable: %v", ferr)
		}
	}

	resp, err := httpClient.Get(
		fmt.Sprintf(
			"https://api.openf1.org/v1/drivers?session_key=%d",
			session_key,
		),
	)
	if err != nil {
		if d, ok := findStale(); ok {
			return d, nil
		}
		return driverNumberType{}, fmt.Errorf("couldn't get driver names: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		if d, ok := findStale(); ok {
			return d, nil
		}
		return driverNumberType{}, fmt.Errorf("OpenF1 returned status: %s", resp.Status)
	}

	var driverNumbers []driverNumberType
	if err := json.NewDecoder(resp.Body).Decode(&driverNumbers); err != nil {
		if d, ok := findStale(); ok {
			return d, nil
		}
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
	if err := writeCacheFile("data/drivers.json", cacheData); err != nil {
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

func atoi(s string) int     { n, _ := strconv.Atoi(s); return n }
func atof(s string) float64 { n, _ := strconv.ParseFloat(s, 64); return n }

type jolpiStandingsResp struct {
	MRData struct {
		StandingsTable struct {
			StandingsLists []struct {
				DriverStandings []struct {
					Position string `json:"position"`
					Points   string `json:"points"`
					Driver   struct {
						PermanentNumber string `json:"permanentNumber"`
					} `json:"Driver"`
					Constructors []struct {
						Name string `json:"name"`
					} `json:"Constructors"`
				} `json:"DriverStandings"`
			} `json:"StandingsLists"`
		} `json:"StandingsTable"`
	} `json:"MRData"`
}

func fetchJolpiStandings(round int, sessionKey int) ([]driverStandings, map[int]string, error) {
	url := "https://api.jolpi.ca/ergast/f1/" + strconv.Itoa(year) + "/" + strconv.Itoa(round) + "/driverstandings.json?limit=100"
	resp, err := jolpiGet(url)
	if err != nil {
		return nil, nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, nil, fmt.Errorf("Jolpica returned status: %s", resp.Status)
	}
	var out jolpiStandingsResp
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		return nil, nil, err
	}
	var standings []driverStandings
	teams := make(map[int]string)
	for _, list := range out.MRData.StandingsTable.StandingsLists {
		for _, d := range list.DriverStandings {
			num := atoi(d.Driver.PermanentNumber)
			standings = append(standings, driverStandings{SessionKey: sessionKey, DriverNumber: num, PositionCurrent: atoi(d.Position), PointsCurrent: atof(d.Points)})
			if len(d.Constructors) > 0 {
				teams[num] = d.Constructors[0].Name
			}
		}
		if len(standings) > 0 {
			break
		}
	}
	if len(standings) == 0 {
		return nil, nil, fmt.Errorf("Jolpica returned no standings")
	}
	return standings, teams, nil
}

type jolpiConstructorStandingsResp struct {
	MRData struct {
		StandingsTable struct {
			StandingsLists []struct {
				ConstructorStandings []struct {
					Position    string `json:"position"`
					Points      string `json:"points"`
					Constructor struct {
						Name string `json:"name"`
					} `json:"Constructor"`
				} `json:"ConstructorStandings"`
			} `json:"StandingsLists"`
		} `json:"StandingsTable"`
	} `json:"MRData"`
}

func fetchJolpiConstructorStandings(round int) ([]constructorStanding, error) {
	url := "https://api.jolpi.ca/ergast/f1/" + strconv.Itoa(year) + "/" + strconv.Itoa(round) + "/constructorstandings.json?limit=100"
	resp, err := jolpiGet(url)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("Jolpica returned status: %s", resp.Status)
	}
	var out jolpiConstructorStandingsResp
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		return nil, err
	}
	var standings []constructorStanding
	for _, list := range out.MRData.StandingsTable.StandingsLists {
		for _, c := range list.ConstructorStandings {
			standings = append(standings, constructorStanding{Position: atoi(c.Position), Points: atof(c.Points), Name: c.Constructor.Name})
		}
		if len(standings) > 0 {
			break
		}
	}
	if len(standings) == 0 {
		return nil, fmt.Errorf("Jolpica returned no constructor standings")
	}
	return standings, nil
}

func getDriverStandings() ([]driverStandings, error) {
	standingsTTL := 6 * time.Hour
	cooldown := 30 * time.Minute
	if raceUnderway.Load() {
		standingsTTL = 30 * time.Minute
		cooldown = 15 * time.Minute
	}
	var stale []driverStandings
	var staleRound int
	var staleUpdated time.Time
	data, err := os.ReadFile("data/standings.json")
	if err == nil {
		var cache driverStandingCache
		if json.Unmarshal(data, &cache) == nil && len(cache.Standings) > 0 {
			stale = cache.Standings
			staleRound = cache.Round
			staleUpdated = cache.UpdatedAt
		}
	}
	staleTop10 := func() []driverStandings {
		if len(stale) > 10 {
			return stale[:10]
		}
		return stale
	}
	races, err := getRaces()
	if err != nil {
		if len(stale) > 0 {
			return staleTop10(), nil
		}
		return nil, fmt.Errorf("loading races: %w", err)
	}
	session_key := getSessionKey(races, "prev")
	if session_key == 0 {
		if len(stale) > 0 {
			return staleTop10(), nil
		}
		return nil, fmt.Errorf("no completed race found (session_key=0)")
	}
	prev, _ := getSessionFromKey(races, session_key)
	if len(stale) > 0 && staleRound == prev.Round && staleRound != 0 && !staleUpdated.IsZero() && time.Since(staleUpdated) < standingsTTL {
		return staleTop10(), nil
	}
	if len(stale) > 0 && !lastStandingsFetch.IsZero() && time.Since(lastStandingsFetch) < cooldown {
		return staleTop10(), nil
	}
	lastStandingsFetch = time.Now()
	if prev.Round > 0 {
		standings, _, ferr := fetchJolpiStandings(prev.Round, session_key)
		if ferr == nil && len(standings) > 0 {
			os.MkdirAll("data", 0755)
			cache := driverStandingCache{UpdatedAt: time.Now().UTC(), Round: prev.Round, Standings: standings}
			data, _ := json.MarshalIndent(cache, "", "  ")
			if err := writeCacheFile("data/standings.json", data); err != nil {
				return nil, err
			}
			if len(standings) > 10 {
				standings = standings[:10]
			}
			return standings, nil
		}
		if len(stale) > 0 {
			return staleTop10(), nil
		}
		if raceUnderway.Load() {
			return nil, fmt.Errorf("Jolpica standings unavailable during live session")
		}
	}
	resp, err := httpClient.Get(fmt.Sprintf("https://api.openf1.org/v1/championship_drivers?session_key=%d", session_key))
	if err != nil {
		if len(stale) > 0 {
			return staleTop10(), nil
		}
		return nil, fmt.Errorf("requesting driver standings: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		if len(stale) > 0 {
			return staleTop10(), nil
		}
		return nil, fmt.Errorf("OpenF1 returned status: %s", resp.Status)
	}
	var standings []driverStandings
	if err := json.NewDecoder(resp.Body).Decode(&standings); err != nil {
		if len(stale) > 0 {
			return staleTop10(), nil
		}
		return nil, fmt.Errorf("decoding driver standings: %w", err)
	}
	os.MkdirAll("data", 0755)
	cache := driverStandingCache{UpdatedAt: time.Now().UTC(), Standings: standings}
	data, err = json.MarshalIndent(cache, "", "  ")
	if err != nil {
		return nil, err
	}
	if err := writeCacheFile("data/standings.json", data); err != nil {
		return nil, err
	}
	if len(standings) > 10 {
		standings = standings[:10]
	}
	return standings, nil
}

func getConstructorStandings() ([]constructorStanding, error) {
	standingsTTL := 6 * time.Hour
	cooldown := 30 * time.Minute
	if raceUnderway.Load() {
		standingsTTL = 30 * time.Minute
		cooldown = 15 * time.Minute
	}
	var stale []constructorStanding
	var staleRound int
	var staleUpdated time.Time
	data, err := os.ReadFile("data/team_standings.json")
	if err == nil {
		var cache constructorStandingCache
		if json.Unmarshal(data, &cache) == nil && len(cache.Standings) > 0 {
			stale = cache.Standings
			staleRound = cache.Round
			staleUpdated = cache.UpdatedAt
		}
	}
	staleTop10 := func() []constructorStanding {
		if len(stale) > 10 {
			return stale[:10]
		}
		return stale
	}
	races, err := getRaces()
	if err != nil {
		if len(stale) > 0 {
			return staleTop10(), nil
		}
		return nil, fmt.Errorf("loading races: %w", err)
	}
	session_key := getSessionKey(races, "prev")
	if session_key == 0 {
		if len(stale) > 0 {
			return staleTop10(), nil
		}
		return nil, fmt.Errorf("no completed race found (session_key=0)")
	}
	prev, _ := getSessionFromKey(races, session_key)
	if len(stale) > 0 && staleRound == prev.Round && staleRound != 0 && !staleUpdated.IsZero() && time.Since(staleUpdated) < standingsTTL {
		return staleTop10(), nil
	}
	if len(stale) > 0 && !lastConstructorStandingsFetch.IsZero() && time.Since(lastConstructorStandingsFetch) < cooldown {
		return staleTop10(), nil
	}
	lastConstructorStandingsFetch = time.Now()
	if prev.Round > 0 {
		standings, ferr := fetchJolpiConstructorStandings(prev.Round)
		if ferr == nil && len(standings) > 0 {
			os.MkdirAll("data", 0755)
			cache := constructorStandingCache{UpdatedAt: time.Now().UTC(), Round: prev.Round, Standings: standings}
			data, _ := json.MarshalIndent(cache, "", "  ")
			if err := writeCacheFile("data/team_standings.json", data); err != nil {
				return nil, err
			}
			if len(standings) > 10 {
				standings = standings[:10]
			}
			return standings, nil
		}
		if len(stale) > 0 {
			return staleTop10(), nil
		}
		if raceUnderway.Load() {
			return nil, fmt.Errorf("Jolpica standings unavailable during live session")
		}
	}
	if len(stale) > 0 {
		return staleTop10(), nil
	}
	return nil, fmt.Errorf("no constructor standings available")
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

type jolpiResultsResp struct {
	MRData struct {
		RaceTable struct {
			Races []struct {
				Results []struct {
					Number   string `json:"number"`
					Position string `json:"position"`
					Points   string `json:"points"`
					Laps     string `json:"laps"`
					Status   string `json:"status"`
					Driver   struct {
						PermanentNumber string `json:"permanentNumber"`
					} `json:"Driver"`
				} `json:"Results"`
			} `json:"Races"`
		} `json:"RaceTable"`
	} `json:"MRData"`
}

type jolpiSprintResultsResp struct {
	MRData struct {
		RaceTable struct {
			Races []struct {
				SprintResults []struct {
					Number   string `json:"number"`
					Position string `json:"position"`
					Points   string `json:"points"`
					Laps     string `json:"laps"`
					Status   string `json:"status"`
					Driver   struct {
						PermanentNumber string `json:"permanentNumber"`
					} `json:"Driver"`
				} `json:"SprintResults"`
			} `json:"Races"`
		} `json:"RaceTable"`
	} `json:"MRData"`
}

func fetchJolpiSprintResults(round int, sessionKey int) ([]sessionResult, error) {
	url := "https://api.jolpi.ca/ergast/f1/" + strconv.Itoa(year) + "/" + strconv.Itoa(round) + "/sprint.json?limit=100"
	resp, err := jolpiGet(url)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("Jolpica returned status: %s", resp.Status)
	}
	var out jolpiSprintResultsResp
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		return nil, err
	}
	var results []sessionResult
	for _, race := range out.MRData.RaceTable.Races {
		for _, r := range race.SprintResults {
			num := atoi(r.Number)
			if num == 0 {
				num = atoi(r.Driver.PermanentNumber)
			}
			results = append(results, sessionResult{SessionKey: sessionKey, DriverNumber: num, Position: atoi(r.Position), Points: atof(r.Points), NumberOfLaps: atoi(r.Laps), DNF: r.Status == "Retired"})
		}
	}
	if len(results) == 0 {
		return nil, fmt.Errorf("Jolpica returned no results yet")
	}
	sort.Slice(results, func(i, j int) bool { return results[i].Position < results[j].Position })
	return results, nil
}

func fetchJolpiResults(round int, sessionKey int) ([]sessionResult, error) {
	url := "https://api.jolpi.ca/ergast/f1/" + strconv.Itoa(year) + "/" + strconv.Itoa(round) + "/results.json?limit=100"
	resp, err := jolpiGet(url)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("Jolpica returned status: %s", resp.Status)
	}
	var out jolpiResultsResp
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		return nil, err
	}
	var results []sessionResult
	for _, race := range out.MRData.RaceTable.Races {
		for _, r := range race.Results {
			num := atoi(r.Number)
			if num == 0 {
				num = atoi(r.Driver.PermanentNumber)
			}
			results = append(results, sessionResult{SessionKey: sessionKey, DriverNumber: num, Position: atoi(r.Position), Points: atof(r.Points), NumberOfLaps: atoi(r.Laps), DNF: r.Status == "Retired"})
		}
	}
	if len(results) == 0 {
		return nil, fmt.Errorf("Jolpica returned no results yet")
	}
	return results, nil
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

	prev, _ := getSessionFromKey(sessions, sessionKey)
	if prev.Round > 0 {
		results, ferr := fetchJolpiResults(prev.Round, sessionKey)
		if ferr == nil && len(results) > 0 {
			sort.Slice(results, func(i, j int) bool { return results[i].Position < results[j].Position })
			os.MkdirAll("data", 0755)
			cache := lastStandingsCache{SessionKey: sessionKey, UpdatedAt: time.Now().UTC(), Standings: results}
			data, _ := json.MarshalIndent(cache, "", "  ")
			if err := writeCacheFile("data/last-standings.json", data); err != nil {
				return nil, err
			}
			return mapSessionResults(results), nil
		}
		if raceUnderway.Load() {
			return nil, fmt.Errorf("Jolpica results unavailable: %v", ferr)
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
	if err := writeCacheFile("data/last-standings.json", data); err != nil {
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
	Round            int       `json:"round"`
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

func getAllSessions() ([]allSession, error) {
	nextTTL := 12 * time.Hour
	cooldown := 30 * time.Minute
	if raceUnderway.Load() {
		nextTTL = 30 * time.Minute
		cooldown = 15 * time.Minute
	}
	var stale []allSession
	data, err := os.ReadFile("data/sessions.json")
	if err == nil {
		var cache allSessionCache
		if json.Unmarshal(data, &cache) == nil && len(cache.AllSessions) > 0 {
			stale = cache.AllSessions
			if !cache.UpdatedAt.IsZero() && time.Since(cache.UpdatedAt) < nextTTL {
				return cache.AllSessions, nil
			}
		}
	}
	if len(stale) > 0 && !lastNextFetch.IsZero() && time.Since(lastNextFetch) < cooldown {
		return stale, nil
	}
	lastNextFetch = time.Now()
	if schedule, ferr := fetchJolpiSchedule(); ferr == nil {
		var all []allSession
		for _, r := range schedule {
			all = append(all, jolpiAllSessions(r)...)
		}
		if len(all) > 0 {
			os.MkdirAll("data", 0755)
			cache := allSessionCache{UpdatedAt: time.Now().UTC(), AllSessions: all}
			cacheData, _ := json.MarshalIndent(cache, "", "  ")
			if err := writeCacheFile("data/sessions.json", cacheData); err != nil {
				return nil, err
			}
			return all, nil
		}
	} else if len(stale) > 0 {
		return stale, nil
	}
	if raceUnderway.Load() {
		return nil, fmt.Errorf("Jolpica schedule unavailable during live session")
	}
	url := fmt.Sprintf("https://api.openf1.org/v1/sessions?year=%d", year)
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
	var all_sessions []allSession
	if err := json.NewDecoder(resp.Body).Decode(&all_sessions); err != nil {
		if len(stale) > 0 {
			return stale, nil
		}
		return nil, fmt.Errorf("decoding driver names: %w", err)
	}
	if err := os.MkdirAll("data", 0755); err != nil {
		return nil, err
	}
	cache := allSessionCache{UpdatedAt: time.Now().UTC(), AllSessions: all_sessions}
	cacheData, err := json.MarshalIndent(cache, "", "  ")
	if err != nil {
		return nil, err
	}
	if err := writeCacheFile("data/sessions.json", cacheData); err != nil {
		return nil, err
	}
	return all_sessions, nil
}

func getNextSession() (*allSession, error) {
	sessions, err := getAllSessions()
	if err != nil {
		return nil, err
	}
	return findNextSession(sessions), nil
}

func findTrackSession(sessions []allSession, arg string) *allSession {
	if arg == "" || strings.EqualFold(arg, "next") {
		return findNextSession(sessions)
	}
	round, err := strconv.Atoi(arg)
	if err != nil || round <= 0 {
		return nil
	}
	var race *allSession
	var first *allSession
	for i := range sessions {
		s := &sessions[i]
		if s.Round != round || s.IsCancelled {
			continue
		}
		if first == nil || s.DateStart.Before(first.DateStart) {
			first = s
		}
		if s.SessionType == "Race" || s.SessionName == "Race" {
			race = s
		}
	}
	if race != nil {
		return race
	}
	return first
}

func migrateSessionIDs() {
	if DB == nil {
		return
	}
	data, err := os.ReadFile("data/race.json")
	if err != nil {
		return
	}
	var cache RaceCache
	if json.Unmarshal(data, &cache) != nil || len(cache.Races) == 0 {
		return
	}
	for _, s := range cache.Races {
		if s.Round != 0 {
			return
		}
	}
	schedule, err := fetchJolpiSchedule()
	if err != nil {
		log.Printf("session id migration skipped: %v", err)
		return
	}
	type key struct {
		t    int64
		name string
	}
	fresh := make(map[key]int)
	for _, r := range schedule {
		for _, s := range jolpiSessions(r) {
			fresh[key{s.DateStart.Unix(), s.SessionName}] = s.SessionKey
		}
	}
	for _, old := range cache.Races {
		newKey, ok := fresh[key{old.DateStart.Unix(), old.SessionName}]
		if !ok || newKey == old.SessionKey {
			continue
		}
		_, err := DB.Exec("UPDATE subscriptions SET last_session_id = ? WHERE last_session_id = ?", fmt.Sprint(newKey), fmt.Sprint(old.SessionKey))
		if err != nil {
			log.Printf("session id migration update: %v", err)
		}
	}
	log.Println("session id migration done")
}

func initDB() error {
	var err error
	// DB lives in data/, not the workdir: in Docker the process runs as
	// USER app, which can only write /app/data (root owns /app), and
	// compose persists /app/data via the franz-data volume — so this
	// both starts in the container and keeps subscriptions across rebuilds.
	if err := os.MkdirAll("data", 0755); err != nil {
		return fmt.Errorf("failed to create data dir for database: %w", err)
	}
	// i find it very hard to get docs for mttn sqlite3 for some reason
	DB, err = sql.Open("sqlite3", "./data/f1bot.db")
	if err != nil {
		return err
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
		return fmt.Errorf("failed to create subscription table: %w", err)
	}
	for _, q := range []string{
		"ALTER TABLE subscriptions ADD COLUMN winner_last_session_id TEXT DEFAULT ''",
		"ALTER TABLE subscriptions ADD COLUMN winner_sent INTEGER DEFAULT 0",
	} {
		if _, err := DB.Exec(q); err != nil && !strings.Contains(err.Error(), "duplicate column name") {
			return fmt.Errorf("failed to migrate subscriptions table: %w", err)
		}
	}
	log.Println("database initzlised sccuesfuly")
	return nil
}

func addSubscription(serverID, channelID, userID string) error {
	if DB == nil {
		return errors.New("database unavailable")
	}
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
	ServerID            string
	ChannelID           string
	InvokerUserID       string
	LastSessionID       string
	Sent                bool
	WinnerLastSessionID string
	WinnerSent          bool
}

func getSubscriptions() ([]Subscription, error) {
	if DB == nil {
		return nil, errors.New("database unavailable")
	}
	rows, err := DB.Query(`
	SELECT
	server_id,
	channel_id,
	invoker_user_id,
	COALESCE(last_session_id, ''),
	COALESCE(sent, 0),
	COALESCE(winner_last_session_id, ''),
	COALESCE(winner_sent, 0)
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
			&sub.WinnerLastSessionID,
			&sub.WinnerSent,
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

func getSubscription(serverID string) (Subscription, bool, error) {
	if DB == nil {
		return Subscription{}, false, errors.New("database unavailable")
	}
	row := DB.QueryRow(`SELECT server_id, channel_id, invoker_user_id, COALESCE(last_session_id, ''), COALESCE(sent, 0), COALESCE(winner_last_session_id, ''), COALESCE(winner_sent, 0) FROM subscriptions WHERE server_id = ?`, serverID)
	var sub Subscription
	err := row.Scan(&sub.ServerID, &sub.ChannelID, &sub.InvokerUserID, &sub.LastSessionID, &sub.Sent, &sub.WinnerLastSessionID, &sub.WinnerSent)
	if err == sql.ErrNoRows {
		return Subscription{}, false, nil
	}
	if err != nil {
		return Subscription{}, false, err
	}
	return sub, true, nil
}

func removeSubscription(serverID string) error {
	if DB == nil {
		return errors.New("database unavailable")
	}
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
	if DB == nil {
		return errors.New("database unavailable")
	}
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

type driversGifData struct {
	Drivers   map[string]string `json:"drivers"`
	Fallbacks []string          `json:"fallbacks"`
}

func loadDriversGif() driversGifData {
	data, err := os.ReadFile("drivers_gif.json")
	if err != nil {
		return driversGifData{}
	}
	var out driversGifData
	if err := json.Unmarshal(data, &out); err != nil {
		return driversGifData{}
	}
	return out
}

func winnerGifURL(acronym string) string {
	g := loadDriversGif()
	if url, ok := g.Drivers[acronym]; ok && url != "" {
		return url
	}
	if len(g.Fallbacks) > 0 {
		return g.Fallbacks[0]
	}
	return ""
}

func fetchSessionResults(session Session) ([]sessionResult, error) {
	var results []sessionResult
	var err error
	if session.SessionName == "Sprint" {
		results, err = fetchJolpiSprintResults(session.Round, session.SessionKey)
	} else {
		results, err = fetchJolpiResults(session.Round, session.SessionKey)
		if err == nil {
			sort.Slice(results, func(i, j int) bool { return results[i].Position < results[j].Position })
		}
	}
	return results, err
}

func getTop3ForSession(session Session) ([]driverStandings, []string, error) {
	results, err := fetchSessionResults(session)
	if err != nil || len(results) == 0 {
		if err == nil {
			err = fmt.Errorf("no results")
		}
		return nil, nil, err
	}
	standings := mapSessionResults(results)
	if len(standings) > 3 {
		standings = standings[:3]
	}
	acronyms := make([]string, len(standings))
	for i, d := range standings {
		if dr, err := getDriverName(d.DriverNumber); err == nil {
			acronyms[i] = dr.NameAcronym
		}
	}
	return standings, acronyms, nil
}

func createWinnerEmbed(session Session, top3 []driverStandings, acronyms []string) Embed {
	label := session.SessionName
	if label == "" {
		label = "Race"
	}
	title := fmt.Sprintf("🏆 %s Results: %s", label, session.Location)
	if session.Location == "" {
		title = fmt.Sprintf("🏆 %s Results", label)
	}
	lines := []string{}
	for i, d := range top3 {
		name := fmt.Sprintf("#%d", d.DriverNumber)
		if dr, err := getDriverName(d.DriverNumber); err == nil {
			if dr.FullName != "" {
				name = dr.FullName
			} else if dr.NameAcronym != "" {
				name = dr.NameAcronym
			}
		}
		team := ""
		if dr, err := getDriverName(d.DriverNumber); err == nil && dr.TeamName != "" {
			team = " — " + dr.TeamName
		}
		lines = append(lines, fmt.Sprintf("**P%d** %s%s", i+1, name, team))
	}
	img := ""
	if len(acronyms) > 0 {
		img = winnerGifURL(acronyms[0])
	}
	embed := Embed{
		Title:       title,
		URL:         siteURL,
		Description: strings.Join(lines, "\n"),
		Color:       0xE10600,
		Footer:      &EmbedFooter{Text: "franz-hermann"},
	}
	if img != "" {
		embed.Image = &EmbedImage{URL: img}
	}
	return embed
}

func updateWinnerNotification(serverID, sessionID string, sent bool) error {
	if DB == nil {
		return errors.New("database unavailable")
	}
	_, err := DB.Exec(`
	UPDATE subscriptions
	SET winner_last_session_id = ?,
		winner_sent = ?,
		last_updated = CURRENT_TIMESTAMP
	WHERE server_id = ?
	`, sessionID, sent, serverID)
	return err
}

func findWinnerSession(sessions []Session) *Session {
	now := time.Now()
	var candidate *Session
	for i := range sessions {
		s := &sessions[i]
		if s.IsCancelled {
			continue
		}
		if s.SessionName != "Race" && s.SessionName != "Sprint" {
			continue
		}
		if now.Before(s.DateEnd.Add(30 * time.Minute)) {
			continue
		}
		if candidate == nil || s.DateEnd.After(candidate.DateEnd) {
			candidate = s
		}
	}
	return candidate
}

func checkWinnerNotifications(bot *gofluxer.Bot, sessions []Session) {
	session := findWinnerSession(sessions)
	if session == nil {
		return
	}
	sessionID := fmt.Sprint(session.SessionKey)
	now := time.Now()
	windowOpen := !now.Before(session.DateEnd.Add(30 * time.Minute))
	windowClosed := !now.Before(session.DateEnd.Add(90 * time.Minute))

	subs, err := getSubscriptions()
	if err != nil {
		log.Printf("Failed to get subscriptions for winner: %v", err)
		return
	}

	if windowClosed {
		for _, sub := range subs {
			if sub.WinnerLastSessionID != sessionID {
				_ = updateWinnerNotification(sub.ServerID, sessionID, false)
			}
		}
		return
	}
	if !windowOpen {
		return
	}

	needs := false
	for _, sub := range subs {
		if sub.WinnerLastSessionID != sessionID || !sub.WinnerSent {
			needs = true
			break
		}
	}
	if !needs {
		return
	}

	top3, acronyms, err := getTop3ForSession(*session)
	if err != nil {
		return
	}
	embed := createWinnerEmbed(*session, top3, acronyms)
	for _, sub := range subs {
		if sub.WinnerLastSessionID == sessionID && sub.WinnerSent {
			continue
		}
		if sub.WinnerLastSessionID != sessionID {
			if err := updateWinnerNotification(sub.ServerID, sessionID, false); err != nil {
				log.Printf("Failed to init winner notification: %v", err)
			}
		}
		SendEmbed(bot, sub.ChannelID, embed)
		if err := updateWinnerNotification(sub.ServerID, sessionID, true); err != nil {
			log.Printf("Failed to update winner notification: %v", err)
		}
	}
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
	if err := initDB(); err != nil {
		log.Printf("database unavailable, continuing without subscriptions: %v", err)
	}
	migrateSessionIDs()

	token := os.Getenv("BOT_TOKEN")
	if token == "" {
		log.Fatal("BOT_TOKEN is not set (check .env)")
	}
	bot := gofluxer.NewBot(token, PREFIX)

	bot.OnReady(func() {
		updateStatus := func() {
			defer func() {
				if r := recover(); r != nil {
					log.Printf("panic in status update: %v", r)
				}
			}()
			status := getStatus()

			log.Println("Status:", status)

			err := bot.SetStatus("online", &gofluxer.CustomStatus{
				Text:   status,
				EmojiID: "1567114760195936256",
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

		ofLatency, ofErr, ofCheckedAt, ofFresh := getJolpiPing()
		ofVal := fmtMs(ofLatency)
		if ofErr != nil {
			ofVal += " (error: " + ofErr.Error() + ")"
		}
		if ofFresh {
			ofVal += " (fresh)"
		} else {
			ofVal += fmt.Sprintf(" (cached %s ago — rechecked every 30m)", time.Since(ofCheckedAt).Round(time.Minute))
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
				{Name: "Jolpica API", Value: ofVal, Inline: true},
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
				{Name: "ping", Value: "Live latency to Fluxer + Jolpica (Jolpica probed at most once per 30m), plus CPU/memory usage", Inline: false},
				{Name: "ds [title]", Value: "Top 10 driver standings from the latest race session", Inline: false},
				{Name: "ts [title]", Value: "Top 10 constructor standings from the latest race session", Inline: false},
				{Name: "who <driver number>", Value: "Driver name, team and country by car number (e.g. `>who 1`)", Inline: false},
				{Name: "last [title]", Value: "Top 10 results from the last race", Inline: false},
				{Name: "next [title]", Value: "Next session with location, circuit, country and countdown", Inline: false},
				{Name: "track [round|next]", Value: "Circuit/location info for a round (defaults to next upcoming)", Inline: false},
				{Name: "follow", Value: "Subscribe this channel to race/sprint reminders 30 minutes before start", Inline: false},
				{Name: "unfollow", Value: "Unsubscribe this server from race/sprint reminders", Inline: false},
				{Name: "substatus", Value: "Tell whether this server is subscribed to race/sprint reminders, and in which channel", Inline: false},
				{Name: "help [title]", Value: "Shows this help message", Inline: false},
				{Name: "Invite", Value: "[Add to server](https://fluxer.com/oauth2/authorize?client_id=1566837767571247107&scope=bot&permissions=274878171200)", Inline: true},
				{Name: "Support", Value: "[Join server](https://fluxer.gg/s1evrala)", Inline: true},
				{Name: "Website", Value: "[korrykatti.github.io](https://korrykatti.github.io/projects/franz-hermann/)", Inline: true},
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

	bot.AddCommand("ts", asyncHandler(bot, func(m *gofluxer.Message, args []string) {
		title := "Top 10 Constructor Leaderboard"
		if len(args) > 0 {
			title = strings.Join(args, " ")
		}

		standings, err := getConstructorStandings()
		if err != nil {
			bot.SendMessage(m.ChannelID, "Failed to fetch team standings: "+err.Error())
			return
		}
		if len(standings) == 0 {
			bot.SendMessage(m.ChannelID, "No team standings available right now.")
			return
		}

		fields := make([]EmbedField, 0, len(standings))
		for _, t := range standings {
			fields = append(fields, EmbedField{
				Name:   t.Name,
				Value:  fmt.Sprintf("%.1f pts (P%d)", t.Points, t.Position),
				Inline: true,
			})
		}

		SendEmbed(bot, m.ChannelID, Embed{
			Title:       title,
			URL:         siteURL,
			Description: "Constructor standings from latest race session",
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

		fields := make([]EmbedField, 0, 3)
		fields = append(fields, EmbedField{
			Name:   fmt.Sprintf("#%d %s", driver.DriverNumber, name),
			Value:  fmt.Sprintf("Team %s | Country %s", team, driverCountry(driver)),
			Inline: true,
		})
		if driver.DateOfBirth != "" {
			born := driver.DateOfBirth
			if dob, derr := time.Parse("2006-01-02", driver.DateOfBirth); derr == nil {
				born = dob.Format("02 Jan 2006")
				age := time.Now().Year() - dob.Year()
				if time.Now().Before(time.Date(time.Now().Year(), dob.Month(), dob.Day(), 0, 0, 0, 0, time.UTC)) {
					age--
				}
				born = fmt.Sprintf("%s (%d years old)", born, age)
			}
			fields = append(fields, EmbedField{Name: "Date of birth", Value: born, Inline: true})
		}
		if driver.WikipediaURL != "" {
			fields = append(fields, EmbedField{Name: "Wikipedia", Value: fmt.Sprintf("[Read article](%s)", driver.WikipediaURL), Inline: false})
		}

		// team_colour comes from OpenF1 as a bare hex string such as "FF8000".
		colInt, err := strconv.ParseInt(strings.TrimPrefix(driver.TeamColour, "#"), 16, 32)
		if err != nil {
			colInt = 0
		}
		embedURL := siteURL
		if driver.WikipediaURL != "" {
			embedURL = driver.WikipediaURL
		}
		embed := Embed{
			Title:  "Your driver is " + name,
			URL:    embedURL,
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
			bot.SendMessage(m.ChannelID, "Failed to fetch next session: "+err.Error())
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

	bot.AddCommand("track", asyncHandler(bot, func(m *gofluxer.Message, args []string) {
		arg := ""
		if len(args) > 0 {
			arg = args[0]
		}

		sessions, err := getAllSessions()
		if err != nil {
			bot.SendMessage(m.ChannelID, "Failed to fetch track info: "+err.Error())
			return
		}

		track := findTrackSession(sessions, arg)
		if track == nil {
			if arg == "" || strings.EqualFold(arg, "next") {
				bot.SendMessage(m.ChannelID, "No upcoming track found.")
			} else {
				bot.SendMessage(m.ChannelID, "No track found for round "+arg+".")
			}
			return
		}

		circuit := track.CircuitShortName
		if circuit == "" {
			circuit = track.Location
		}
		if circuit == "" {
			circuit = "Unknown"
		}
		location := track.Location
		if location == "" {
			location = "Unknown"
		}
		country := track.CountryName
		if country == "" {
			country = track.CountryCode
		}
		if country == "" {
			country = "Unknown"
		}

		title := fmt.Sprintf("Round %d — %s", track.Round, circuit)
		if track.Round == 0 {
			title = circuit
		}

		fields := []EmbedField{
			{Name: "Circuit", Value: circuit, Inline: true},
			{Name: "Location", Value: location, Inline: true},
			{Name: "Country", Value: country, Inline: true},
		}
		if track.Round != 0 || track.Year != 0 {
			roundVal := fmt.Sprintf("%d", track.Round)
			if track.Year != 0 {
				roundVal = fmt.Sprintf("%d (%d)", track.Round, track.Year)
			}
			fields = append(fields, EmbedField{Name: "Round", Value: roundVal, Inline: true})
		}

		desc := fmt.Sprintf("%s, %s", location, country)
		if !track.DateStart.IsZero() {
			ts := track.DateStart.Unix()
			sessionLabel := track.SessionName
			if sessionLabel == "" {
				sessionLabel = track.SessionType
			}
			if sessionLabel == "" {
				sessionLabel = "Session"
			}
			when := fmt.Sprintf("<t:%d:F>\n<t:%d:R>", ts, ts)
			if track.DateEnd.Before(time.Now().UTC()) {
				when = fmt.Sprintf("<t:%d:F>", ts)
			}
			fields = append(fields, EmbedField{Name: "Session (" + sessionLabel + ")", Value: when, Inline: false})
			if track.DateEnd.After(time.Now().UTC()) {
				desc = fmt.Sprintf("%s, %s", location, country)
			}
		}

		SendEmbed(bot, m.ChannelID, Embed{
			Title:       title,
			URL:         siteURL,
			Description: desc,
			Color:       0x5865F2,
			Fields:      fields,
			Footer:      &EmbedFooter{Text: "franz-hermann bot"},
		})
	}))

	canManageFollow := func(m *gofluxer.Message) bool {
	if m.GuildID == "" {
		return false
	}
	if bot.IsOwner(m) {
		return true
	}
	return bot.HasPermission(m, gofluxer.PermissionManageChannels) ||
		bot.HasPermission(m, gofluxer.PermissionManageMessages) ||
		bot.HasPermission(m, gofluxer.PermissionManageEvents)
}

	bot.AddCommand("follow", asyncHandler(bot, func(m *gofluxer.Message, args []string) {
		if !canManageFollow(m) {
			bot.SendMessage(m.ChannelID, "You need Manage Channels, Manage Messages, Manage Events, or be the server owner to use this command.")
			return
		}
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
		if !canManageFollow(m) {
			bot.SendMessage(m.ChannelID, "You need Manage Channels, Manage Messages, Manage Events, or be the server owner to use this command.")
			return
		}
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

	subStatus := func(m *gofluxer.Message, args []string) {
		if m.GuildID == "" {
			bot.SendMessage(m.ChannelID, "Run this in a server, not in DMs.")
			return
		}
		sub, ok, err := getSubscription(m.GuildID)
		if err != nil {
			bot.SendMessage(m.ChannelID, "Database unavailable right now.")
			return
		}
		if !ok {
			bot.SendMessage(m.ChannelID, "This server is **not** subscribed. Run `>follow` in the channel that should get race reminders.")
			return
		}
		last := "none yet"
		if sub.LastSessionID != "" {
			last = sub.LastSessionID
			if races, rerr := getRaces(); rerr == nil {
				if key, kerr := strconv.Atoi(sub.LastSessionID); kerr == nil {
					if s, serr := getSessionFromKey(races, key); serr == nil {
						last = fmt.Sprintf("%s (%s, %s)", s.SessionName, s.Location, s.DateStart.Format("02 Jan 2006"))
					}
				}
			}
		}
		bot.SendMessage(m.ChannelID, fmt.Sprintf("This server **is** subscribed.\nChannel ID: `%s`\nSet by (user ID): `%s`\nLast reminder session: %s", sub.ChannelID, sub.InvokerUserID, last))
	}
	bot.AddCommand("substatus", asyncHandler(bot, subStatus))
	bot.AddCommand("subscription", asyncHandler(bot, subStatus))

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
