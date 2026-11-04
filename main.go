package main

import (
	"encoding/json"
	"fmt"
	"github.com/go-fluxer/gofluxer"
	"github.com/joho/godotenv"
	"log"
	"net/http"
	"os"
	"strconv"
	"strings"
	"sync/atomic"
	"time"
)

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

const (
	PREFIX = ">"

	// Race-day banner: shown from 1 hour before lights out until 12 hours
	// after the start (race + post-race).
	raceDayLead  = time.Hour
	raceDayGrace = 12 * time.Hour
)

// raceUnderway is true while we are inside the race-day window — i.e. from
// 1h before lights out until 12h after the start. Updated by getStatus on
// every presence refresh, read by anything that needs to know whether a race
// is on. atomic because the ready handler and the refresh ticker both call
// getStatus from different goroutines.
var raceUnderway atomic.Bool

func getRaces() ([]Session, error) {
	data, err := os.ReadFile("data/race.json")

	if err == nil {
		var cache RaceCache
		if json.Unmarshal(data, &cache) == nil &&
			!cache.UpdatedAt.IsZero() &&
			time.Since(cache.UpdatedAt) < 72*time.Hour {
			return cache.Races, nil
		}
	}

	resp, err := http.Get("https://api.openf1.org/v1/sessions?year=2026&session_type=Race")
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	var races []Session
	if err := json.NewDecoder(resp.Body).Decode(&races); err != nil {
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
		start := races[i].DateStart

		remaining := start.Sub(now)

		// Inside the race-day window: from 1h before the start up to 12h after.
		if remaining <= raceDayLead && remaining > -raceDayGrace {
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

	if !foundNext {
		return "🏁 No upcoming races"
	}

	remaining := nextDate.Sub(now)

	if remaining < 24*time.Hour {
		hours := int(remaining.Hours())
		unit := "hours"
		if hours == 1 {
			unit = "hour"
		}
		return fmt.Sprintf(" %s | %d %s until race", next.Location, hours, unit)
	}

	days := int(remaining / (24 * time.Hour))
	unit := "days"
	if days == 1 {
		unit = "day"
	}
	return fmt.Sprintf(" %s | %d %s until race", next.Location, days, unit)
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

	resp, err := http.Get(
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

	return driverNumberType{}, fmt.Errorf("driver %d not found", driverNumber)
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

	resp, err := http.Get(
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

func main() {
	godotenv.Load()

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

	bot.AddCommand("ping", func(m *gofluxer.Message, args []string) {
		bot.SendMessage(m.ChannelID, "Pong!")
	})

	bot.AddCommand("say", func(m *gofluxer.Message, args []string) {
		if len(args) == 0 {
			bot.SendMessage(m.ChannelID, "What do you want me to say?")
			return
		}
		bot.SendMessage(m.ChannelID, strings.Join(args, " "))
	})

	bot.AddCommand("embed", func(m *gofluxer.Message, args []string) {
		title := "Embed demo"
		if len(args) > 0 {
			title = strings.Join(args, " ")
		}
		SendEmbed(bot, m.ChannelID, Embed{
			Title:       title,
			Description: "Sent with the typed Embed helper.",
			Color:       0x5865F2,
			Fields: []EmbedField{
				{Name: "User", Value: m.Author.Username, Inline: true},
				{Name: "Channel", Value: m.ChannelID, Inline: true},
			},
			Footer: &EmbedFooter{Text: "franz-hermann bot"},
		})
	})

	//bot.AddCommand("ds",func(m *gofluxer.))
	//i doubt the existence of embeds
	bot.AddCommand("ds", func(m *gofluxer.Message, args []string) {
		title := "Top 10 Driver Leaderboard"
		if len(args) > 0 {
			title = strings.Join(args, " ")
		}

		standings, err := getDriverStandings()
		if err != nil {
			bot.SendMessage(m.ChannelID, "Failed to fetch driver standings: "+err.Error())
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
			Description: "Driver standings from latest race session",
			Color:       0x584F99,
			Fields:      fields,
		})
	})

	bot.AddCommand("who", func(m *gofluxer.Message, args []string) {
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
			bot.SendMessage(m.ChannelID, "Error fetching driver: "+err.Error())
			return
		}

		name := driver.FullName
		if name == "" {
			name = driver.BroadcastName
		}
		if name == "" {
			name = driver.NameAcronym
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
		SendEmbed(bot, m.ChannelID, Embed{
			Title:     "Your driver is " + name,
			Color:     int(colInt),
			Thumbnail: &EmbedImage{URL: driver.HeadshotURL},
			Fields:    fields,
		})

	})

	bot.Run()
}
