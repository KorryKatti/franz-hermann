package main

import (
	"encoding/json"
	"fmt"
	"github.com/go-fluxer/gofluxer"
	"github.com/joho/godotenv"
	"log"
	"net/http"
	"os"
	"strings"
	"sync/atomic"
	"time"
)

type Session struct {
	SessionKey       int    `json:"session_key"`
	SessionType      string `json:"session_type"`
	SessionName      string `json:"session_name"`
	DateStart        string `json:"date_start"`
	DateEnd          string `json:"date_end"`
	MeetingKey       int    `json:"meeting_key"`
	CircuitKey       int    `json:"circuit_key"`
	CircuitShortName string `json:"circuit_short_name"`
	CountryKey       int    `json:"country_key"`
	CountryCode      string `json:"country_code"`
	CountryName      string `json:"country_name"`
	Location         string `json:"location"`
	GMTOffset        string `json:"gmt_offset"`
	Year             int    `json:"year"`
	IsCancelled      bool   `json:"is_cancelled"`
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

	var raceDay *Session
	var raceDayStart time.Time

	var next *Session
	var nextDate time.Time

	for i := range races {
		start, err := time.Parse(time.RFC3339, races[i].DateStart)
		if err != nil {
			continue
		}

		remaining := start.Sub(now)

		// Inside the race-day window: from 1h before the start up to 12h after.
		if remaining <= raceDayLead && remaining > -raceDayGrace {
			if raceDay == nil || start.Before(raceDayStart) {
				raceDay = &races[i]
				raceDayStart = start
			}
			continue
		}

		// Already over (past the window) or started and grace expired.
		if remaining <= 0 {
			continue
		}

		if next == nil || start.Before(nextDate) {
			next = &races[i]
			nextDate = start
		}
	}

	raceUnderway.Store(raceDay != nil)

	if raceDay != nil {
		return fmt.Sprintf("🏁 RACE DAY at %s", raceDay.Location)
	}

	if next == nil {
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

func getSessionKey(data []byte, which string) string {
	// possible thigs in which is prev and curr , if prev return prev session id and if curr return today's session id and if not found default to prev

	return ""
}

func getDriverStandings() {
	// so the thing here is it will take session id of last race conducted and give top 10
	data, err := os.ReadFile("data/race.json")

	if err == nil {
		var cache RaceCache
		if json.Unmarshal(data, &cache) == nil &&
			!cache.UpdatedAt.IsZero() &&
			time.Since(cache.UpdatedAt) < 72*time.Hour {
			// return cache.Races, nil
			log.Printf("Issue in driver cache have a look , occured at ", time.Now())
		}
	}

	session_key := getSessionKey(data, "prev") // todo
	resp, err := http.Get(
		fmt.Sprintf(
			"https://api.openf1.org/v1/championship_drivers?session_key=%s",
			session_key,
		),
	)
	if err != nil {
		log.Printf("Error getting driver standings at %v: %v", time.Now(), err)
	}
	defer resp.Body.Close()
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

	bot.Run()
}
