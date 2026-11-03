package main

import (
	_ "fmt"
	"github.com/go-fluxer/gofluxer"
	"github.com/joho/godotenv"
	"log"
	"os"
	"strings"
)

const PREFIX = ">"

func main() {
	err := godotenv.Load()
	if err != nil {
		log.Fatal("Error loading .env file")
	}

	BOT_TOKEN := os.Getenv("BOT_TOKEN")
	bot := gofluxer.NewBot(BOT_TOKEN, PREFIX)

	bot.OnReady(func() {
		// gofluxer's SetPresence maps its argument straight onto d.status, which
		// must be one of online/idle/dnd/invisible/offline. Custom status text
		// lives in d.custom_status instead, which the library does not model,
		// so we send the op:3 payload ourselves via the exported Conn.
		err := bot.Conn.WriteJSON(map[string]any{
			"op": 3,
			"d": map[string]any{
				"status": "online",
				"afk":    false,
				"custom_status": map[string]any{
					"text":       " Hello",
					"emoji_name": "🏎️",
				},
			},
		})
		if err != nil {
			log.Println("Failed to set presence:", err)
		}
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
