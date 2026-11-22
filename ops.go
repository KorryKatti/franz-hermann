package main

import (
	"log"
	"os"
	"sync"
	"time"

	"github.com/go-fluxer/gofluxer"
)

var (
	errorGuildID   string
	errorChannelID string

	errorReportMu sync.Mutex
	errorReportAt = map[string]time.Time{}
)

const errorReportInterval = 30 * time.Minute

func initErrorReporting() {
	errorGuildID = os.Getenv("ERROR_GUILD_ID")
	errorChannelID = os.Getenv("ERROR_CHANNEL_ID")
	if errorChannelID != "" {
		log.Printf("error reports: guild=%s channel=%s", errorGuildID, errorChannelID)
	}
}

// reportError logs every failure and mirrors it to ERROR_CHANNEL_ID, at most
// once per context per 30 minutes so a broken dependency cannot spam the channel.
func reportError(bot *gofluxer.Bot, context string, err error, meta ...string) {
	log.Printf("%s: %v", context, err)
	if bot == nil || errorChannelID == "" {
		return
	}

	errorReportMu.Lock()
	last, seen := errorReportAt[context]
	errorReportMu.Unlock()
	if seen && time.Since(last) < errorReportInterval {
		return
	}

	fields := make([]EmbedField, 0, len(meta)/2+1)
	for i := 0; i+1 < len(meta); i += 2 {
		fields = append(fields, EmbedField{Name: meta[i], Value: meta[i+1], Inline: true})
	}
	fields = append(fields, EmbedField{Name: "Error", Value: truncateText(err.Error(), 900)})

	footer := "franz-hermann ops"
	if errorGuildID != "" {
		footer += " · guild " + errorGuildID
	}

	sendErr := SendEmbed(bot, errorChannelID, Embed{
		Title:       "⚠️ " + context,
		Description: "An error needs attention.",
		Color:       0xE10600,
		Fields:      fields,
		Footer:      &EmbedFooter{Text: footer},
	})
	if sendErr != nil {
		log.Printf("failed to report error to ops channel: %v", sendErr)
		return
	}

	errorReportMu.Lock()
	errorReportAt[context] = time.Now()
	errorReportMu.Unlock()
}

func truncateText(s string, n int) string {
	if len(s) <= n {
		return s
	}
	runes := []rune(s)
	if len(runes) <= n {
		return s
	}
	return string(runes[:n]) + "…"
}
