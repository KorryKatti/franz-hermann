package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"

	"github.com/go-fluxer/gofluxer"
)

// Embed is a typed helper for building Discord-style embeds.
// Convert with ToMap() and pass to bot.SendEmbed / EditMessage.
type Embed struct {
	Title       string       `json:"title,omitempty"`
	Description string       `json:"description,omitempty"`
	URL         string       `json:"url,omitempty"`
	Color       int          `json:"color,omitempty"`
	Timestamp   string       `json:"timestamp,omitempty"`
	Footer      *EmbedFooter `json:"footer,omitempty"`
	Image       *EmbedImage  `json:"image,omitempty"`
	Thumbnail   *EmbedImage  `json:"thumbnail,omitempty"`
	Author      *EmbedAuthor `json:"author,omitempty"`
	Fields      []EmbedField `json:"fields,omitempty"`
}

type EmbedFooter struct {
	Text    string `json:"text"`
	IconURL string `json:"icon_url,omitempty"`
}

type EmbedImage struct {
	URL string `json:"url"`
}

type EmbedAuthor struct {
	Name    string `json:"name"`
	URL     string `json:"url,omitempty"`
	IconURL string `json:"icon_url,omitempty"`
}

type EmbedField struct {
	Name   string `json:"name"`
	Value  string `json:"value"`
	Inline bool   `json:"inline,omitempty"`
}

func (e Embed) ToMap() map[string]interface{} {
	b, _ := json.Marshal(e)
	var m map[string]interface{}
	json.Unmarshal(b, &m)
	return m
}

// SendEmbed posts a typed Embed and returns an error if Fluxer rejects it.
// gofluxer's helper returns nothing, so delivery failures were invisible.
func SendEmbed(bot *gofluxer.Bot, channelID string, e Embed) error {
	// Drop empty image/thumbnail URLs — "thumbnail":{"url":""} makes
	// the API reject the whole embed (this is what silenced `who 50`).
	if e.Thumbnail != nil && e.Thumbnail.URL == "" {
		e.Thumbnail = nil
	}
	if e.Image != nil && e.Image.URL == "" {
		e.Image = nil
	}

	payload, err := json.Marshal(map[string]interface{}{
		"embeds": []interface{}{e.ToMap()},
	})
	if err != nil {
		return fmt.Errorf("marshal embed: %w", err)
	}

	url := fmt.Sprintf("%s/channels/%s/messages", bot.BaseURL, channelID)
	req, err := http.NewRequest(http.MethodPost, url, bytes.NewReader(payload))
	if err != nil {
		return fmt.Errorf("build embed request: %w", err)
	}
	req.Header.Set("Authorization", "Bot "+bot.Token)
	req.Header.Set("Content-Type", "application/json")

	resp, err := httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("send embed: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 512))
		detail := strings.TrimSpace(string(body))
		if detail == "" {
			return fmt.Errorf("send embed: fluxer returned %s", resp.Status)
		}
		return fmt.Errorf("send embed: fluxer returned %s: %s", resp.Status, detail)
	}
	return nil
}
