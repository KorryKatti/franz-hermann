package main

import (
	"encoding/json"

	"github.com/go-fluxer/gofluxer"
)

// Embed is a typed helper for building Discord-style embeds.
// Convert with ToMap() and pass to bot.SendEmbed / EditMessage.
type Embed struct {
	Title       string         `json:"title,omitempty"`
	Description string         `json:"description,omitempty"`
	URL         string         `json:"url,omitempty"`
	Color       int            `json:"color,omitempty"`
	Timestamp   string         `json:"timestamp,omitempty"`
	Footer      *EmbedFooter   `json:"footer,omitempty"`
	Image       *EmbedImage    `json:"image,omitempty"`
	Thumbnail   *EmbedImage    `json:"thumbnail,omitempty"`
	Author      *EmbedAuthor   `json:"author,omitempty"`
	Fields      []EmbedField   `json:"fields,omitempty"`
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

// SendEmbed sends a typed Embed.
func SendEmbed(bot *gofluxer.Bot, channelID string, e Embed) {
	bot.SendEmbed(channelID, e.ToMap())
}
