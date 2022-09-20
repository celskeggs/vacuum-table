package api

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
)

type Config struct {
	BearerToken string `json:"token"`
}

type Clerk struct {
	Config
	App    string
	Client *http.Client
}

func NewClerk(app string, config Config, client *http.Client) *Clerk {
	return &Clerk{
		App:    app,
		Config: config,
		Client: client,
	}
}

func IsAirTableId(name string) bool {
	if len(name) != 17 {
		return false
	}
	for _, c := range []byte(name) {
		if !('0' <= c && c <= '9') && !('a' <= c && c <= 'z') && !('A' <= c && c <= 'Z') {
			return false
		}
	}
	return true
}

type ListRecordsReply struct {
	Records []Record `json:"records"`
	Offset  string   `json:"offset"`
}

type Record struct {
	Id          string                 `json:"id"`
	CreatedTime string                 `json:"createdTime"`
	Fields      map[string]interface{} `json:"fields"`
}

func (c *Clerk) ListRecordsPage(table, offset string) (*ListRecordsReply, error) {
	if !strings.HasPrefix(c.BearerToken, "key") || !IsAirTableId(c.BearerToken) {
		return nil, fmt.Errorf("invalid API key")
	}
	if !IsAirTableId(c.App) {
		return nil, fmt.Errorf("not a valid app ID: %q", c.App)
	}
	if !IsAirTableId(table) {
		return nil, fmt.Errorf("not a valid table ID: %q", table)
	}
	var suffix string
	if offset != "" {
		suffix = "?offset=" + offset
	}
	req, err := http.NewRequest(http.MethodGet, "https://api.airtable.com/v0/"+c.App+"/"+table+suffix, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Add("Authorization", "Bearer "+c.BearerToken)
	response, err := c.Client.Do(req)
	if err != nil {
		return nil, err
	}
	defer func() {
		_ = response.Body.Close()
	}()
	if response.StatusCode != 200 {
		return nil, fmt.Errorf("status code was not 200, but rather %d %q", response.StatusCode, response.Status)
	}
	var result ListRecordsReply
	decoder := json.NewDecoder(response.Body)
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&result); err != nil {
		return nil, err
	}
	return &result, nil
}

func (c *Clerk) ListRecordsAll(table string) ([]Record, error) {
	var records []Record
	var offset string
	for {
		reply, err := c.ListRecordsPage(table, offset)
		if err != nil {
			return nil, err
		}
		records = append(records, reply.Records...)
		if reply.Offset == "" {
			return records, nil
		}
		offset = reply.Offset
	}
}
