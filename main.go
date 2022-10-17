package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"path"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/celskeggs/vacuum-table/api"
	"github.com/hashicorp/go-multierror"
)

const AttachmentLinkPrefix = "https://dl.airtable.com/.attachments/"

type Config struct {
	api.Config
	Tables map[string][]string `json:"app-tables"`
}

func loadConfig(path string) (Config, error) {
	var config Config
	f, err := os.Open(path)
	if err != nil {
		return Config{}, err
	}
	decoder := json.NewDecoder(f)
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&config); err != nil {
		return Config{}, err
	}
	return config, nil
}

type Backup struct {
	Tables      map[string][]api.Record `json:"tables"`
	Attachments []Attachment            `json:"attachments"`
}

func (b *Backup) Save(outputPath string) error {
	output, err := os.Create(outputPath)
	if err != nil {
		return err
	}
	encoder := json.NewEncoder(output)
	encoder.SetIndent("", "  ")
	if err := encoder.Encode(b); err != nil {
		return multierror.Append(err, output.Close(), os.Remove(outputPath))
	}
	if err := output.Close(); err != nil {
		return multierror.Append(err, os.Remove(outputPath))
	}
	return nil
}

func ExtractAllTables(config Config, client *http.Client) (map[string][]api.Record, error) {
	var wg sync.WaitGroup
	errChan := make(chan error, len(config.Tables))
	outputMap := map[string][]api.Record{}
	for app, tables := range config.Tables {
		wg.Add(1)
		go func(app string, tables []string) {
			clerk := api.NewClerk(app, config.Config, client)
			for _, table := range tables {
				startTime := time.Now()
				records, err := clerk.ListRecordsAll(table)
				if err != nil {
					errChan <- err
					break
				} else {
					_, _ = fmt.Fprintf(
						os.Stderr, "App %s -> Table %s: Listed %d records in %.3f seconds.\n",
						app, table, len(records), time.Since(startTime).Seconds(),
					)
					outputMap[table] = records
				}
			}
			wg.Done()
		}(app, tables)
	}
	wg.Wait()
	close(errChan)
	var allErrors error
	for err := range errChan {
		allErrors = multierror.Append(allErrors, err)
	}
	if allErrors != nil {
		return nil, allErrors
	}
	return outputMap, nil
}

type Attachment struct {
	Link string `json:"link"`
	Size int64  `json:"size"`
}

func ExtractAttachments(tables map[string][]api.Record) (attachments []Attachment) {
	for _, table := range tables {
		for _, record := range table {
			for _, value := range record.Fields {
				if contents, ok := value.([]interface{}); ok {
					for _, item := range contents {
						if itemMap, okMap := item.(map[string]interface{}); okMap {
							if url, found := itemMap["url"]; found {
								urlStr := url.(string)
								if !strings.HasPrefix(urlStr, AttachmentLinkPrefix) {
									panic("unexpected string prefix when scanning for attachment links")
								}
								size := itemMap["size"].(float64)
								if size != float64(int64(size)) {
									panic("invalid size")
								}
								attachments = append(attachments, Attachment{
									Link: urlStr,
									Size: int64(size),
								})
							}
						}
					}
				}
			}
		}
	}
	return attachments
}

func AttachmentFilename(link string) string {
	if !strings.HasPrefix(link, AttachmentLinkPrefix) {
		panic("invalid prefix for attachment link")
	}
	link = link[len(AttachmentLinkPrefix):]
	link = strings.ReplaceAll(link, "_", "__")
	link = strings.ReplaceAll(link, "/", "_")
	return link
}

func DownloadAttachment(attachment Attachment, outputDir, outputFilename string, client *http.Client) (errOut error) {
	resp, err := client.Get(attachment.Link)
	if err != nil {
		return err
	}
	defer func() {
		if err := resp.Body.Close(); err != nil {
			errOut = multierror.Append(errOut, err)
		}
	}()
	tempPath := path.Join(outputDir, "TEMP."+outputFilename)
	outputPath := path.Join(outputDir, outputFilename)
	output, err := os.Create(tempPath)
	if err != nil {
		return err
	}
	needsClose, needsRemove := true, true
	defer func() {
		if needsClose {
			if err := output.Close(); err != nil {
				errOut = multierror.Append(errOut, err)
			}
		}
		if needsRemove {
			if err := os.Remove(tempPath); err != nil {
				errOut = multierror.Append(errOut, err)
			}
		}
	}()
	if size, err := io.Copy(output, resp.Body); err != nil {
		return err
	} else if size != attachment.Size {
		return fmt.Errorf("mismatch on download for %q: received %d bytes but expected attachment to have %d",
			attachment.Link, size, attachment.Size)
	}
	needsClose = false
	if err := output.Close(); err != nil {
		return err
	}
	if err := os.Rename(tempPath, outputPath); err != nil {
		return err
	}
	needsRemove = false
	return nil
}

func DownloadAttachments(attachments []Attachment, downloadDir string, client *http.Client) error {
	if fi, err := os.Stat(downloadDir); err != nil {
		return err
	} else if !fi.IsDir() {
		return errors.New("download directory is not a directory")
	}
	sort.Slice(attachments, func(i, j int) bool {
		return attachments[i].Link < attachments[j].Link
	})
	for i, attachment := range attachments {
		downloadFilename := AttachmentFilename(attachment.Link)
		fi, err := os.Stat(path.Join(downloadDir, downloadFilename))
		if err != nil && os.IsNotExist(err) {
			if err := DownloadAttachment(attachment, downloadDir, downloadFilename, client); err != nil {
				return err
			}
			_, _ = fmt.Fprintf(
				os.Stderr, "%d/%d: Downloaded %q to %q (%d bytes)\n",
				i+1, len(attachments), attachment.Link, downloadFilename, attachment.Size,
			)
		} else if fi.Size() != attachment.Size {
			return fmt.Errorf("invalid size for already-downloaded attachment %q: %d instead of %d",
				attachment.Link, fi.Size(), attachment.Size)
		}
	}
	return nil
}

func Main(configPath, outputPath, downloadPath string) error {
	var client http.Client
	config, err := loadConfig(configPath)
	if err != nil {
		return err
	}
	tables, err := ExtractAllTables(config, &client)
	if err != nil {
		return err
	}
	backup := Backup{
		Tables:      tables,
		Attachments: ExtractAttachments(tables),
	}
	if err := backup.Save(outputPath); err != nil {
		return err
	}
	return DownloadAttachments(backup.Attachments, downloadPath, &client)
}

func main() {
	if len(os.Args) != 4 {
		_, _ = fmt.Fprintf(os.Stderr, "Usage: %s <config.json> <output.json> <dl.dir>\n", os.Args[0])
		os.Exit(1)
	}
	err := Main(os.Args[1], os.Args[2], os.Args[3])
	if err != nil {
		_, _ = fmt.Fprintf(os.Stderr, "Error: %s\n", err.Error())
		os.Exit(1)
	}
}
