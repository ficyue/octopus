package update

import (
	"fmt"

	"github.com/bestruirui/octopus/internal/utils/log"
)

type LatestInfo struct {
	TagName     string `json:"tag_name"`
	PublishedAt string `json:"published_at"`
	Body        string `json:"body"`
	Message     string `json:"message"`
}

func GetLatestInfo() (*LatestInfo, error) {
	return nil, fmt.Errorf("auto-update is disabled in this fork")
}

func UpdateCore() error {
	return fmt.Errorf("auto-update is disabled in this fork")
}
