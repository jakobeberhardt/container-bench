package config

import (
	"crypto/md5"
	"encoding/hex"
	"encoding/json"
	"sort"
)

type traceChecksumEntry struct {
	Key      string `json:"key"`
	Index    int    `json:"index"`
	Image    string `json:"image"`
	Command  string `json:"command,omitempty"`
	NumCores int    `json:"num_cores"`
	StartS   int    `json:"start_s"`
	StopS    int    `json:"stop_s"`
}

type traceChecksumPayload struct {
	Containers []traceChecksumEntry `json:"containers"`
}

// TraceChecksum returns a short, stable checksum that identifies the effective trace
// (the concrete container schedule that was executed), independent of scheduler choice.
//
// It computes MD5 over a canonical JSON representation and returns the first 6 hex
// characters (equivalent to `md5sum | cut -c1-6`).
func TraceChecksum(cfg *BenchmarkConfig) (string, error) {
	if cfg == nil {
		return "", nil
	}

	entries := make([]traceChecksumEntry, 0, len(cfg.Containers))
	for key, c := range cfg.Containers {
		entries = append(entries, traceChecksumEntry{
			Key:      key,
			Index:    c.Index,
			Image:    c.Image,
			Command:  c.Command,
			NumCores: c.GetRequestedNumCores(),
			StartS:   c.GetStartSeconds(),
			StopS:    c.GetStopSeconds(cfg.Benchmark.MaxT),
		})
	}

	sort.Slice(entries, func(i, j int) bool {
		if entries[i].Index != entries[j].Index {
			return entries[i].Index < entries[j].Index
		}
		return entries[i].Key < entries[j].Key
	})

	payload := traceChecksumPayload{Containers: entries}
	b, err := json.Marshal(payload)
	if err != nil {
		return "", err
	}

	sum := md5.Sum(b)
	hexStr := hex.EncodeToString(sum[:])
	if len(hexStr) > 6 {
		hexStr = hexStr[:6]
	}
	return hexStr, nil
}
