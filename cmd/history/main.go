package main

import (
	"encoding/hex"
	"encoding/json"
	"fmt"
	"log"
	"log/slog"
	"os"
	"strings"

	"github.com/Arkiv-Network/pebble-bitmap-store/pebblestore"
	"github.com/ethereum/go-ethereum/common/hexutil"
	"github.com/urfave/cli/v2"
)

type historyEntry struct {
	ID                uint64            `json:"id"`
	FromBlock         uint64            `json:"fromBlock"`
	ToBlock           *uint64           `json:"toBlock"`
	Active            bool              `json:"active"`
	ContentType       string            `json:"contentType"`
	Payload           hexutil.Bytes     `json:"payload"`
	StringAttributes  map[string]string `json:"stringAttributes,omitempty"`
	NumericAttributes map[string]uint64 `json:"numericAttributes,omitempty"`
}

func main() {
	logger := slog.New(slog.NewTextHandler(os.Stderr, nil))

	cfg := struct {
		dbPath string
	}{}

	app := &cli.App{
		Name:      "history",
		Usage:     "Show the version history of an entity by key",
		ArgsUsage: "<entity-key-hex>",
		Flags: []cli.Flag{
			&cli.PathFlag{
				Name:        "db-path",
				Value:       "arkiv-data.db",
				Destination: &cfg.dbPath,
				EnvVars:     []string{"DB_PATH"},
			},
		},
		Action: func(c *cli.Context) error {
			keyHex := c.Args().First()
			if keyHex == "" {
				return fmt.Errorf("entity key is required (hex string)")
			}

			keyHex = strings.TrimPrefix(keyHex, "0x")
			keyBytes, err := hex.DecodeString(keyHex)
			if err != nil {
				return fmt.Errorf("invalid hex key: %w", err)
			}
			if len(keyBytes) != 32 {
				return fmt.Errorf("entity key must be 32 bytes, got %d", len(keyBytes))
			}

			st, err := pebblestore.NewPebbleStore(logger, cfg.dbPath)
			if err != nil {
				return fmt.Errorf("failed to open database: %w", err)
			}
			defer st.Close()

			rows, err := st.GetPayloadHistoryByEntityKey(keyBytes)
			if err != nil {
				return fmt.Errorf("failed to get history: %w", err)
			}

			if len(rows) == 0 {
				fmt.Fprintln(os.Stderr, "no history found for this entity key")
				fmt.Println("[]")
				return nil
			}

			entries := make([]historyEntry, len(rows))
			for i, r := range rows {
				e := historyEntry{
					ID:          r.ID,
					FromBlock:   r.FromBlock,
					Active:      r.ToBlockIsNull,
					ContentType: r.ContentType,
					Payload:     r.Payload,
				}
				if !r.ToBlockIsNull {
					e.ToBlock = &r.ToBlock
				}
				if r.StringAttributes != nil {
					e.StringAttributes = r.StringAttributes.Values
				}
				if r.NumericAttributes != nil {
					e.NumericAttributes = r.NumericAttributes.Values
				}
				entries[i] = e
			}

			enc := json.NewEncoder(os.Stdout)
			enc.SetIndent("", "  ")
			return enc.Encode(entries)
		},
	}

	if err := app.Run(os.Args); err != nil {
		log.Fatal(err)
	}
}
