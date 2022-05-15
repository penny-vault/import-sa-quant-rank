package sa

import (
	"context"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"strings"
	"time"

	"github.com/adrg/strutil"
	"github.com/adrg/strutil/metrics"
	"github.com/spf13/viper"

	"github.com/jackc/pgx/v4"

	"github.com/rs/zerolog/log"
)

func LoadRatings(ratingsDir string, limit int) []*SeekingAlphaRecord {
	log.Info().Str("DirPath", ratingsDir).Msg("Loading ratings from directory")

	files, err := ioutil.ReadDir(ratingsDir)
	if err != nil {
		log.Error().Msg(err.Error())
		return make([]*SeekingAlphaRecord, 0)
	}

	if limit > 0 {
		files = files[:limit]
	}

	ratings := make([]*SeekingAlphaRecord, 0, len(files))
	for _, ff := range files {
		if !ff.IsDir() {
			log.Info().Str("FileName", ff.Name()).Msg("Reading file")
			content, err := ioutil.ReadFile(fmt.Sprintf("%s/%s", ratingsDir, ff.Name()))
			if err != nil {
				log.Error().Msg(err.Error())
			}

			var record SeekingAlphaRecord
			err = json.Unmarshal(content, &record)
			if err != nil {
				log.Error().Str("Original error", err.Error()).Msg("Could not unmarshal json")
			}

			// convert date -- ignores error
			record.Date, _ = time.Parse("2006-01-02", record.DateStr)

			ratings = append(ratings, &record)
		}
	}

	return ratings
}

func isValidExchange(record *SeekingAlphaRecord) bool {
	return (record.Exchange != "OTCQX" &&
		record.Exchange != "OTCQB" &&
		record.Exchange != "OTC Markets" &&
		record.Exchange != "Grey Market" &&
		record.Exchange != "Pink No Info" &&
		record.Exchange != "Pink Current Info")
}

func EnrichWithFigi(records []*SeekingAlphaRecord) []*SeekingAlphaRecord {
	conn, err := pgx.Connect(context.Background(), viper.GetString("DATABASE_URL"))
	if err != nil {
		log.Error().Str("OriginalError", err.Error()).Msg("Could not connect to database")
	}
	defer conn.Close(context.Background())

	// build a list of all active records that have SA composite figi's
	saIdMap := make(map[int]*Ticker)
	rows, err := conn.Query(context.Background(), "SELECT ticker, seeking_alpha_id, composite_figi FROM tickers_v1 WHERE active='t' AND seeking_alpha_id IS NOT NULL AND composite_figi IS NOT NULL")
	if err != nil {
		log.Error().Str("OriginalError", err.Error()).Msg("Failed to retrieve tickers from database")
	}

	for rows.Next() {
		var ticker Ticker
		err := rows.Scan(&ticker.Ticker, &ticker.TickerId, &ticker.CompositeFigi)
		if err != nil {
			log.Error().Str("OriginalError", err.Error()).Msg("Failed to retrieve ticker row from database")
		}
		saIdMap[ticker.TickerId] = &ticker
	}

	// Fill out composite figi in sa records
	missingTickers := make(map[string]*SeekingAlphaRecord)
	for _, r := range records {
		if t, ok := saIdMap[r.TickerId]; ok {
			if r.Ticker == t.Ticker {
				r.CompositeFigi = t.CompositeFigi
			} else {
				missingTickers[r.Ticker] = r
			}
		} else {
			missingTickers[r.Ticker] = r
		}
	}

	// For each missing FIGI search for it in the tickers_v1 table
	for tickerStr, record := range missingTickers {
		var ticker Ticker
		saTickerId := record.TickerId

		if isValidExchange(record) {
			log.Info().Str("Ticker", tickerStr).Int("SeekingAlphaId", saTickerId).Msg("Ticker is not currently associated with Seeking Alpha ID in database")
		}

		err := conn.QueryRow(context.Background(), `
			SELECT
			    name,
				composite_figi,
				ticker
			FROM
				tickers_v1
			WHERE
				active = 't' AND
				composite_figi IS NOT NULL AND
				seeking_alpha_id IS NULL AND
				ticker = $1
		`, tickerStr).Scan(&ticker.CompanyName, &ticker.CompositeFigi, &ticker.Ticker)

		if err != nil {
			if isValidExchange(record) {
				log.Warn().Str("ticker", tickerStr).Int("SeekingAlphaId", saTickerId).Msg("No tickers found for ticker")
			}
			continue
		}

		// first make sure the company names are similar - as a protective measure
		similarity := strutil.Similarity(strings.ToLower(ticker.CompanyName), strings.ToLower(record.CompanyName), metrics.NewJaroWinkler())
		if similarity < .7 {
			log.Warn().Float64("Similarity", similarity).Str("ticker", record.Ticker).Int("SeekingAlphaId", record.TickerId).Str("DbCompanyName", ticker.CompanyName).Str("SaCompanyName", record.CompanyName).Msg("Not linking ticker due to company name's being too dissimilar")
			continue
		}

		ticker.TickerId = saTickerId
		saIdMap[saTickerId] = &ticker
		record.CompositeFigi = ticker.CompositeFigi

		// Update database with Seeking Alpha ID
		_, err = conn.Exec(context.Background(), `
			UPDATE tickers_v1 SET
				seeking_alpha_id=$1
			WHERE
				active='t' AND
				seeking_alpha_id IS NULL AND
				composite_figi=$2 AND
				ticker=$3
		`, ticker.TickerId, ticker.CompositeFigi, ticker.Ticker)
		if err != nil {
			log.Error().Str("ticker", tickerStr).Int("SeekingAlphaId", ticker.TickerId).Str("compositeFigi", ticker.CompositeFigi).Msg("Failed to update database with ticker info")
		}
	}

	return records
}
