// Copyright 2022
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package sa

import (
	"context"
	"strings"

	"github.com/adrg/strutil"
	"github.com/adrg/strutil/metrics"
	"github.com/jackc/pgx/v4"
	"github.com/rs/zerolog/log"
	"github.com/spf13/viper"
)

func EnrichWithFigi(records []*SeekingAlphaRecord) []*SeekingAlphaRecord {
	conn, err := pgx.Connect(context.Background(), viper.GetString("database.url"))
	if err != nil {
		log.Error().Err(err).Msg("Could not connect to database")
	}
	defer conn.Close(context.Background())

	// build a list of all active records that have SA composite figi's
	saIdMap := make(map[int]*Ticker)
	rows, err := conn.Query(context.Background(), "SELECT ticker, seeking_alpha_id, composite_figi FROM assets WHERE active='t' AND seeking_alpha_id IS NOT NULL AND composite_figi IS NOT NULL")
	if err != nil {
		log.Error().Err(err).Msg("Failed to retrieve tickers from database")
	}

	for rows.Next() {
		var ticker Ticker
		err := rows.Scan(&ticker.Ticker, &ticker.TickerId, &ticker.CompositeFigi)
		if err != nil {
			log.Error().Err(err).Msg("Failed to retrieve ticker row from database")
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

	// For each missing FIGI search for it in the assets table
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
				assets
			WHERE
				active = 't' AND
				composite_figi IS NOT NULL AND
				seeking_alpha_id IS NULL AND
				ticker = $1
		`, tickerStr).Scan(&ticker.CompanyName, &ticker.CompositeFigi, &ticker.Ticker)

		if err != nil {
			if isValidExchange(record) {
				log.Warn().Err(err).Str("ticker", tickerStr).Int("SeekingAlphaId", saTickerId).Msg("No assets found for ticker")
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
			UPDATE assets SET
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

func SaveToDB(records []*SeekingAlphaRecord) {
	conn, err := pgx.Connect(context.Background(), viper.GetString("database.url"))
	if err != nil {
		log.Error().Err(err).Msg("Could not connect to database")
	}
	defer conn.Close(context.Background())

	for _, r := range records {
		conn.Exec(context.Background(),
			`INSERT INTO seeking_alpha (
			"ticker",
			"composite_figi",
			"event_date",
			"market_cap_mil",
			"quant_rating",
			"growth_grade",
			"profitability_grade",
			"value_grade",
			"eps_revisions_grade",
			"authors_rating_pro",
			"sell_side_rating"
		) VALUES (
			$1,
			$2,
			$3,
			$4,
			$5,
			$6,
			$7,
			$8,
			$9,
			$10,
			$11
		) ON CONFLICT ON CONSTRAINT seeking_alpha_pkey
		DO UPDATE SET
			market_cap_mil = EXCLUDED.market_cap_mil,
			quant_rating = EXCLUDED.quant_rating,
			growth_grade = EXCLUDED.growth_grade,
			profitability_grade = EXCLUDED.profitability_grade,
			value_grade = EXCLUDED.value_grade,
			eps_revisions_grade = EXCLUDED.eps_revisions_grade,
			authors_rating_pro = EXCLUDED.authors_rating_pro,
			sell_side_rating = EXCLUDED.sell_side_rating;
		`,
			r.Ticker, r.CompositeFigi, r.Date, r.MarketCap/1e6,
			r.QuantRating, r.GrowthCategory, r.ProfitabilityCategory,
			r.ValueCategory, r.EpsRevisionsCategory,
			r.AuthorsRatingPro, r.SellSideRating)
	}

	log.Info().Int("NumRecords", len(records)).Msg("records saved to DB")
}
