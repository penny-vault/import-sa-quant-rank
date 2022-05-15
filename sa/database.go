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

	"github.com/jackc/pgx/v4"
	"github.com/rs/zerolog/log"
	"github.com/spf13/viper"
)

func SaveToDB(records []*SeekingAlphaRecord) {
	conn, err := pgx.Connect(context.Background(), viper.GetString("DATABASE_URL"))
	if err != nil {
		log.Error().Str("OriginalError", err.Error()).Msg("Could not connect to database")
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
