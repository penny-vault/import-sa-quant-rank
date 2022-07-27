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

import "github.com/rs/zerolog/log"

func ValidateRatings(records []*SeekingAlphaRecord) {
	log.Info().Msg("validating downloaded ratings fields have non-zero values")
	var sumQuant float32 = 0.0
	var sumGrowth float32 = 0.0
	var sumRevisions float32 = 0.0
	var sumMomentum float32 = 0.0
	var sumProfit float32 = 0.0
	var sumValue float32 = 0.0

	for _, record := range records {
		sumQuant += record.QuantRating
		sumGrowth += record.GrowthCategory
		sumRevisions += record.EpsRevisionsCategory
		sumMomentum += record.MomentumCategory
		sumProfit += record.ProfitabilityCategory
		sumValue += record.ValueCategory
	}

	if sumQuant < 1 {
		log.Fatal().Float32("sumQuant", sumQuant).Msg("quant_rating field is 0 for all records")
	}

	if sumGrowth < 1 {
		log.Fatal().Float32("sumGrowth", sumGrowth).Msg("growth_category field is 0 for all records")
	}

	if sumRevisions < 1 {
		log.Fatal().Float32("sumRevisions", sumRevisions).Msg("eps_revisions_category field is 0 for all records")
	}

	if sumMomentum < 1 {
		log.Fatal().Float32("sumMomentum", sumMomentum).Msg("momentum_category field is 0 for all records")
	}

	if sumProfit < 1 {
		log.Fatal().Float32("sumProfit", sumProfit).Msg("profitability_category field is 0 for all records")
	}

	if sumValue < 1 {
		log.Fatal().Float32("sumValue", sumValue).Msg("value_category field is 0 for all records")
	}

}
