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
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"reflect"
	"strconv"
	"strings"
	"time"

	"github.com/penny-vault/import-sa-quant-rank/common"
	"github.com/playwright-community/playwright-go"
	"github.com/rs/zerolog/log"
	"github.com/schollz/progressbar/v3"
	"github.com/spf13/viper"
)

func Download() ([]*SeekingAlphaRecord, error) {
	page, context, browser, pw := common.StartPlaywright(viper.GetBool("playwright.headless"))

	// get time of metrics
	today := getMarketTime()

	// start fetching metrics for each ticker
	consolidatedMetrics := make(map[string]*SeekingAlphaRecord)

	log.Info().Time("Date", today).Msg("running Seeking Alpha quant import")

	// Block unnessessary requests
	setupPageBlocks(page)

	pageNum := 1
	numPages := 2

	// Load the screeners page
	if _, err := page.Goto(SCREENER_PAGE_URL, playwright.PageGotoOptions{
		WaitUntil: playwright.WaitUntilStateNetworkidle,
	}); err != nil {
		log.Error().Err(err).Msg("could not load activity page")
		return []*SeekingAlphaRecord{}, err
	}

	var bar *progressbar.ProgressBar
	if !viper.GetBool("display.hide_progress") {
		bar = progressbar.Default(256)
	}

	for ; pageNum < numPages; pageNum++ {

		// restart chromium every 5 pages to deal with strange segfault error in playwright
		if (pageNum % 5) == 0 {
			common.StopPlaywright(page, context, browser, pw)
			page, context, browser, pw = common.StartPlaywright(viper.GetBool("playwright.headless"))
			setupPageBlocks(page)

			if _, err := page.Goto(SCREENER_PAGE_URL, playwright.PageGotoOptions{
				WaitUntil: playwright.WaitUntilStateNetworkidle,
			}); err != nil {
				log.Error().Err(err).Msg("could not load activity page")
				return []*SeekingAlphaRecord{}, err
			}
		}

		if !viper.GetBool("display.hide_progress") {
			bar.Add(1)
		}

		var tickerStrs []string
		var err error
		tickerStrs, numPages, err = fetchScreenerResults(page, pageNum)
		if err != nil {
			log.Error().Err(err).Msg("error during fetchScreenerResults")
			return []*SeekingAlphaRecord{}, err
		}

		if !viper.GetBool("display.hide_progress") {
			bar.ChangeMax(numPages)
		}

		for _, metricsUrl := range []string{METRICS_1_URL, METRICS_2_URL, METRICS_3_URL, METRICS_4_URL, METRICS_5_URL, METRICS_6_URL, METRICS_7_URL, METRICS_8_URL, METRICS_9_URL, METRICS_10_URL, METRICS_11_URL, METRICS_12_URL} {
			// Delay 150 ms to prevent being blocked
			page.WaitForTimeout(150)

			// fetch metrics
			metrics, err := fetchMetricsResults(page, metricsUrl, tickerStrs)
			if err != nil {
				log.Error().Err(err).Msg("error during fetchMetricsResults")
				return []*SeekingAlphaRecord{}, err
			}

			// parse metrics
			parseMetrics(metrics, consolidatedMetrics)
		}
	}

	log.Info().Int("NumRecords", len(consolidatedMetrics)).Msg("loaded seeking alpha record")
	common.StopPlaywright(page, context, browser, pw)

	result := make([]*SeekingAlphaRecord, 0, len(consolidatedMetrics))
	for _, item := range consolidatedMetrics {
		item.Ticker = strings.ReplaceAll(strings.ToUpper(item.Ticker), ".", "/")
		result = append(result, item)
	}

	return result, nil
}

func parseMetrics(metricsResult MetricsResponse, consolidatedMetrics map[string]*SeekingAlphaRecord) {
	today := getMarketTime()
	metricTickers, metricTypes := parseMetricsMeta(metricsResult)

	for _, item := range metricsResult.Data {
		if item.Type == "ticker_metric_grade" || item.Type == "metric" {
			tickerId := item.Relationships.Ticker.Data.ID
			tickerIdInt, err := strconv.Atoi(tickerId)
			if err != nil {
				log.Warn().
					Str("tickerId", tickerId).
					Msg("cannot parse int from tickerId")
				continue
			}

			metricId := item.Relationships.MetricType.Data.ID

			var metricBundle *SeekingAlphaRecord
			var ok bool

			if metricBundle, ok = consolidatedMetrics[tickerId]; !ok {
				// no current entry
				if tickerData, ok := metricTickers[tickerId]; ok {
					metricBundle = &SeekingAlphaRecord{
						DateStr:        today.Format("2006-01-02"),
						Date:           today,
						TickerId:       tickerIdInt,
						Ticker:         tickerData.Ticker,
						CompanyName:    tickerData.CompanyName,
						Exchange:       tickerData.Exchange,
						Type:           tickerData.EquityType,
						FollowersCount: tickerData.FollowersCount,
					}
					consolidatedMetrics[tickerId] = metricBundle
				} else {
					log.Warn().Str("tickerId", tickerId).Msg("cannot find ticker for associated tickerId")
					continue
				}
			}

			var metricName string
			if metricName, ok = metricTypes[metricId]; !ok {
				log.Warn().Str("MetricId", metricId).Msg("could not find info metric id")
				continue
			}

			evaluateMetrics(metricName, item, metricBundle)
		} else {
			log.Warn().Str("Type", item.Type).Msg("unknown item type")
		}
	}
}

func parseMetricsMeta(metricsResult MetricsResponse) (map[string]*Ticker, map[string]string) {
	metricTickers := make(map[string]*Ticker)
	metricTypes := make(map[string]string)

	for _, item := range metricsResult.Meta {
		switch item.Type {
		case "ticker":
			t := Ticker{}

			if companyName, ok := item.Attributes["companyName"]; ok {
				if t.CompanyName, ok = companyName.(string); !ok {
					log.Warn().Msg("companyName is not a string")
				}
			}

			if tickerId, err := strconv.Atoi(item.ID); err != nil {
				log.Warn().Err(err).Msg("could not parse ticker id")
				continue
			} else {
				t.TickerId = tickerId
			}

			if ticker, ok := item.Attributes["slug"]; ok {
				if t.Ticker, ok = ticker.(string); !ok {
					log.Warn().Msg("ticker is not a string")
				}
			} else {
				log.Warn().Msg("cannot get ticker name")
				continue
			}

			if equityType, ok := item.Attributes["equityType"]; ok {
				if t.EquityType, ok = equityType.(string); !ok {
					log.Warn().Msg("equityType is not a string")
				}
			}

			if exchange, ok := item.Attributes["exchange"]; ok {
				if t.Exchange, ok = exchange.(string); !ok {
					log.Warn().Msg("Exchange is not a string")
				}
			}

			if isBdc, ok := item.Attributes["isBdc"]; ok {
				if t.IsBdc, ok = isBdc.(bool); !ok {
					log.Warn().Msg("isBdc is not a boolean")
				}
			}

			if isDefunct, ok := item.Attributes["isDefunct"]; ok {
				if t.IsDefunct, ok = isDefunct.(bool); !ok {
					log.Warn().Msg("isDefunct is not a bool")
				}
			}

			if isReit, ok := item.Attributes["isReit"]; ok {
				if t.IsReit, ok = isReit.(bool); !ok {
					log.Warn().Msg("isReit is not a bool")
				}
			}

			if followersCount, ok := item.Attributes["followersCount"]; ok {
				if typedFollowersCount, ok := followersCount.(float64); !ok {
					typ := reflect.TypeOf(followersCount)
					log.Warn().Str("ExpectedType", typ.Name()).Msg("followersCount is not an int")
				} else {
					t.FollowersCount = int(typedFollowersCount)
				}
			}

			metricTickers[item.ID] = &t
		case "metric_type":
			if field, ok := item.Attributes["field"]; ok {
				if metricTypes[item.ID], ok = field.(string); !ok {
					log.Warn().Msg("field is not a string")
				}
			} else {
				log.Warn().Msg("metric field name not present")
				continue
			}
		default:
			log.Warn().Str("Type", item.Type).Msg("unknown meta-data type")
		}
	}

	return metricTickers, metricTypes
}

func fetchMetricsResults(page playwright.Page, metricsUrl string, tickerStrs []string) (MetricsResponse, error) {
	encodedTickers := strings.Join(tickerStrs, "%2C")
	myUrl := fmt.Sprintf("%s%s", metricsUrl, encodedTickers)

	resp, err := page.ExpectResponse("**/api/v3/*metric*", func() error {
		_, err := page.Evaluate(`(url) => {
                    fetch(url);
                }`, myUrl)
		if err != nil {
			log.Error().Err(err).Str("Url", myUrl).Msg("error in metrics page evaluate")
		}
		return err
	})
	if err != nil {
		log.Error().Err(err).Msg("error in expect response")
		return MetricsResponse{}, err
	}

	status := resp.Status()
	var metricsResult MetricsResponse
	if status == 200 {
		err = resp.JSON(&metricsResult)
		if err != nil {
			log.Error().Err(err).Msg("error deserializing JSON for metrics response")
			return MetricsResponse{}, errors.New("error deserializing JSON for metrics response")
		}
	} else {
		log.Error().Int("Status", status).Msg("metrics response has invalid status")
		return MetricsResponse{}, errors.New("metrics response has invalid status")
	}

	return metricsResult, nil
}

func fetchScreenerResults(page playwright.Page, pageNum int) ([]string, int, error) {
	screenerArguments := ScreenerArguments{
		Filter: FilterGroup{
			QuantRating: FilterDef{
				Gte:     1,
				Lte:     5,
				Exclude: false,
			},
		},
		Page:       pageNum,
		PerPage:    100,
		Sort:       nil,
		TotalCount: true,
		Type:       "stock",
	}

	args, err := json.Marshal(screenerArguments)
	if err != nil {
		log.Warn().Err(err).Msg("could not marshal screener arguments")
	}

	resp, err := page.ExpectResponse(SCREENER_API_URL, func() error {
		_, err := page.Evaluate(`(params) => {
            fetch('https://seekingalpha.com/api/v3/screener_results', {
                method: 'POST',
                cache: 'no-cache',
                headers: {
                    'Content-Type': 'application/json'
                },
                body: params,
            });
        }`, string(args))
		if err != nil {
			log.Error().Err(err).Msg("error in page evaluate")
		}
		return err
	})
	if err != nil {
		log.Error().Err(err).Msg("failed waiting for response for POST SCREENER_URL")
		return []string{}, 0, err
	}

	status := resp.Status()

	var screenerData ScreenerResponse
	if status == 200 {
		if err := resp.JSON(&screenerData); err != nil {
			log.Error().Err(err).Msg("error parsing JSON response for SCREENER_URL")
			return []string{}, 0, err
		}
	} else {
		log.Error().Int("Status", status).Int("PageNum", pageNum).Msg("invalid status received from POST SCREENER_URL")
		return []string{}, 0, errors.New("invalid screener API status returned")
	}

	if screenerData.Meta.Count < 3000 {
		log.Error().Int("Count", screenerData.Meta.Count).Msg("Num tickers matching screen is below threshold")
		return []string{}, 0, errors.New("tickers returned below threshold")
	}

	tickerStrs := make([]string, 0, len(screenerData.Data))
	for _, item := range screenerData.Data {
		tickerStrs = append(tickerStrs, item.Attributes.Slug)
	}

	// recalculate the number of pages
	numPages := int(math.Ceil(float64(screenerData.Meta.Count) / 100.0))

	return tickerStrs, numPages, nil
}

func getMarketTime() time.Time {
	nyc, err := time.LoadLocation("America/New_York")
	if err != nil {
		log.Error().Err(err).Msg("could not load timezone")
	}
	now := time.Now()
	today := time.Date(now.Year(), now.Month(), now.Day(), 9, 30, 0, 0, nyc)
	return today
}

func setupPageBlocks(page playwright.Page) {
	// block a variety of domains that contain trackers and ads
	page.Route("**/*", func(route playwright.Route) {
		request := route.Request()
		if strings.Contains(request.URL(), "google.com") ||
			strings.Contains(request.URL(), "facebook.com") ||
			strings.Contains(request.URL(), "adsystem.com") ||
			strings.Contains(request.URL(), "sitescout.com") ||
			strings.Contains(request.URL(), "ipredictive.com") ||
			strings.Contains(request.URL(), "eyeota.net") ||
			// strings.Contains(request.URL(), "collect") ||
			strings.Contains(request.URL(), "beacon") ||
			strings.Contains(request.URL(), "mone") ||
			strings.Contains(request.URL(), "mone_event") {
			err := route.Abort("failed")
			if err != nil {
				log.Error().Err(err).Msg("failed blocking route")
			}
			return
		}

		if request.ResourceType() == "image" {
			err := route.Abort("failed")
			if err != nil {
				log.Error().Err(err).Msg("failed blocking image")
			}
		}

		route.Continue()
	})
}

func evaluateMetrics(metricName string, item MetricItem, metricBundle *SeekingAlphaRecord) {
	var ok bool
	switch metricName {
	case "marketcap_display":
		var meaningful bool
		if meaningful, ok = item.Attributes["meaningful"].(bool); !ok {
			return
		}
		if !meaningful {
			return
		}
		if metricBundle.MarketCap, ok = item.Attributes["value"].(float64); !ok {
			log.Warn().Str("metricName", metricName).Msg("could not convert value")
		}
	case "quant_rating":
		var meaningful bool
		if meaningful, ok = item.Attributes["meaningful"].(bool); !ok {
			return
		}
		if !meaningful {
			return
		}
		if quantRating, ok := item.Attributes["value"].(float64); !ok {
			log.Warn().Str("metricName", metricName).Msg("could not convert value")
		} else {
			metricBundle.QuantRating = float32(quantRating)
		}
	case "authors_rating":
		var meaningful bool
		if meaningful, ok = item.Attributes["meaningful"].(bool); !ok {
			return
		}
		if !meaningful {
			return
		}
		if theAuthorsRatingPro, ok := item.Attributes["value"].(float64); ok {
			metricBundle.AuthorsRatingPro = float32(theAuthorsRatingPro)
		} else {
			log.Warn().Str("metricName", metricName).Msg("could not convert value")
		}
	case "sell_side_rating":
		var meaningful bool
		if meaningful, ok = item.Attributes["meaningful"].(bool); !ok {
			return
		}
		if !meaningful {
			return
		}
		if theSellSideRating, ok := item.Attributes["value"].(float64); ok {
			metricBundle.SellSideRating = float32(theSellSideRating)
		} else {
			log.Warn().Str("metricName", metricName).Msg("could not convert value")
		}
	case "value_category":
		if theValueCategory, ok := item.Attributes["grade"].(float64); ok {
			metricBundle.ValueCategory = float32(theValueCategory)
		} else {
			log.Warn().Str("metricName", metricName).Msg("could not convert value")
		}
	case "growth_category":
		if theGrowthCategory, ok := item.Attributes["grade"].(float64); ok {
			metricBundle.GrowthCategory = float32(theGrowthCategory)
		} else {
			log.Warn().Str("metricName", metricName).Msg("could not convert value")
		}
	case "profitability_category":
		if theProfitabilityCategory, ok := item.Attributes["grade"].(float64); ok {
			metricBundle.ProfitabilityCategory = float32(theProfitabilityCategory)
		} else {
			log.Warn().Str("metricName", metricName).Msg("could not convert value")
		}
	case "momentum_category":
		if theMomentumCategory, ok := item.Attributes["grade"].(float64); ok {
			metricBundle.MomentumCategory = float32(theMomentumCategory)
		} else {
			log.Warn().Str("metricName", metricName).Msg("could not convert value")
		}
	case "eps_revisions_category":
		if theEpsRevisionsCategory, ok := item.Attributes["grade"].(float64); ok {
			metricBundle.EpsRevisionsCategory = float32(theEpsRevisionsCategory)
		} else {
			log.Warn().Str("metricName", metricName).Msg("could not convert value")
		}
	case "earning_announce_date":
		var meaningful bool
		if meaningful, ok = item.Attributes["meaningful"].(bool); !ok {
			return
		}
		if !meaningful {
			return
		}
		if theEarningAnnounceTimestamp, ok := item.Attributes["value"].(float64); ok {
			metricBundle.EarningAnnounceTimestamp = int64(theEarningAnnounceTimestamp)
		} else {
			log.Warn().Str("metricName", metricName).Msg("could not convert value")
		}
	case "eps_estimate_fy1":
		var meaningful bool
		if meaningful, ok = item.Attributes["meaningful"].(bool); !ok {
			return
		}
		if !meaningful {
			return
		}
		if metricBundle.EpsEstimateFy1, ok = item.Attributes["value"].(float64); !ok {
			log.Warn().Str("metricName", metricName).Msg("could not convert value")
		}
	case "revenue_estimate":
		var meaningful bool
		if meaningful, ok = item.Attributes["meaningful"].(bool); !ok {
			return
		}
		if !meaningful {
			return
		}
		if metricBundle.RevenueEstimate, ok = item.Attributes["value"].(float64); !ok {
			log.Warn().Str("metricName", metricName).Msg("could not convert value")
		}
	case "eps_normalized_actual":
		var meaningful bool
		if meaningful, ok = item.Attributes["meaningful"].(bool); !ok {
			return
		}
		if !meaningful {
			return
		}
		if theEpsNormalizedActual, ok := item.Attributes["value"].(float64); ok {
			metricBundle.EpsNormalizedActual = float32(theEpsNormalizedActual)
		} else {
			log.Warn().Str("metricName", metricName).Msg("could not convert value")
		}
	case "eps_surprise":
		var meaningful bool
		if meaningful, ok = item.Attributes["meaningful"].(bool); !ok {
			return
		}
		if !meaningful {
			return
		}
		if theEpsSurprise, ok := item.Attributes["value"].(float64); ok {
			metricBundle.EpsSurprise = float32(theEpsSurprise)
		} else {
			log.Warn().Str("metricName", metricName).Msg("could not convert value")
		}
	case "revenue_actual":
		var meaningful bool
		if meaningful, ok = item.Attributes["meaningful"].(bool); !ok {
			return
		}
		if !meaningful {
			return
		}
		if metricBundle.RevenueActual, ok = item.Attributes["value"].(float64); !ok {
			log.Warn().Str("metricName", metricName).Msg("could not convert value")
		}
	case "revenue_surprise":
		var meaningful bool
		if meaningful, ok = item.Attributes["meaningful"].(bool); !ok {
			return
		}
		if !meaningful {
			return
		}
		if metricBundle.RevenueSurprise, ok = item.Attributes["value"].(float64); !ok {
			log.Warn().Str("metricName", metricName).Msg("could not convert value")
		}
	case "tev":
		var meaningful bool
		if meaningful, ok = item.Attributes["meaningful"].(bool); !ok {
			return
		}
		if !meaningful {
			return
		}
		if metricBundle.Tev, ok = item.Attributes["value"].(float64); !ok {
			log.Warn().Str("metricName", metricName).Msg("could not convert value")
		}
	case "pe_ratio":
		var meaningful bool
		if meaningful, ok = item.Attributes["meaningful"].(bool); !ok {
			return
		}
		if !meaningful {
			return
		}
		if thePeRatio, ok := item.Attributes["value"].(float64); ok {
			metricBundle.PeRatio = float32(thePeRatio)
		} else {
			log.Warn().Str("metricName", metricName).Msg("could not convert value")
		}
	case "pe_nongaap_fy1":
		var meaningful bool
		if meaningful, ok = item.Attributes["meaningful"].(bool); !ok {
			return
		}
		if !meaningful {
			return
		}
		if thePeNonGaapFy1, ok := item.Attributes["value"].(float64); ok {
			metricBundle.PeNonGaapFy1 = float32(thePeNonGaapFy1)
		} else {
			log.Warn().Str("metricName", metricName).Msg("could not convert value")
		}
	case "ps_ratio":
		var meaningful bool
		if meaningful, ok = item.Attributes["meaningful"].(bool); !ok {
			return
		}
		if !meaningful {
			return
		}
		if thePsRatio, ok := item.Attributes["value"].(float64); ok {
			metricBundle.PsRatio = float32(thePsRatio)
		} else {
			log.Warn().Str("metricName", metricName).Msg("could not convert value")
		}
	case "ev_12m_sales_ratio":
		var meaningful bool
		if meaningful, ok = item.Attributes["meaningful"].(bool); !ok {
			return
		}
		if !meaningful {
			return
		}
		if theEv12mSalesRatio, ok := item.Attributes["value"].(float64); ok {
			metricBundle.Ev12mSalesRatio = float32(theEv12mSalesRatio)
		} else {
			log.Warn().Str("metricName", metricName).Msg("could not convert value")
		}
	case "ev_ebitda":
		var meaningful bool
		if meaningful, ok = item.Attributes["meaningful"].(bool); !ok {
			return
		}
		if !meaningful {
			return
		}
		if theEvEbitda, ok := item.Attributes["value"].(float64); ok {
			metricBundle.EvEbitda = float32(theEvEbitda)
		} else {
			log.Warn().Str("metricName", metricName).Msg("could not convert value")
		}
	case "pb_ratio":
		var meaningful bool
		if meaningful, ok = item.Attributes["meaningful"].(bool); !ok {
			return
		}
		if !meaningful {
			return
		}
		if thePbRatio, ok := item.Attributes["value"].(float64); ok {
			metricBundle.PbRatio = float32(thePbRatio)
		} else {
			log.Warn().Str("metricName", metricName).Msg("could not convert value")
		}
	case "price_cf_ratio":
		var meaningful bool
		if meaningful, ok = item.Attributes["meaningful"].(bool); !ok {
			return
		}
		if !meaningful {
			return
		}
		if thePriceCfRatio, ok := item.Attributes["value"].(float64); ok {
			metricBundle.PriceCfRatio = float32(thePriceCfRatio)
		} else {
			log.Warn().Str("metricName", metricName).Msg("could not convert value")
		}
	case "revenue_growth":
		var meaningful bool
		if meaningful, ok = item.Attributes["meaningful"].(bool); !ok {
			return
		}
		if !meaningful {
			return
		}
		if theRevenueGrowth, ok := item.Attributes["value"].(float64); ok {
			metricBundle.RevenueGrowth = float32(theRevenueGrowth)
		} else {
			log.Warn().Str("metricName", metricName).Msg("could not convert value")
		}
	case "revenue_change_display":
		var meaningful bool
		if meaningful, ok = item.Attributes["meaningful"].(bool); !ok {
			return
		}
		if !meaningful {
			return
		}
		if theRevenueChange, ok := item.Attributes["value"].(float64); ok {
			metricBundle.RevenueChange = float32(theRevenueChange)
		} else {
			log.Warn().Str("metricName", metricName).Msg("could not convert value")
		}
	case "revenue_growth3":
		var meaningful bool
		if meaningful, ok = item.Attributes["meaningful"].(bool); !ok {
			return
		}
		if !meaningful {
			return
		}
		if theRevenueGrowth3, ok := item.Attributes["value"].(float64); ok {
			metricBundle.RevenueGrowth3 = float32(theRevenueGrowth3)
		} else {
			log.Warn().Str("metricName", metricName).Msg("could not convert value")
		}
	case "ebitda_yoy":
		var meaningful bool
		if meaningful, ok = item.Attributes["meaningful"].(bool); !ok {
			return
		}
		if !meaningful {
			return
		}
		if theEbitdaYoy, ok := item.Attributes["value"].(float64); ok {
			metricBundle.EbitdaYoy = float32(theEbitdaYoy)
		} else {
			log.Warn().Str("metricName", metricName).Msg("could not convert value")
		}
	case "ebitda_3y_cagr":
		var meaningful bool
		if meaningful, ok = item.Attributes["meaningful"].(bool); !ok {
			return
		}
		if !meaningful {
			return
		}
		if theEbitda3yCagr, ok := item.Attributes["value"].(float64); ok {
			metricBundle.Ebitda3yCagr = float32(theEbitda3yCagr)
		} else {
			log.Warn().Str("metricName", metricName).Msg("could not convert value")
		}
	case "net_income_3y_cagr":
		var meaningful bool
		if meaningful, ok = item.Attributes["meaningful"].(bool); !ok {
			return
		}
		if !meaningful {
			return
		}
		if theNetIncome3yCagr, ok := item.Attributes["value"].(float64); ok {
			metricBundle.NetIncome3yCagr = float32(theNetIncome3yCagr)
		} else {
			log.Warn().Str("metricName", metricName).Msg("could not convert value")
		}
	case "diluted_eps_growth":
		var meaningful bool
		if meaningful, ok = item.Attributes["meaningful"].(bool); !ok {
			return
		}
		if !meaningful {
			return
		}
		if theDilutedEpsGrowth, ok := item.Attributes["value"].(float64); ok {
			metricBundle.DilutedEpsGrowth = float32(theDilutedEpsGrowth)
		} else {
			log.Warn().Str("metricName", metricName).Msg("could not convert value")
		}
	case "earnings_growth_3y_cagr":
		var meaningful bool
		if meaningful, ok = item.Attributes["meaningful"].(bool); !ok {
			return
		}
		if !meaningful {
			return
		}
		if theEarningsGrowth3yCagr, ok := item.Attributes["value"].(float64); ok {
			metricBundle.EarningsGrowth3yCagr = float32(theEarningsGrowth3yCagr)
		} else {
			log.Warn().Str("metricName", metricName).Msg("could not convert value")
		}
	case "tangible_book_value_3y_cagr":
		var meaningful bool
		if meaningful, ok = item.Attributes["meaningful"].(bool); !ok {
			return
		}
		if !meaningful {
			return
		}
		if theTangibleBookValue3yCagr, ok := item.Attributes["value"].(float64); ok {
			metricBundle.TangibleBookValue3yCagr = float32(theTangibleBookValue3yCagr)
		} else {
			log.Warn().Str("metricName", metricName).Msg("could not convert value")
		}
	case "total_assets_3y_cagr":
		var meaningful bool
		if meaningful, ok = item.Attributes["meaningful"].(bool); !ok {
			return
		}
		if !meaningful {
			return
		}
		if theTotalAssets3yCagr, ok := item.Attributes["value"].(float64); ok {
			metricBundle.TotalAssets3yCagr = float32(theTotalAssets3yCagr)
		} else {
			log.Warn().Str("metricName", metricName).Msg("could not convert value")
		}
	case "total_revenue":
		var meaningful bool
		if meaningful, ok = item.Attributes["meaningful"].(bool); !ok {
			return
		}
		if !meaningful {
			return
		}
		if metricBundle.TotalRevenue, ok = item.Attributes["value"].(float64); !ok {
			log.Warn().Str("metricName", metricName).Msg("could not convert value")
		}
	case "net_income":
		var meaningful bool
		if meaningful, ok = item.Attributes["meaningful"].(bool); !ok {
			return
		}
		if !meaningful {
			return
		}
		if metricBundle.NetIncome, ok = item.Attributes["value"].(float64); !ok {
			log.Warn().Str("metricName", metricName).Msg("could not convert value")
		}
	case "cash_from_operations_as_reported":
		var meaningful bool
		if meaningful, ok = item.Attributes["meaningful"].(bool); !ok {
			return
		}
		if !meaningful {
			return
		}
		if metricBundle.CashFromOperationsAsReported, ok = item.Attributes["value"].(float64); !ok {
			log.Warn().Str("metricName", metricName).Msg("could not convert value")
		}
	case "gross_margin":
		var meaningful bool
		if meaningful, ok = item.Attributes["meaningful"].(bool); !ok {
			return
		}
		if !meaningful {
			return
		}
		if theGrossMargin, ok := item.Attributes["value"].(float64); ok {
			metricBundle.GrossMargin = float32(theGrossMargin)
		} else {
			log.Warn().Str("metricName", metricName).Msg("could not convert value")
		}
	case "ebit_margin":
		var meaningful bool
		if meaningful, ok = item.Attributes["meaningful"].(bool); !ok {
			return
		}
		if !meaningful {
			return
		}
		if theEbitMargin, ok := item.Attributes["value"].(float64); ok {
			metricBundle.EbitMargin = float32(theEbitMargin)
		} else {
			log.Warn().Str("metricName", metricName).Msg("could not convert value")
		}
	case "ebitda_margin":
		var meaningful bool
		if meaningful, ok = item.Attributes["meaningful"].(bool); !ok {
			return
		}
		if !meaningful {
			return
		}
		if theEbitdaMargin, ok := item.Attributes["value"].(float64); ok {
			metricBundle.EbitdaMargin = float32(theEbitdaMargin)
		} else {
			log.Warn().Str("metricName", metricName).Msg("could not convert value")
		}
	case "net_margin":
		var meaningful bool
		if meaningful, ok = item.Attributes["meaningful"].(bool); !ok {
			return
		}
		if !meaningful {
			return
		}
		if theNetMargin, ok := item.Attributes["value"].(float64); ok {
			metricBundle.NetMargin = float32(theNetMargin)
		} else {
			log.Warn().Str("metricName", metricName).Msg("could not convert value")
		}
	case "levered_fcf_margin":
		var meaningful bool
		if meaningful, ok = item.Attributes["meaningful"].(bool); !ok {
			return
		}
		if !meaningful {
			return
		}
		if theLeveredFcfMargin, ok := item.Attributes["value"].(float64); ok {
			metricBundle.LeveredFcfMargin = float32(theLeveredFcfMargin)
		} else {
			log.Warn().Str("metricName", metricName).Msg("could not convert value")
		}
	case "roe":
		var meaningful bool
		if meaningful, ok = item.Attributes["meaningful"].(bool); !ok {
			return
		}
		if !meaningful {
			return
		}
		if theRoe, ok := item.Attributes["value"].(float64); ok {
			metricBundle.Roe = float32(theRoe)
		} else {
			log.Warn().Str("metricName", metricName).Msg("could not convert value")
		}
	case "return_on_avg_tot_assets":
		var meaningful bool
		if meaningful, ok = item.Attributes["meaningful"].(bool); !ok {
			return
		}
		if !meaningful {
			return
		}
		if theReturnOnAvgTotAssets, ok := item.Attributes["value"].(float64); ok {
			metricBundle.ReturnOnAvgTotAssets = float32(theReturnOnAvgTotAssets)
		} else {
			log.Warn().Str("metricName", metricName).Msg("could not convert value")
		}
	case "return_on_total_capital":
		var meaningful bool
		if meaningful, ok = item.Attributes["meaningful"].(bool); !ok {
			return
		}
		if !meaningful {
			return
		}
		if theReturnOnTotalCapital, ok := item.Attributes["value"].(float64); ok {
			metricBundle.ReturnOnTotalCapital = float32(theReturnOnTotalCapital)
		} else {
			log.Warn().Str("metricName", metricName).Msg("could not convert value")
		}
	case "assets_turnover":
		var meaningful bool
		if meaningful, ok = item.Attributes["meaningful"].(bool); !ok {
			return
		}
		if !meaningful {
			return
		}
		if theAssetsTurnover, ok := item.Attributes["value"].(float64); ok {
			metricBundle.AssetsTurnover = float32(theAssetsTurnover)
		} else {
			log.Warn().Str("metricName", metricName).Msg("could not convert value")
		}
	case "net_inc_per_employee":
		var meaningful bool
		if meaningful, ok = item.Attributes["meaningful"].(bool); !ok {
			return
		}
		if !meaningful {
			return
		}
		if metricBundle.NetIncPerEmployee, ok = item.Attributes["value"].(float64); !ok {
			log.Warn().Str("metricName", metricName).Msg("could not convert value")
		}
	case "capex_to_sales":
		var meaningful bool
		if meaningful, ok = item.Attributes["meaningful"].(bool); !ok {
			return
		}
		if !meaningful {
			return
		}
		if theCapexToSales, ok := item.Attributes["value"].(float64); ok {
			metricBundle.CapexToSales = float32(theCapexToSales)
		} else {
			log.Warn().Str("metricName", metricName).Msg("could not convert value")
		}
	case "short_interest_percent_of_float":
		var meaningful bool
		if meaningful, ok = item.Attributes["meaningful"].(bool); !ok {
			return
		}
		if !meaningful {
			return
		}
		if theShortInterestPercentOfFloat, ok := item.Attributes["value"].(float64); ok {
			metricBundle.ShortInterestPercentOfFloat = float32(theShortInterestPercentOfFloat)
		} else {
			log.Warn().Str("metricName", metricName).Msg("could not convert value")
		}
	case "short_interest_coverage_ratio":
		var meaningful bool
		if meaningful, ok = item.Attributes["meaningful"].(bool); !ok {
			return
		}
		if !meaningful {
			return
		}
		if theShortInterestCoverageRatio, ok := item.Attributes["value"].(float64); ok {
			metricBundle.ShortInterestCoverageRatio = float32(theShortInterestCoverageRatio)
		} else {
			log.Warn().Str("metricName", metricName).Msg("could not convert value")
		}
	case "beta24":
		var meaningful bool
		if meaningful, ok = item.Attributes["meaningful"].(bool); !ok {
			return
		}
		if !meaningful {
			return
		}
		if theBeta24, ok := item.Attributes["value"].(float64); ok {
			metricBundle.Beta24 = float32(theBeta24)
		} else {
			log.Warn().Str("metricName", metricName).Msg("could not convert value")
		}
	case "altman_z_score":
		var meaningful bool
		if meaningful, ok = item.Attributes["meaningful"].(bool); !ok {
			return
		}
		if !meaningful {
			return
		}
		if theAltmanZScore, ok := item.Attributes["value"].(float64); ok {
			metricBundle.AltmanZScore = float32(theAltmanZScore)
		} else {
			log.Warn().Str("metricName", metricName).Msg("could not convert value")
		}
	case "shares":
		var meaningful bool
		if meaningful, ok = item.Attributes["meaningful"].(bool); !ok {
			return
		}
		if !meaningful {
			return
		}
		if theShares, ok := item.Attributes["value"].(float64); ok {
			metricBundle.Shares = int64(theShares)
		} else {
			log.Warn().Str("metricName", metricName).Msg("could not convert value")
		}
	case "float_percent":
		var meaningful bool
		if meaningful, ok = item.Attributes["meaningful"].(bool); !ok {
			return
		}
		if !meaningful {
			return
		}
		if theFloatPercent, ok := item.Attributes["value"].(float64); ok {
			metricBundle.FloatPercent = float32(theFloatPercent)
		} else {
			log.Warn().Str("metricName", metricName).Msg("could not convert value")
		}
	case "insiders_shares":
		var meaningful bool
		if meaningful, ok = item.Attributes["meaningful"].(bool); !ok {
			return
		}
		if !meaningful {
			return
		}
		if theInsiderShares, ok := item.Attributes["value"].(float64); ok {
			metricBundle.InsidersShares = int64(theInsiderShares)
		} else {
			log.Warn().Str("metricName", metricName).Msg("could not convert value")
		}
	case "insiders_share_percent":
		var meaningful bool
		if meaningful, ok = item.Attributes["meaningful"].(bool); !ok {
			return
		}
		if !meaningful {
			return
		}
		if metricBundle.InsidersSharePercent, ok = item.Attributes["value"].(float64); !ok {
			log.Warn().Str("metricName", metricName).Msg("could not convert value")
		}
	case "institutions_shares":
		var meaningful bool
		if meaningful, ok = item.Attributes["meaningful"].(bool); !ok {
			return
		}
		if !meaningful {
			return
		}
		if theInstitutionsShares, ok := item.Attributes["value"].(float64); ok {
			metricBundle.InstitutionsShares = int64(theInstitutionsShares)
		} else {
			log.Warn().Str("metricName", metricName).Msg("could not convert value")
		}
	case "institutions_share_percent":
		var meaningful bool
		if meaningful, ok = item.Attributes["meaningful"].(bool); !ok {
			return
		}
		if !meaningful {
			return
		}
		if metricBundle.InstitutionsSharePercent, ok = item.Attributes["value"].(float64); !ok {
			log.Warn().Str("metricName", metricName).Msg("could not convert value")
		}
	case "total_debt":
		var meaningful bool
		if meaningful, ok = item.Attributes["meaningful"].(bool); !ok {
			return
		}
		if !meaningful {
			return
		}
		if metricBundle.TotalDebt, ok = item.Attributes["value"].(float64); !ok {
			log.Warn().Str("metricName", metricName).Msg("could not convert value")
		}
	case "debt_long_term":
		var meaningful bool
		if meaningful, ok = item.Attributes["meaningful"].(bool); !ok {
			return
		}
		if !meaningful {
			return
		}
		if metricBundle.DebtLongTerm, ok = item.Attributes["value"].(float64); !ok {
			log.Warn().Str("metricName", metricName).Msg("could not convert value")
		}
	case "total_cash":
		var meaningful bool
		if meaningful, ok = item.Attributes["meaningful"].(bool); !ok {
			return
		}
		if !meaningful {
			return
		}
		if metricBundle.TotalCash, ok = item.Attributes["value"].(float64); !ok {
			log.Warn().Str("metricName", metricName).Msg("could not convert value")
		}
	case "debt_fcf":
		var meaningful bool
		if meaningful, ok = item.Attributes["meaningful"].(bool); !ok {
			return
		}
		if !meaningful {
			return
		}
		if theDebtFcf, ok := item.Attributes["value"].(float64); ok {
			metricBundle.DebtFcf = float32(theDebtFcf)
		} else {
			log.Warn().Str("metricName", metricName).Msg("could not convert value")
		}
	case "current_ratio":
		var meaningful bool
		if meaningful, ok = item.Attributes["meaningful"].(bool); !ok {
			return
		}
		if !meaningful {
			return
		}
		if theCurrentRatio, ok := item.Attributes["value"].(float64); ok {
			metricBundle.CurrentRatio = float32(theCurrentRatio)
		} else {
			log.Warn().Str("metricName", metricName).Msg("could not convert value")
		}
	case "quick_ratio":
		var meaningful bool
		if meaningful, ok = item.Attributes["meaningful"].(bool); !ok {
			return
		}
		if !meaningful {
			return
		}
		if theQuickRatio, ok := item.Attributes["value"].(float64); ok {
			metricBundle.QuickRatio = float32(theQuickRatio)
		} else {
			log.Warn().Str("metricName", metricName).Msg("could not convert value")
		}
	case "interest_coverage_ratio":
		var meaningful bool
		if meaningful, ok = item.Attributes["meaningful"].(bool); !ok {
			return
		}
		if !meaningful {
			return
		}
		if theInterestCoverageRatio, ok := item.Attributes["value"].(float64); ok {
			metricBundle.InterestCoverageRatio = float32(theInterestCoverageRatio)
		} else {
			log.Warn().Str("metricName", metricName).Msg("could not convert value")
		}
	case "debt_eq":
		var meaningful bool
		if meaningful, ok = item.Attributes["meaningful"].(bool); !ok {
			return
		}
		if !meaningful {
			return
		}
		if theDebtEq, ok := item.Attributes["value"].(float64); ok {
			metricBundle.DebtEq = float32(theDebtEq)
		} else {
			log.Warn().Str("metricName", metricName).Msg("could not convert value")
		}
	case "long_term_debt_per_capital":
		var meaningful bool
		if meaningful, ok = item.Attributes["meaningful"].(bool); !ok {
			return
		}
		if !meaningful {
			return
		}
		if theLongTermDebtPerCapital, ok := item.Attributes["value"].(float64); ok {
			metricBundle.LongTermDebtPerCapital = float32(theLongTermDebtPerCapital)
		} else {
			log.Warn().Str("metricName", metricName).Msg("could not convert value")
		}
	}
}
