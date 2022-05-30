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
	"time"

	"github.com/rs/zerolog"
)

type Ticker struct {
	CompanyName    string `json:"companyName"`
	TickerId       int
	Ticker         string `json:"slug"`
	CompositeFigi  string
	EquityType     string `json:"equityType"`
	Exchange       string `json:"exchange"`
	IsBdc          bool   `json:"isBdc"`
	IsDefunct      bool   `json:"isDefunct"`
	FollowersCount int    `json:"followersCount"`
	IsReit         bool   `json:"isReit"`
}

type SeekingAlphaRecord struct {
	DateStr                      string `json:"date" parquet:"name=Date, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
	Date                         time.Time
	TickerId                     int     `json:"tickerId" parquet:"name=SeekingAlphaTickerId, type=INT32"`
	Ticker                       string  `json:"ticker" parquet:"name=Ticker, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
	CompositeFigi                string  `json:"compositeFigi" parquet:"name=CompositeFigi, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
	CompanyName                  string  `json:"companyName" parquet:"name=CompanyName, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
	Exchange                     string  `json:"exchange" parquet:"name=Exchange, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
	Type                         string  `json:"type" parquet:"name=Type, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
	FollowersCount               int     `parquet:"name=FollowersCount, type=INT32"`
	MarketCap                    float64 `json:"marketcap_display" parquet:"name=MarketCap, type=DOUBLE"`
	QuantRating                  float32 `json:"quant_rating" parquet:"name=QuantRating, type=FLOAT"`
	AuthorsRatingPro             float32 `json:"authors_rating_pro" parquet:"name=AuthorsRatingPro, type=FLOAT"`
	SellSideRating               float32 `json:"sell_side_rating" parquet:"name=SellSideRating, type=FLOAT"`
	ValueCategory                float32 `json:"value_category" parquet:"name=ValueCategory, type=FLOAT"`
	GrowthCategory               float32 `json:"growth_category" parquet:"name=GrowthCategory, type=FLOAT"`
	ProfitabilityCategory        float32 `json:"profitability_category" parquet:"name=ProfitabilityCategory, type=FLOAT"`
	MomentumCategory             float32 `json:"momentum_category" parquet:"name=MomentumCategory, type=FLOAT"`
	EpsRevisionsCategory         float32 `json:"eps_revisions_category" parquet:"name=EpsRevisionsCategory, type=FLOAT"`
	EarningAnnounceTimestamp     int64   `json:"earning_announce_date" parquet:"name=EarningAnnounceTimestamp, type=INT64"`
	EpsEstimateFy1               float64 `json:"eps_estimate_fy1" parquet:"name=EpsEstimateFy1, type=DOUBLE"`
	RevenueEstimate              float64 `json:"revenue_estimate" parquet:"name=RevenueEstimate, type=DOUBLE"`
	EpsNormalizedActual          float32 `json:"eps_normalized_actual" parquet:"name=EpsNormalizedActual, type=FLOAT"`
	EpsSurprise                  float32 `json:"eps_surprise" parquet:"name=EpsSurprise, type=FLOAT"`
	RevenueActual                float64 `json:"revenue_actual" parquet:"name=RevenueActual, type=DOUBLE"`
	RevenueSurprise              float64 `json:"revenue_surprise" parquet:"name=RevenueSurprise, type=DOUBLE"`
	Tev                          float64 `json:"tev" parquet:"name=Tev, type=DOUBLE"`
	PeRatio                      float32 `json:"pe_ratio" parquet:"name=PeRatio, type=FLOAT"`
	PeNonGaapFy1                 float32 `json:"pe_nongaap_fy1" parquet:"name=PeNonGaapFy1, type=FLOAT"`
	PsRatio                      float32 `json:"ps_ratio" parquet:"name=PsRatio, type=FLOAT"`
	Ev12mSalesRatio              float32 `json:"ev_12m_sales_ratio" parquet:"name=Ev12mSalesRatio, type=FLOAT"`
	EvEbitda                     float32 `json:"ev_ebitda" parquet:"name=EvEbitda, type=FLOAT"`
	PbRatio                      float32 `json:"pb_ratio" parquet:"name=PbRatio, type=FLOAT"`
	PriceCfRatio                 float32 `json:"price_cf_ratio" parquet:"name=PriceCfRatio, type=FLOAT"`
	RevenueGrowth                float32 `json:"revenue_growth" parquet:"name=RevenueGrowth, type=FLOAT"`
	RevenueChange                float32 `json:"revenue_change_display" parquet:"name=RevenueChange, type=FLOAT"`
	RevenueGrowth3               float32 `json:"revenue_growth3" parquet:"name=RevenueGrowth3, type=FLOAT"`
	EbitdaYoy                    float32 `json:"ebitda_yoy" parquet:"name=EbitdaYoy, type=FLOAT"`
	Ebitda3yCagr                 float32 `json:"ebitda_3y_cagr" parquet:"name=Ebitda3yCagr, type=FLOAT"`
	NetIncome3yCagr              float32 `json:"net_income_3y_cagr" parquet:"name=NetIncome3yCagr, type=FLOAT"`
	DilutedEpsGrowth             float32 `json:"diluted_eps_growth" parquet:"name=DilutedEpsGrowth, type=FLOAT"`
	EarningsGrowth3yCagr         float32 `json:"earnings_growth_3y_cagr" parquet:"name=EarningsGrowth3yCagr, type=FLOAT"`
	TangibleBookValue3yCagr      float32 `json:"tangible_book_value_3y_cagr" parquet:"name=TangibleBookValue3yCagr, type=FLOAT"`
	TotalAssets3yCagr            float32 `json:"total_assets_3y_cagr" parquet:"name=TotalAssets3yCagr, type=FLOAT"`
	TotalRevenue                 float64 `json:"total_revenue" parquet:"name=TotalRevenue, type=DOUBLE"`
	NetIncome                    float64 `json:"net_income" parquet:"name=NetIncome, type=DOUBLE"`
	CashFromOperationsAsReported float64 `json:"cash_from_operations_as_reported" parquet:"name=CashFromOperationsAsReported, type=DOUBLE"`
	GrossMargin                  float32 `json:"gross_margin" parquet:"name=GrossMargin, type=FLOAT"`
	EbitMargin                   float32 `json:"ebit_margin" parquet:"name=EbitMargin, type=FLOAT"`
	EbitdaMargin                 float32 `json:"ebitda_margin" parquet:"name=EbitdaMargin, type=FLOAT"`
	NetMargin                    float32 `json:"net_margin" parquet:"name=NetMargin, type=FLOAT"`
	LeveredFcfMargin             float32 `json:"levered_fcf_margin" parquet:"name=LeveredFcfMargin, type=FLOAT"`
	Roe                          float32 `json:"roe" parquet:"name=Roe, type=FLOAT"`
	ReturnOnAvgTotAssets         float32 `json:"return_on_avg_tot_assets" parquet:"name=ReturnOnAvgTotAssets, type=FLOAT"`
	ReturnOnTotalCapital         float32 `json:"return_on_total_capital" parquet:"name=ReturnOnTotalCapital, type=FLOAT"`
	AssetsTurnover               float32 `json:"assets_turnover" parquet:"name=AssetsTurnover, type=FLOAT"`
	NetIncPerEmployee            float64 `json:"net_inc_per_employee" parquet:"name=NetIncPerEmployee, type=DOUBLE"`
	CapexToSales                 float32 `json:"capex_to_sales" parquet:"name=CapexToSales, type=FLOAT"`
	ShortInterestPercentOfFloat  float32 `json:"short_interest_percent_of_float" parquet:"name=ShortInterestPercentOfFloat, type=FLOAT"`
	ShortInterestCoverageRatio   float32 `json:"short_interest_coverage_ratio" parquet:"name=ShortInterestCoverageRatio, type=FLOAT"`
	Beta24                       float32 `json:"beta24" parquet:"name=Beta24, type=FLOAT"`
	AltmanZScore                 float32 `json:"altman_z_score" parquet:"name=AltmanZScore, type=FLOAT"`
	Shares                       int64   `json:"shares" parquet:"name=Shares, type=INT64"`
	FloatPercent                 float32 `json:"float_percent" parquet:"name=FloatPercent, type=FLOAT"`
	InsidersShares               int64   `json:"insiders_shares" parquet:"name=InsidersShares, type=INT64"`
	InsidersSharePercent         float64 `json:"insiders_share_percent" parquet:"name=InsidersSharePercent, type=DOUBLE"`
	InstitutionsShares           int64   `json:"institutions_shares" parquet:"name=InstitutionsShares, type=INT64"`
	InstitutionsSharePercent     float64 `json:"institutions_share_percent" parquet:"name=InstitutionsSharePercent, type=DOUBLE"`
	TotalDebt                    float64 `json:"total_debt" parquet:"name=TotalDebt, type=DOUBLE"`
	DebtLongTerm                 float64 `json:"debt_long_term" parquet:"name=DebtLongTerm, type=DOUBLE"`
	TotalCash                    float64 `json:"total_cash" parquet:"name=TotalCash, type=DOUBLE"`
	DebtFcf                      float32 `json:"debt_fcf" parquet:"name=DebtFcf, type=FLOAT"`
	CurrentRatio                 float32 `json:"current_ratio" parquet:"name=CurrentRatio, type=FLOAT"`
	QuickRatio                   float32 `json:"quick_ratio" parquet:"name=QuickRatio, type=FLOAT"`
	InterestCoverageRatio        float32 `json:"interest_coverage_ratio" parquet:"name=InterestCoverageRatio, type=FLOAT"`
	DebtEq                       float32 `json:"debt_eq" parquet:"name=DebtEq, type=FLOAT"`
	LongTermDebtPerCapital       float32 `json:"long_term_debt_per_capital" parquet:"name=LongTermDebtPerCapital, type=FLOAT"`
}

type FilterDef struct {
	Gte      int  `json:"gte"`
	Lte      int  `json:"lte"`
	Disabled bool `json:"disabled"`
}

type FilterGroup struct {
	QuantRating      FilterDef `json:"quant_rating"`
	AuthorsRatingPro FilterDef `json:"authors_rating_pro"`
	SellSideRating   FilterDef `json:"sell_side_rating"`
}

type ScreenerArguments struct {
	Filter  FilterGroup `json:"filter"`
	Page    int         `json:"page"`
	PerPage int         `json:"per_page"`
}

type ScreenerResponseMeta struct {
	Count int `json:"count"`
}

type ScreenerResponse struct {
	Data []ScreenerItem       `json:"data"`
	Meta ScreenerResponseMeta `json:"meta"`
}

type ScreenerItem struct {
	ID         string           `json:"id"`
	Type       string           `json:"type"`
	Attributes TickerAttributes `json:"attributes"`
	Meta       TickerMeta       `json:"meta"`
}

type TickerAttributes struct {
	Slug        string `json:"slug"`
	Name        string `json:"name"`
	Company     string `json:"company"`
	CompanyName string `json:"companyName"`
}

type TickerMeta struct {
	CompanyLogoUrl string `json:"companyLogoUrl"`
	QuantRank      int    `json:"quant_rank"`
}

type MetricsMeta struct {
	ID         string         `json:"id"`
	Type       string         `json:"type"`
	Attributes map[string]any `json:"attributes"`
}

type MetricRelationshipData struct {
	ID   string `json:"id"`
	Type string `json:"type"`
}

type MetricRelationshipValue struct {
	Data MetricRelationshipData `json:"data"`
}

type MetricRelationship struct {
	MetricType MetricRelationshipValue `json:"metric_type"`
	Ticker     MetricRelationshipValue `json:"ticker"`
}

type MetricItem struct {
	ID            string             `json:"id"`
	Type          string             `json:"type"`
	Attributes    map[string]any     `json:"attributes"`
	Relationships MetricRelationship `json:"relationships"`
}

type MetricsResponse struct {
	Data []MetricItem  `json:"data"`
	Meta []MetricsMeta `json:"included"`
}

func (record *SeekingAlphaRecord) MarshalZerologObject(e *zerolog.Event) {
	e.Str("CompanyName", record.CompanyName)
	e.Str("Ticker", record.Ticker)
	e.Str("CompositeFigi", record.CompositeFigi)
	e.Time("EventDate", record.Date)
	e.Float32("QuantRating", record.QuantRating)
	e.Float64("MarketCapMil", record.MarketCap)
}
