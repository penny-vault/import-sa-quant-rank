package saimport

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

	"github.com/xitongsys/parquet-go-source/local"
	"github.com/xitongsys/parquet-go/parquet"
	"github.com/xitongsys/parquet-go/writer"
)

type Ticker struct {
	CompanyName   string
	TickerId      int
	Ticker        string
	CompositeFigi string
}

type SeekingAlphaRecord struct {
	DateStr                      string `json:"date" parquet:"name=Date, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
	Date                         time.Time
	TickerId                     int     `json:"tickerId" parquet:"name=SeekingAlphaTickerId, type=INT32"`
	Ticker                       string  `json:"ticker" parquet:"name=Ticker, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
	CompositeFigi                string  `json:"compositeFigi" parquet:"name=CompositeFigi, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
	Slug                         string  `json:"slug" parquet:"name=Slug, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
	CompanyName                  string  `json:"companyName" parquet:"name=CompanyName, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
	Exchange                     string  `json:"exchange" parquet:"name=Exchange, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
	Type                         string  `json:"type" parquet:"name=Type, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
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

func SaveToDB(records []*SeekingAlphaRecord) {
	conn, err := pgx.Connect(context.Background(), viper.GetString("DATABASE_URL"))
	if err != nil {
		log.Error().Str("OriginalError", err.Error()).Msg("Could not connect to database")
	}
	defer conn.Close(context.Background())

	for _, r := range records {
		conn.Exec(context.Background(),
			`INSERT INTO seeking_alpha_v1 (
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
		) ON CONFLICT ON CONSTRAINT seeking_alpha_v1_pkey
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

func SaveToParquet(records []*SeekingAlphaRecord, fn string) error {
	var err error

	fh, err := local.NewLocalFileWriter(fn)
	if err != nil {
		log.Error().Str("OriginalError", err.Error()).Str("FileName", fn).Msg("cannot create local file")
		return err
	}
	defer fh.Close()

	pw, err := writer.NewParquetWriter(fh, new(SeekingAlphaRecord), 4)
	if err != nil {
		log.Error().
			Str("OriginalError", err.Error()).
			Msg("Parquet write failed")
		return err
	}

	pw.RowGroupSize = 128 * 1024 * 1024 // 128M
	pw.PageSize = 8 * 1024              // 8k
	pw.CompressionType = parquet.CompressionCodec_GZIP

	for _, r := range records {
		if err = pw.Write(r); err != nil {
			log.Error().
				Str("OriginalError", err.Error()).
				Str("EventDate", r.DateStr).Str("Ticker", r.Ticker).
				Str("CompositeFigi", r.CompositeFigi).
				Msg("Parquet write failed for record")
		}
	}

	if err = pw.WriteStop(); err != nil {
		log.Error().Str("OriginalError", err.Error()).Msg("Parquet write failed")
		return err
	}

	log.Info().Int("NumRecords", len(records)).Msg("Parquet write finished")
	return nil
}
