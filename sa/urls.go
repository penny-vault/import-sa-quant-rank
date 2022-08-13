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

const (
	HOMEPAGE_URL      string = `https://seekingalpha.com/`
	SCREENER_PAGE_URL string = `https://seekingalpha.com/screeners`
	SCREENER_API_URL  string = `https://seekingalpha.com/api/v3/screener_results`
	METRICS_1_URL     string = `https://seekingalpha.com/api/v3/metrics?filter[fields]=marketcap_display%2Cdividend_yield%2Cquant_rating%2Cauthors_rating%2Csell_side_rating&filter[slugs]=`
	METRICS_2_URL     string = `https://seekingalpha.com/api/v3/ticker_metric_grades?filter[algos][]=etf&filter[algos][]=dividends&filter[algos][]=main_quant&filter[algos][]=reit&filter[algos][]=reit_dividend&filter[fields]=value_category%2Cgrowth_category%2Cprofitability_category%2Cmomentum_category%2Ceps_revisions_category&filter[slugs]=`
	METRICS_3_URL     string = `https://seekingalpha.com/api/v3/metrics?filter[fields]=earning_announce_date%2Ceps_estimate_fy1%2Crevenue_estimate%2Ceps_normalized_actual%2Ceps_surprise%2Crevenue_actual%2Crevenue_surprise&filter[slugs]=`
	METRICS_4_URL     string = `https://seekingalpha.com/api/v3/metrics?filter[fields]=div_growth_category%2Cdiv_safety_category%2Cdiv_yield_category%2Cdiv_consistency_category&filter[slugs]=`
	METRICS_5_URL     string = `https://seekingalpha.com/api/v3/metrics?filter[fields]=last_div_date%2Cdiv_pay_date%2Cdividend_yield%2Cdiv_yield_fwd%2Cdiv_yield_4y%2Cdiv_rate_ttm%2Cdiv_rate_fwd%2Cpayout_ratio%2Cpayout_ratio_4y%2Cdiv_grow_rate3%2Cdiv_grow_rate5%2Cdividend_growth&filter[slugs]=`
	METRICS_6_URL     string = `https://seekingalpha.com/api/v3/metrics?filter[fields]=eps_revisions_category&filter[slugs]=`
	METRICS_7_URL     string = `https://seekingalpha.com/api/v3/metrics?filter[fields]=marketcap_display%2Ctev%2Cpe_ratio%2Cpe_nongaap_fy1%2Cpeg_gaap%2Cpeg_nongaap_fy1%2Cps_ratio%2Cev_12m_sales_ratio%2Cev_ebitda%2Cpb_ratio%2Cprice_cf_ratio&filter[slugs]=`
	METRICS_8_URL     string = `https://seekingalpha.com/api/v3/metrics?filter[fields]=revenue_growth%2Crevenue_change_display%2Crevenue_growth3%2Crevenue_growth5%2Cebitda_yoy%2Cebitda_change_display%2Cebitda_3y_cagr%2Cnet_income_3y_cagr%2Cdiluted_eps_growth%2Ceps_change_display%2Cearnings_growth_3y_cagr%2Ctangible_book_value_3y_cagr%2Ctotal_assets_3y_cagr%2Clevered_free_cash_flow_3y_cagr&filter[slugs]=`
	METRICS_9_URL     string = `https://seekingalpha.com/api/v3/metrics?filter[fields]=total_revenue%2Cnet_income%2Ccash_from_operations_as_reported%2Cgross_margin%2Cebit_margin%2Cebitda_margin%2Cnet_margin%2Clevered_fcf_margin%2Croe%2Creturn_on_avg_tot_assets%2Creturn_on_total_capital%2Cassets_turnover%2Cnet_inc_per_employee%2Ccapex_to_sales&filter[slugs]=`
	METRICS_10_URL    string = `https://seekingalpha.com/api/v3/metrics?filter[fields]=short_interest_percent_of_float%2Clast_closing_shares_short%2Cshort_interest_coverage_ratio%2Cbeta24%2Cbeta60%2Caltman_z_score&filter[slugs]=`
	METRICS_11_URL    string = `https://seekingalpha.com/api/v3/metrics?filter[fields]=shares%2Cfloat_percent%2Cinsiders_shares%2Cinsiders_share_percent%2Cinstitutions_shares%2Cinstitutions_share_percent&filter[slugs]=`
	METRICS_12_URL    string = `https://seekingalpha.com/api/v3/metrics?filter[fields]=total_debt%2Cdebt_short_term%2Cdebt_long_term%2Ctotal_cash%2Cdebt_fcf%2Ccurrent_ratio%2Cquick_ratio%2Cinterest_coverage_ratio%2Cdebt_eq%2Clong_term_debt_per_capital&filter[slugs]=`
)
