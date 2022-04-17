// Download quantitative ratings from Seeking Alpha
const Apify = require('apify');

const { utils: { log } } = Apify;

Apify.main(async () => {
    const requestQueue = await Apify.openRequestQueue();
    await requestQueue.addRequest({ url: `https://seekingalpha.com/screeners` });

    // open datasets
    const ratingDataset = await Apify.openDataset('ratings');

    const cookies = JSON.parse(process.env['COOKIES'])

    // const proxyConfiguration = await Apify.createProxyConfiguration();

    const crawler = new Apify.PuppeteerCrawler({
        requestQueue,
        // proxyConfiguration,
        launchContext: {
            // Chrome with stealth should work for most websites.
            // If it doesn't, feel free to remove this.
            useChrome: true,
            stealth: true,
        },
        persistCookiesPerSession: true,
        useSessionPool: true,
        maxConcurrency: 1,
        handlePageTimeoutSecs: 600,
        // This function will be called for each URL to crawl.
        // Here you can write the Puppeteer scripts you are familiar with,
        // with the exception that browsers and pages are automatically managed by the Apify SDK.
        // The function accepts a single parameter, which is an object with the following fields:
        // - request: an instance of the Request class with information such as URL and HTTP method
        // - page: Puppeteer's Page object (see https://pptr.dev/#show=api-class-page)
        handlePageFunction: async ({ request, page }) => {
            const today = new Date();
            console.log(`Running screen on ${today}...`);

            await Apify.utils.puppeteer.blockRequests(page, {
                urlPatterns: ['collect', 'beacon', 'collector', 'mone', 'mone_event'],
            });

            let pageNum = 1;
            let numPages = 2;
            let screenArguments = {
                "filter": {
                    "quant_rating": {
                        "gte": 1,
                        "lte": 5,
                        "disabled": false
                    },
                    "authors_rating_pro": {
                        "gte": 1,
                        "lte": 5,
                        "disabled": true
                    },
                    "sell_side_rating": {
                        "gte": 1,
                        "lte": 5,
                        "disabled": true
                    }
                },
                "page": pageNum,
                "per_page":100
            }

            const screenerUrl = 'https://seekingalpha.com/api/v3/screener_results?quant_rank=true';
            const metricsUrls = [
                'https://seekingalpha.com/api/v3/metrics?filter[fields]=marketcap_display%2Cdividend_yield%2Cquant_rating%2Cauthors_rating_pro%2Csell_side_rating&filter[slugs]=',
                'https://seekingalpha.com/api/v3/metrics?filter[fields]=value_category%2Cgrowth_category%2Cprofitability_category%2Cmomentum_category%2Ceps_revisions_category&filter[slugs]=',
                'https://seekingalpha.com/api/v3/metrics?filter[fields]=earning_announce_date%2Ceps_estimate_fy1%2Crevenue_estimate%2Ceps_normalized_actual%2Ceps_surprise%2Crevenue_actual%2Crevenue_surprise&filter[slugs]=',
                'https://seekingalpha.com/api/v3/metrics?filter[fields]=div_growth_category%2Cdiv_safety_category%2Cdiv_yield_category%2Cdiv_consistency_category&filter[slugs]=',
                'https://seekingalpha.com/api/v3/metrics?filter[fields]=last_div_date%2Cdiv_pay_date%2Cdividend_yield%2Cdiv_yield_fwd%2Cdiv_yield_4y%2Cdiv_rate_ttm%2Cdiv_rate_fwd%2Cpayout_ratio%2Cpayout_ratio_4y%2Cdiv_grow_rate3%2Cdiv_grow_rate5%2Cdividend_growth&filter[slugs]=',
                'https://seekingalpha.com/api/v3/metrics?filter[fields]=eps_revisions_category&filter[slugs]=',
                'https://seekingalpha.com/api/v3/metrics?filter[fields]=marketcap_display%2Ctev%2Cpe_ratio%2Cpe_nongaap_fy1%2Cpeg_gaap%2Cpeg_nongaap_fy1%2Cps_ratio%2Cev_12m_sales_ratio%2Cev_ebitda%2Cpb_ratio%2Cprice_cf_ratio&filter[slugs]=',
                'https://seekingalpha.com/api/v3/metrics?filter[fields]=revenue_growth%2Crevenue_change_display%2Crevenue_growth3%2Crevenue_growth5%2Cebitda_yoy%2Cebitda_change_display%2Cebitda_3y_cagr%2Cnet_income_3y_cagr%2Cdiluted_eps_growth%2Ceps_change_display%2Cearnings_growth_3y_cagr%2Ctangible_book_value_3y_cagr%2Ctotal_assets_3y_cagr%2Clevered_free_cash_flow_3y_cagr&filter[slugs]=',
                'https://seekingalpha.com/api/v3/metrics?filter[fields]=total_revenue%2Cnet_income%2Ccash_from_operations_as_reported%2Cgross_margin%2Cebit_margin%2Cebitda_margin%2Cnet_margin%2Clevered_fcf_margin%2Croe%2Creturn_on_avg_tot_assets%2Creturn_on_total_capital%2Cassets_turnover%2Cnet_inc_per_employee%2Ccapex_to_sales&filter[slugs]=',
                'https://seekingalpha.com/api/v3/metrics?filter[fields]=short_interest_percent_of_float%2Clast_closing_shares_short%2Cshort_interest_coverage_ratio%2Cbeta24%2Cbeta60%2Caltman_z_score&filter[slugs]=',
                'https://seekingalpha.com/api/v3/metrics?filter[fields]=shares%2Cfloat_percent%2Cinsiders_shares%2Cinsiders_share_percent%2Cinstitutions_shares%2Cinstitutions_share_percent&filter[slugs]=',
                'https://seekingalpha.com/api/v3/metrics?filter[fields]=total_debt%2Cdebt_short_term%2Cdebt_long_term%2Ctotal_cash%2Cdebt_fcf%2Ccurrent_ratio%2Cquick_ratio%2Cinterest_coverage_ratio%2Cdebt_eq%2Clong_term_debt_per_capital&filter[slugs]=',
            ];

            // load the screener page to prevent the "are you a human?" test
            const title = await page.title();

            // iteratively execute screen and fetch metrics
            for (;pageNum < numPages; pageNum++) {
                // wait 1 second between each load so as not to overload the server
                await page.waitForTimeout(1000);

                screenArguments.page = pageNum;

                console.log(`Screen page: ${pageNum}`);

                //let screenerUrlPage = `${screenerUrl}&page=${pageNum}`
                await page.evaluate((url, params) => {
                    console.log(params);
                    fetch(url, {
                        method: "POST",
                        cache: 'no-cache',
                        headers: {
                            'Content-Type': 'application/json'
                        },
                        body: JSON.stringify(params),
                    });
                }, screenerUrl, screenArguments);

                let resp = await page.waitForResponse(screenerUrl, (response) => {
                    return response;
                });

                let status = await resp.status();
                console.log(`Screener Status: ${status}`);

                let tickerData = {};
                if (status === 200) {
                    tickerData = await resp.json();
                } else {
                    process.exit(11);
                }

                let tickers = [];
                tickerData.data.forEach((ticker) => {
                    tickers.push(ticker.attributes.slug);
                });

                console.log(tickers)

                numPages = Math.ceil(tickerData.meta.count / 100);

                console.log(`Count = ${tickerData.meta.count}; per page = 100; Expected pages: ${numPages}`);

                // fetch metrics
                let consolidatedMetrics = new Map();

                for (var ii=0; ii < metricsUrls.length; ii++) {
                    const metricsUrl = metricsUrls[ii];
                    await page.waitForTimeout(150);
                    const myUrl = metricsUrl + encodeURIComponent(tickers.join());
                    console.log(`Loading metrics: ${myUrl}`);
                    await page.evaluate((url, params) => {
                        fetch(url);
                    }, myUrl);

                    let resp = await page.waitForResponse(myUrl, (response) => {
                        return response;
                    });

                    let status = await resp.status();
                    console.log(`Metrics status: ${status}`);

                    let metricsResult = {};
                    if (status === 200) {
                        metricsResult = await resp.json();

                        // parse out the meta-data
                        let metricTickers = new Map();
                        let metricTypes = new Map();

                        metricsResult.included.forEach((item) => {
                            switch(item.type) {
                                case 'ticker':
                                    metricTickers.set(item.id, item.attributes);
                                    break;
                                case 'metric_type':
                                    metricTypes.set(item.id, item.attributes.field);
                                    break;
                                default:
                                    console.log(`Unknown meta-data type '${item.type}' skipping...`);
                            }
                        });

                        console.log(metricTypes);

                        // now parse the returned data into the consolidated metrics structure
                        metricsResult.data.forEach((item) => {
                            switch(item.type) {
                                case 'metric':
                                    let tickerId = item.relationships.ticker.data.id;
                                    let metricId = item.relationships.metric_type.data.id;
                                    let metricBundle = consolidatedMetrics.get(tickerId);
                                    if (metricBundle === undefined) {
                                        tickerData = metricTickers.get(tickerId);
                                        metricBundle = {
                                            date: today.toISOString().split('T')[0],
                                            tickerId: parseInt(tickerId),
                                            ticker: tickerData.name,
                                            slug: tickerData.slug,
                                            companyName: tickerData.companyName,
                                            exchange: tickerData.exchange,
                                            type: tickerData.equityType,
                                        };
                                    }
                                    metricName = metricTypes.get(metricId);
                                    if (metricName === undefined) {
                                        console.log("couldn't find metric");
                                        process.exit(8);
                                    }

                                    let val = item.attributes.value;
                                    let grade = item.attributes.grade;
                                    let meaningful = item.attributes.meaningful;
                                    if (meaningful && val !== null) {
                                        metricBundle[metricName] = val;
                                    } else if (meaningful && grade !== null) {
                                        metricBundle[metricName] = grade;
                                    } else {
                                        metricBundle[metricName] = null;
                                    }
                                    consolidatedMetrics.set(tickerId, metricBundle);
                                    break;
                                default:
                                    console.log(`Unknown data type '${item.type}' skipping...`);
                            }
                        });
                    } else {
                        process.exit(9);
                    }
                }

                consolidatedMetrics.forEach((value) => {
                    ratingDataset.pushData(value);
                });
            }
        },

        // This function is called if the page processing failed more than maxRequestRetries+1 times.
        handleFailedRequestFunction: async ({ request }) => {
            console.log(`Request ${request.url} failed too many times.`);
        },

        preNavigationHooks: [
            async (crawlingContext, gotoOptions) => {
                const { page } = crawlingContext;
                if (cookies && cookies.length) {
                    await page.setCookie(...cookies);
                }
            },
        ],
    });

    log.info('Starting the crawl.');
    await crawler.run();
    log.info('Crawl finished.');
});
