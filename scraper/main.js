/**
 * This template is a production ready boilerplate for developing with `PuppeteerCrawler`.
 * Use this to bootstrap your projects using the most up-to-date code.
 * If you're looking for examples or want to learn more, see README.
 */

const Apify = require('apify');

const { utils: { log } } = Apify;

Apify.main(async () => {
    const requestQueue = await Apify.openRequestQueue();
    await requestQueue.addRequest({ url: `https://seekingalpha.com/screeners` });

    // open datasets
    const ratingDataset = await Apify.openDataset('ratings');

    const cookies = [{"name": "machine_cookie", "value": "9248452754069", "url": "https://seekingalpha.com"}, {"name": "__pat", "value": "-14400000", "url": "https://seekingalpha.com"}, {"name": "_ga", "value": "GA1.2.754476173.1648536372", "url": "https://seekingalpha.com"}, {"name": "_gcl_au", "value": "1.1.560084397.1648536372", "url": "https://seekingalpha.com"}, {"name": "prism_25946650", "value": "83c8ded3-7cf6-4531-92b6-97da62c89bfb", "url": "https://seekingalpha.com"}, {"name": "_fbp", "value": "fb.1.1648536372737.1788301924", "url": "https://seekingalpha.com"}, {"name": "_cc_id", "value": "bc6e503ae95cb6a83a5e8f43a995778d", "url": "https://seekingalpha.com"}, {"name": "panoramaId_expiry", "value": "1649141173281", "url": "https://seekingalpha.com"}, {"name": "panoramaId", "value": "9b5eef72692a0fc9d804d6c267f44945a702e3806949bed1adc48e530bdfcd6e", "url": "https://seekingalpha.com"}, {"name": "pxcts", "value": "ecf89af2-af2b-11ec-852b-6567486d614c", "url": "https://seekingalpha.com"}, {"name": "_pxvid", "value": "ecf888d3-af2b-11ec-852b-6567486d614c", "url": "https://seekingalpha.com"}, {"name": "_sasource", "value": "", "url": "https://seekingalpha.com"}, {"name": "has_paid_subscription", "value": "true", "url": "https://seekingalpha.com"}, {"name": "ever_pro", "value": "1", "url": "https://seekingalpha.com"}, {"name": "_hjid", "value": "95a6dd91-c6db-4a15-99d9-a161dc046cba", "url": "https://seekingalpha.com"}, {"name": "_hjSessionUser_65666", "value": "eyJpZCI6ImI4YWVkM2ZlLTQyMTgtNWMxZS05NzhiLWY3OGQzMjM2NGI2MiIsImNyZWF0ZWQiOjE2NDg2MjQwMjUzMzMsImV4aXN0aW5nIjp0cnVlfQ==", "url": "https://seekingalpha.com"}, {"name": "_igt", "value": "77f00412-2a0d-42ee-c05e-da9ca54699b7", "url": "https://seekingalpha.com"}, {"name": "g_state", "value": "{\"i_p\":1648612654594,\"i_l\":1,\"i_t\":1648712520031}", "url": "https://seekingalpha.com"}, {"name": "_ig", "value": "972064", "url": "https://seekingalpha.com"}, {"name": "_gid", "value": "GA1.2.637906036.1648928983", "url": "https://seekingalpha.com"}, {"name": "session_id", "value": "43691b2d-ec10-4afd-a938-5815d2f31fc5", "url": "https://seekingalpha.com"}, {"name": "user_id", "value": "972064", "url": "https://seekingalpha.com"}, {"name": "user_nick", "value": "jdfergason", "url": "https://seekingalpha.com"}, {"name": "user_devices", "value": "1", "url": "https://seekingalpha.com"}, {"name": "u_voc", "value": "45", "url": "https://seekingalpha.com"}, {"name": "marketplace_author_slugs", "value": "", "url": "https://seekingalpha.com"}, {"name": "user_cookie_key", "value": "106u8j9", "url": "https://seekingalpha.com"}, {"name": "user_perm", "value": "", "url": "https://seekingalpha.com"}, {"name": "sapu", "value": "12", "url": "https://seekingalpha.com"}, {"name": "user_remember_token", "value": "2b5c00f8cb5494c28c40d8ed169732d5a574cc5c", "url": "https://seekingalpha.com"}, {"name": "gk_user_access", "value": "1*archived*1648937600", "url": "https://seekingalpha.com"}, {"name": "gk_user_access_sign", "value": "4b59d8431f6153e3658aef3f253ac84328a78e79", "url": "https://seekingalpha.com"}, {"name": "__tbc", "value": "%7Bkpex%7DLPZwm3m2s2hzxtYdnX_SB4VqJtdw5a9Qq5NAdui666M_a6IUTUkiKRqC9DLzcpQDK5dZZ7BHhxNssuCAh3P7TJkbWU6CSWLAlXzamqdjtUUHkiPlKAHN6apBexP9CKRw", "url": "https://seekingalpha.com"}, {"name": "xbc", "value": "%7Bkpex%7DuBbmYndajtBPx4dJ93gELU8GM-M2FSit3IoYwspsbk-yNmPRds7RrFa0l3oVMvPdYj3PcTifefTwC_dtybQlg5ZEGl4Ks0b06o3EvYnvdDGHjy3_UCIuRwMGuL3stj7OWWonW-vGMV7E-9Swkg9WkjxDkizNbh4T0vRw8L91mJXYoz3VB3OLSDiJQcl-eHvuUjE5Sx4MVy7A-BUC7-85saziex8podoT_I96bl4A7lBi5Qn23y-bmmgk_JtIGg_Un0G9bzjb9VlcKrBrYRmv45W4011dY-Py0CiF4rrMNdt4Xa4QndpVVwtLwnizCzpuPxtS355wXz13AxD3x-qVJFz8zrsJtLDqwfJzwpkwrojGWQj_Jwh6GyRIhprDaBYH", "url": "https://seekingalpha.com"}, {"name": "sailthru_hid", "value": "6fa7629ea2404e501926089baa3ad17760afbb6d418457284b01bc576e13db356209a2350b578beb278ba815", "url": "https://seekingalpha.com"}, {"name": "__tac", "value": "", "url": "https://seekingalpha.com"}, {"name": "__tae", "value": "1648937602286", "url": "https://seekingalpha.com"}, {"name": "_clck", "value": "mv16w6|1|f0b|0", "url": "https://seekingalpha.com"}, {"name": "_hjCachedUserAttributes", "value": "eyJhdHRyaWJ1dGVzIjp7ImxvZ2dlZF9pbiI6dHJ1ZSwibXBfc3ViIjp0cnVlLCJwcmVtaXVtX3N1YiI6dHJ1ZSwicHJvX3N1YiI6ZmFsc2V9LCJ1c2VySWQiOiI5NzIwNjQifQ==", "url": "https://seekingalpha.com"}, {"name": "_hjIncludedInSessionSample", "value": "1", "url": "https://seekingalpha.com"}, {"name": "sailthru_pageviews", "value": "1", "url": "https://seekingalpha.com"}, {"name": "_uetsid", "value": "0a65be60b2be11ec9ae0692073b2efa2", "url": "https://seekingalpha.com"}, {"name": "_uetvid", "value": "0db955b0dbb711eb8c2dd1129a026645", "url": "https://seekingalpha.com"}, {"name": "sailthru_content", "value": "9e32a6735f36b7454f5b8ac011f3fb4d6d033c8f7ec9accb700e427f77fffe9e6f639d5d74530074897446b297fe6d7e3c5f57e561d9315a8a9a86be8720511eee5d0b4f780b6a4e452ca0c28ea3100a32d5dea8c5b7589b9534999488faf750a6868281678e0269f2e30c72a59520f839783b4c739ccb552aa4da330375438331583f9d2bd94273afc4170de664316d", "url": "https://seekingalpha.com"}, {"name": "sailthru_visitor", "value": "78d5a270-d324-4d63-8c5e-15aad1c40490", "url": "https://seekingalpha.com"}, {"name": "_hjSession_65666", "value": "eyJpZCI6ImYyYjdjYjk5LWRjNmItNGUyNS04MjJiLTNmYTRiNmUxYzRjNSIsImNyZWF0ZWQiOjE2NDkwMTAyMDgxMDEsImluU2FtcGxlIjp0cnVlfQ==", "url": "https://seekingalpha.com"}, {"name": "_hjAbsoluteSessionInProgress", "value": "1", "url": "https://seekingalpha.com"}, {"name": "_px2", "value": "eyJ1IjoiMjhhODcyNDAtYjM3Yi0xMWVjLWIyNzUtNzc4MWQ3MzFhODQxIiwidiI6ImVjZjg4OGQzLWFmMmItMTFlYy04NTJiLTY1Njc0ODZkNjE0YyIsInQiOjE1NjE1MDcyMDAwMDAsImgiOiI5NGZlZGUyYjY1ZTg4OWUxM2YzYjc4MTI4NWM3ZjAwM2U5NjljNWRmOTE1ZDNiOGFkYjhiZmM2ZWNkYjQ2ZGVkIn0=", "url": "https://seekingalpha.com"}, {"name": "_px", "value": "LBM3tXOF2Rb430Iweja0bT4nCijVwo4Ja86a0xGTfCwfM8gxcy5tkUDb5aLN3WEHfnJO7Gw6p65EjDebfZgo0w==:1000:99kJN+xCOMMq4KH4LMiIqoW64ILx2UCIsPcTmdZHo3Hg4OsnNUp5FBL+Z71UVZtWFZQZqhfvB3QRFlojn4wnp5IHccuYs9MrusXE0XEJlST3T87YUEId9mhPUxyy5T3p4nFuKtuXsUiyiAgPm2Snz+pfxB7LZbRaBGV0FHww6HMdw5U1QmYG2ncwOK28oYJJaArROxOJ1rfYLFISacCfES8mtltIYHq3yUF+UPpORTjfzXHJLlAFxVV/6iVsTA8mNEdBzexsVpY0xDWxiOsaoA==", "url": "https://seekingalpha.com"}, {"name": "_hjUserAttributesHash", "value": "2f526d62d6cf5e1847be44315d51b5d0", "url": "https://seekingalpha.com"}, {"name": "_gat_UA-142576245-4", "value": "1", "url": "https://seekingalpha.com"}, {"name": "LAST_VISITED_PAGE", "value": "%7B%22pathname%22%3A%22https%3A%2F%2Fseekingalpha.com%2Fsymbol%2FBRK.B%2Fratings%2Fquant-ratings%22%2C%22pageKey%22%3A%22787805b3-515e-4124-a2b3-284815cc7aa3%22%7D", "url": "https://seekingalpha.com"}, {"name": "__pvi", "value": "%7B%22id%22%3A%22v-2022-04-03-11-23-26-555-qOIFhNNDgjubOxms-5c6c1b810cd8e25c1d11a2ebe7ee0b7e%22%2C%22domain%22%3A%22.seekingalpha.com%22%2C%22time%22%3A1649011528726%7D", "url": "https://seekingalpha.com"}, {"name": "_clsk", "value": "184s7th|1649011528819|8|0|e.clarity.ms/collect", "url": "https://seekingalpha.com"}, {"name": "_pxde", "value": "e83a4d69f50c862091da272e01c8ba4ddae9c6692c5f131e6345b1c03ed2e96d:eyJ0aW1lc3RhbXAiOjE2NDkwMTE1Mjk1MDIsImZfa2IiOjB9", "url": "https://seekingalpha.com"}];

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
