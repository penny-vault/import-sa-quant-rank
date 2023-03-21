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

package common

import (
	"encoding/json"
	"os"
	"strings"

	"github.com/playwright-community/playwright-go"
	"github.com/rs/zerolog/log"
	"github.com/spf13/viper"
)

// StealthPage creates a new playwright page with stealth js loaded to prevent bot detection
func StealthPage(context *playwright.BrowserContext) playwright.Page {
	page, err := (*context).NewPage()
	if err != nil {
		log.Error().Err(err).Msg("could not create page")
	}

	if err = page.AddInitScript(playwright.PageAddInitScriptOptions{
		Script: playwright.String(stealthJS),
	}); err != nil {
		log.Error().Err(err).Msg("could not load stealth mode")
	}

	return page
}

// BuildUserAgent dynamically determines the user agent and removes the headless identifier
func BuildUserAgent(browser *playwright.Browser) string {
	context, err := (*browser).NewContext()
	if err != nil {
		log.Error().Err(err).Msg("could not create context for building user agent")
	}
	defer context.Close()

	page, err := context.NewPage()
	if err != nil {
		log.Error().Err(err).Msg("could not create page BuildUserAgent")
	}

	resp, err := page.Goto("https://playwright.dev", playwright.PageGotoOptions{
		WaitUntil: playwright.WaitUntilStateNetworkidle,
	})
	if err != nil {
		log.Error().Err(err).Str("Url", "https://playwright.dev").Msg("could not load page")
	}

	headers, err := resp.Request().AllHeaders()
	if err != nil {
		log.Error().Err(err).Msg("could not load request headers")
	}

	userAgent := headers["user-agent"]
	log.Info().Str("userAgent", userAgent).Msg("User-Agent discoverd from headers")
	userAgent = strings.Replace(userAgent, "Headless", "", -1)
	return userAgent
}

// StartPlaywright starts the playwright server and browser, it then creates a new context and page with the stealth extensions loaded
func StartPlaywright(headless bool) (page playwright.Page, context playwright.BrowserContext, browser playwright.Browser, pw *playwright.Playwright) {
	pw, err := playwright.Run()
	if err != nil {
		log.Error().Err(err).Msg("could not launch playwright")
	}

	var browserOpts playwright.BrowserTypeLaunchOptions
	proxy := viper.GetString("playwright.proxy")
	if proxy == "" {
		log.Info().Msg("no proxy server used")
		browserOpts = playwright.BrowserTypeLaunchOptions{
			Headless: playwright.Bool(headless),
		}
	} else {
		log.Info().Str("proxy", proxy).Msg("using proxy server")
		browserOpts = playwright.BrowserTypeLaunchOptions{
			Headless: playwright.Bool(headless),
			Proxy: &playwright.BrowserTypeLaunchOptionsProxy{
				Server: playwright.String(proxy),
			},
		}
	}

	browser, err = pw.Chromium.Launch(browserOpts)
	if err != nil {
		log.Fatal().Err(err).Str("exe", pw.Chromium.ExecutablePath()).Msg("could not launch Chromium")
	}

	log.Info().Bool("Headless", headless).Str("ExecutablePath", pw.Chromium.ExecutablePath()).Str("BrowserVersion", browser.Version()).Msg("starting playwright")

	// calculate user-agent
	userAgent := viper.GetString("playwright.user_agent")
	if userAgent == "" {
		userAgent = BuildUserAgent(&browser)
	}
	log.Info().Str("UserAgent", userAgent).Msg("using user-agent")

	// load browser state
	stateFileName := viper.GetString("playwright.state_file")
	log.Info().Str("StateFile", stateFileName).Msg("state location")
	var storageState playwright.BrowserNewContextOptionsStorageState
	data, err := os.ReadFile(stateFileName)
	if err != nil {
		log.Error().Err(err)
	}
	err = json.Unmarshal(data, &storageState)
	if err != nil {
		log.Error().Err(err)
	}

	// create context
	context, err = browser.NewContext(playwright.BrowserNewContextOptions{
		UserAgent:    playwright.String(userAgent),
		StorageState: &storageState,
	})
	if err != nil {
		log.Error().Msg("could not create browser context")
	}

	// get a page
	page = StealthPage(&context)

	return
}

func StopPlaywright(page playwright.Page, context playwright.BrowserContext, browser playwright.Browser, pw *playwright.Playwright) {
	// save session state
	log.Info().Msg("saving state")
	stateFileName := viper.GetString("playwright.state_file")
	storage, err := context.StorageState(stateFileName)
	if err != nil {
		log.Error().Err(err).Msg("could not get storage state")
	}
	log.Info().Int("NumCookies", len(storage.Cookies)).Msg("session state")

	log.Info().Msg("closing context")
	if err := context.Close(); err != nil {
		log.Error().Err(err).Msg("error encountered when closing context")
	}

	log.Info().Msg("closing browser")
	if err := browser.Close(); err != nil {
		log.Error().Err(err).Msg("error encountered when closing browser")
	}

	log.Info().Msg("stopping playwright")
	if err := pw.Stop(); err != nil {
		log.Error().Err(err).Msg("error encountered when stopping playwright")
	}
}
