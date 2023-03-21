// Copyright 2022
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package cmd

import (
	"bufio"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/gosimple/slug"
	"github.com/penny-vault/import-sa-quant-rank/common"
	"github.com/playwright-community/playwright-go"
	"github.com/rs/zerolog/log"
	"github.com/spf13/cobra"
)

// testCmd represents the test command
var testCmd = &cobra.Command{
	Use:   "test",
	Short: "enable interactive testing of a webpage",
	Long: `The test command enables interactive debugging of web scraping. It
allows users to query for a selector and view the bounding box coordinates of
the DOM object, issue mouse move / click events, and exit.`,
	Run: func(cmd *cobra.Command, args []string) {
		page, context, browser, pw := common.StartPlaywright(false)

		// load the default homepage
		if _, err := page.Goto("https://bot.incolumitas.com", playwright.PageGotoOptions{
			WaitUntil: playwright.WaitUntilStateNetworkidle,
		}); err != nil {
			log.Error().Err(err).Msg("could not load login page")
		}

		reader := bufio.NewReader(os.Stdin)
		var wizardCmd string
		for wizardCmd != "e" {
			fmt.Println("What do you want to do?")
			fmt.Println("\t[1] Query selector")
			fmt.Println("\t[2] Click Mouse")
			fmt.Println("\t[3] Mouse Down")
			fmt.Println("\t[4] Mouse Up")
			fmt.Println("\t[5] Screenshot Element")
			fmt.Println("\t[e] Exit")

			line, _ := reader.ReadString('\n')
			wizardCmd = strings.Trim(line, " \n")

			switch wizardCmd {
			case "1": // query selector and print bbox
				fmt.Println("Enter selector: ")
				line, _ := reader.ReadString('\n')
				selector := strings.Trim(line, " \n")

				fmt.Printf("Value: %s \n", selector)

				sel, err := page.QuerySelector(selector)
				if err != nil {
					log.Error().Err(err).Msg("failed getting selector")
					continue
				}

				if sel == nil {
					log.Info().Msg("selector not found!")
				} else {
					bbox, err := sel.BoundingBox()
					if err != nil {
						log.Error().Err(err).Msg("failed to get bounding box")
					} else {
						log.Info().Int("X", bbox.X).Int("Y", bbox.Y).Int("Height", bbox.Height).Int("Width", bbox.Width).Msg("bounding box")
					}
				}
			case "2": // click mouse
				var x float64
				var y float64
				var dur float64
				fmt.Println("Enter position [X Y duration]: ")
				fmt.Scan(&x, &y, &dur)

				fmt.Printf("Clicking @ %f, %f for %f milliseconds\n", x, y, dur)
				err := page.Mouse().Move(x, y)
				if err != nil {
					log.Error().Err(err).Msg("mouse move failed")
				}
				err = page.Mouse().Click(x, y, playwright.MouseClickOptions{
					Delay: playwright.Float(dur),
				})
				if err != nil {
					log.Error().Err(err).Msg("mouse click failed")
				}
				fmt.Printf("mouse action complete")
			case "3": // mouse down
				var x float64
				var y float64
				fmt.Println("Enter position [X Y]: ")
				fmt.Scan(&x, &y)

				fmt.Printf("Clicking @ %f, %f\n", x, y)
				err := page.Mouse().Move(x, y)
				if err != nil {
					log.Error().Err(err).Msg("mouse move failed")
				}
				err = page.Mouse().Down()
				if err != nil {
					log.Error().Err(err).Msg("mouse click failed")
				}
			case "4": // mouse up
				err := page.Mouse().Up()
				if err != nil {
					log.Error().Err(err).Msg("mouse click failed")
				}
			case "5": // screenshot element
				fmt.Println("Enter selector: ")
				line, _ := reader.ReadString('\n')
				selector := strings.Trim(line, " \n")

				fn := fmt.Sprintf("%s.png", slug.Make(selector))
				fmt.Printf("Saving to: %s \n", fn)

				start := time.Now()
				sel, err := page.QuerySelector(selector)
				if err != nil {
					log.Error().Err(err).Msg("failed getting selector")
					continue
				}

				if sel == nil {
					log.Info().Msg("selector not found!")
				} else {
					_, err := sel.Screenshot(playwright.ElementHandleScreenshotOptions{
						Path: playwright.String(fn),
					})
					if err != nil {
						log.Error().Err(err).Msg("failed to get screenshot")
						continue
					}
					end := time.Now()
					dur := end.Sub(start)
					fmt.Printf("Screenshot took %d ms\n", dur.Milliseconds())
				}
			case "e":
				log.Info().Msg("exiting...")
			default:
				log.Warn().Msg("unknown command selected")
				continue
			}
		}

		common.StopPlaywright(page, context, browser, pw)
	},
}

func init() {
	rootCmd.AddCommand(testCmd)
}
