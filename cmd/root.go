/*
Copyright 2022

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package cmd

import (
	"fmt"
	"os"

	"github.com/penny-vault/import-sa-quant-rank/backblaze"
	"github.com/penny-vault/import-sa-quant-rank/sa"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"

	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
)

var cfgFile string
var test bool

// rootCmd represents the base command when called without any subcommands
var rootCmd = &cobra.Command{
	Use:   "import-sa-quant-rank",
	Short: "Import JSON ratings downloaded from Seeking Alpha's stock screener",
	// Long: ``,
	Run: func(cmd *cobra.Command, args []string) {
		log.Info().Bool("Test", test).Msg("Download SeekingAlpha ratings")
		ratings := sa.Download()

		if !test {
			sa.EnrichWithFigi(ratings)
			sa.SaveToDB(ratings)
		}

		// Save data as parquet to a temporary directory
		tmpdir, err := os.MkdirTemp(os.TempDir(), "import-sa")
		if err != nil {
			log.Error().Err(err).Msg("could not create tempdir")
		}

		parquetFn := fmt.Sprintf("%s/sa-%s.parquet", tmpdir, ratings[0].Date.Format("20060102"))
		log.Info().Str("FileName", parquetFn).Msg("writing seeking alpha ratings data to parquet")
		sa.SaveToParquet(ratings, parquetFn)

		// Upload to backblaze
		if !test {
			backblaze.UploadToBackBlaze(parquetFn, viper.GetString("backblaze.bucket"), ratings[0].Date.Format("2006"))
		}

		// Cleanup after ourselves
		os.RemoveAll(tmpdir)
	},
}

// Execute adds all child commands to the root command and sets flags appropriately.
// This is called by main.main(). It only needs to happen once to the rootCmd.
func Execute() {
	err := rootCmd.Execute()
	if err != nil {
		log.Error().Msg(err.Error())
		os.Exit(1)
	}
}

func init() {
	cobra.OnInitialize(initConfig)
	cobra.OnInitialize(initLog)

	// Persistent flags that are global to application
	rootCmd.PersistentFlags().StringVar(&cfgFile, "config", "", "config file (default is $HOME/.import-sa-quant-rank.toml)")
	rootCmd.PersistentFlags().Bool("log-json", false, "print logs as json to stderr")
	viper.BindPFlag("log.json", rootCmd.PersistentFlags().Lookup("log-json"))
	rootCmd.PersistentFlags().Bool("hide-progress", false, "hide progress bar")
	viper.BindPFlag("display.hide_progress", rootCmd.PersistentFlags().Lookup("hide-progress"))

	rootCmd.Flags().BoolVarP(&test, "test", "t", false, "run in test mode and do not save results to database or upload to backblaze")

	rootCmd.PersistentFlags().String("state_file", "state.json", "state file")
	viper.BindPFlag("playwright.state_file", rootCmd.PersistentFlags().Lookup("state_file"))

	// Add flags
	rootCmd.Flags().StringP("database_url", "d", "host=localhost port=5432", "DSN for database connection")
	viper.BindPFlag("database.url", rootCmd.Flags().Lookup("database_url"))

	rootCmd.Flags().Uint32P("limit", "l", 0, "limit results to N")
	viper.BindPFlag("limit", rootCmd.Flags().Lookup("limit"))

	rootCmd.Flags().StringP("backblaze_bucket", "b", "seeking-alpha", "Backblaze bucket name")
	viper.BindPFlag("backblaze.bucket", rootCmd.Flags().Lookup("backblaze_bucket"))

	rootCmd.Flags().String("backblaze_application_id", "<not-set>", "Backblaze application id")
	viper.BindPFlag("backblaze.application_id", rootCmd.Flags().Lookup("backblaze_application_id"))

	rootCmd.Flags().String("backblaze_application_key", "<not-set>", "Backblaze application key")
	viper.BindPFlag("backblaze.application_key", rootCmd.Flags().Lookup("backblaze_application_key"))
}

// initConfig reads in config file and ENV variables if set.
func initConfig() {
	if cfgFile != "" {
		// Use config file from the flag.
		viper.SetConfigFile(cfgFile)
	} else {
		// Find home directory.
		home, err := os.UserHomeDir()
		cobra.CheckErr(err)

		// Search config in home directory with name ".import-sa-quant-rank" (without extension).
		viper.AddConfigPath("/etc") // path to look for the config file in
		viper.AddConfigPath(fmt.Sprintf("%s/.config", home))
		viper.AddConfigPath(".")
		viper.SetConfigType("toml")
		viper.SetConfigName("import-sa-quant-rank")
	}

	viper.AutomaticEnv() // read in environment variables that match

	// If a config file is found, read it in.
	if err := viper.ReadInConfig(); err == nil {
		fmt.Fprintln(os.Stderr, "Using config file:", viper.ConfigFileUsed())
	}
}

func initLog() {
	if !viper.GetBool("log.json") {
		log.Logger = log.Output(zerolog.ConsoleWriter{Out: os.Stderr})
	}
}
