package sa

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"time"

	"github.com/rs/zerolog/log"
)

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
		record.Exchange != "Grey Market" &&
		record.Exchange != "Pink No Info" &&
		record.Exchange != "Pink Current Info")
}
