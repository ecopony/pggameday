package pggameday

import (
	"github.com/ecopony/gamedayapi"
	"log"
)

// ImportPitchesForTeamAndYears saves all pitch data fields for a team and season.
func ImportPitchesForTeamAndYears(teamCode string, years []int) {
	log.Println("Importing for " + teamCode)
	gamedayapi.FetchByTeamAndYears(teamCode, years, importAllPitches)
}

func importAllPitches(game *gamedayapi.Game) {
	log.Println(">>>> " + game.ID + " <<<<")
	for _, inning := range game.AllInnings().Innings {
		for _, atBat := range inning.AtBats() {
			for _, pitch := range atBat.Pitches {
				log.Println("> " + pitch.Des)
			}
		}
	}
}
