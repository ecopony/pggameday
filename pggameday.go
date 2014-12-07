package pggameday

import (
	"database/sql"
	"github.com/ecopony/gamedayapi"
	_ "github.com/lib/pq" // PostgreSQL driver
	"log"
)

// ImportPitchesForTeamAndYears saves all pitch data fields for a team and season.
func ImportPitchesForTeamAndYears(teamCode string, years []int) {
	log.Println("Importing for " + teamCode)

	// Assumes a pg database exists named go-gameday, a role that can access it.
	// Assumes a table called pitches with a character column called code.
	db, err := sql.Open("postgres", "user=go-gameday dbname=go-gameday sslmode=disable")
	issue := db.Ping()
	log.Println(issue)
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	fetchFunction := func(game *gamedayapi.Game) {
		log.Println(">>>> " + game.ID + " <<<<")
		for _, inning := range game.AllInnings().Innings {
			for _, atBat := range inning.AtBats() {
				for _, pitch := range atBat.Pitches {
					log.Println("> " + pitch.Des)
					res, err := db.Query("INSERT INTO pitches (code) VALUES ($1)", pitch.ID)
					if err != nil {
						log.Fatal(err)
					}
					res.Close()
				}
			}
		}
	}
	gamedayapi.FetchByTeamAndYears(teamCode, years, fetchFunction)
}
