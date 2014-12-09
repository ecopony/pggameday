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

	db.Exec("DROP TABLE IF EXISTS pitches")
	db.Exec(`CREATE TABLE pitches (pitchid SERIAL PRIMARY KEY, game_id varchar(40), year int, inning int, half varchar(6),
	at_bat_num int, at_bat_b int, at_bat_s int, at_bat_o int, at_bat_start_tfs int, batter int, stand char(1), b_height
	varchar(4), pitcher int, p_throws char(1), at_bat_des varchar(400), at_bat_event varchar(20), pitch_des varchar(40),
	pitch_id int, pitch_type char(2), type_confidence DECIMAL(4, 3), pitch_tfs int, pitch_x DECIMAL(5, 2), pitch_y
	DECIMAL(5, 2), pitch_sv_id varchar(40), pitch_start_speed DECIMAL(4, 1), pitch_end_speed DECIMAL(4, 1), sz_top
	DECIMAL(3, 2), sz_bottom DECIMAL(3, 2), pfx_x DECIMAL(4, 2), pfx_z DECIMAL(4, 2), px DECIMAL(4, 3), pz
	DECIMAL(4, 3), x0 DECIMAL(5, 3), y0 DECIMAL(5, 3), z0 DECIMAL(5, 3), vx0 DECIMAL(4, 2), vy0 DECIMAL(6, 3), vz0
	DECIMAL(5, 3), ax DECIMAL(5, 3), ay DECIMAL(5, 3), az DECIMAL(5, 3), break_y DECIMAL(3, 1), break_angle
	DECIMAL(4, 1), break_length DECIMAL(3, 1), zone int, spin_dir DECIMAL(6, 3), spin_rate DECIMAL(7, 3))`)

	fetchFunction := func(game *gamedayapi.Game) {
		log.Println(">>>> " + game.ID + " <<<<")
		for _, inning := range game.AllInnings().Innings {
			half := "top"
			for _, atBat := range inning.AtBats() {
				if atBat.O == "3" {
					half = "bottom"
				}
				for _, pitch := range atBat.Pitches {
					log.Println("> " + atBat.StartTFS)

					res, err := db.Query(`INSERT INTO pitches
						(game_id, year, inning, half, at_bat_num, at_bat_b, at_bat_s, at_bat_o, at_bat_start_tfs,
						batter, stand, b_height, pitcher, p_throws, at_bat_des, at_bat_event,
						pitch_des, pitch_id, pitch_type, type_confidence, pitch_tfs
						)
						VALUES
						($1, $2, $3, $4, $5, $6, $7, $8, $9,
						$10, $11, $12, $13, $14, $15, $16,
						$17, $18, $19, $20, $21)`,
						game.ID, game.Year(), inning.Num, half, atBat.Num, atBat.B, atBat.S, atBat.O, nullableString(atBat.StartTFS),
						atBat.Batter, atBat.Stand, atBat.BHeight, atBat.Pitcher, atBat.PThrows, atBat.Des, atBat.Event,
						pitch.Des, pitch.ID, pitch.Type, nullableString(pitch.TypeConfidence), nullableString(pitch.TFS))
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

func nullableString(value string) interface {} {
	if value == "" {
		return nil
	}
	return value
}
