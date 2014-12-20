package pggameday

import (
	"database/sql"
	"github.com/ecopony/gamedayapi"
	_ "github.com/lib/pq" // PostgreSQL driver
	"log"
)

// CreateTables creates the database tables used by the importer. For now it's just the pitches table.
// This will drop tables if they already exist.
func CreateTables() {
	log.Println("Creating database tables.")

	// Assumes a pg database exists named go-gameday, a role that can access it.
	db, err := sql.Open("postgres", "user=go-gameday dbname=go-gameday sslmode=disable")
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	log.Println("\t-Creating pitches table")
	db.Exec("DROP INDEX IF EXISTS pitches_game_id")
	db.Exec("DROP TABLE IF EXISTS pitches")
	db.Exec(`CREATE TABLE pitches (pitchid SERIAL PRIMARY KEY, game_id varchar(40), year int, inning int, half varchar(6),
		at_bat_num int, at_bat_b int, at_bat_s int, at_bat_o int, at_bat_start_tfs int, batter int, stand char(1), b_height
		varchar(4), pitcher int, p_throws char(1), at_bat_des varchar(400), at_bat_event varchar(20), pitch_des varchar(40),
		pitch_id int, pitch_type char(1), pitch_pitch_type char(2), type_confidence DECIMAL(4, 3), pitch_tfs int,
	  	pitch_x DECIMAL(5, 2), pitch_y DECIMAL(5, 2), pitch_sv_id varchar(40), pitch_start_speed DECIMAL(4, 1),
	  	pitch_end_speed DECIMAL(4, 1), sz_top DECIMAL(3, 2), sz_bottom DECIMAL(3, 2), pfx_x DECIMAL(4, 2), pfx_z
	  	DECIMAL(4, 2), px DECIMAL(4, 3), pz DECIMAL(4, 3), x0 DECIMAL(5, 3), y0 DECIMAL(5, 3), z0 DECIMAL(5, 3), vx0
	  	DECIMAL(4, 2), vy0 DECIMAL(6, 3), vz0 DECIMAL(5, 3), ax DECIMAL(5, 3), ay DECIMAL(5, 3), az DECIMAL(5, 3), break_y
	  	DECIMAL(3, 1), break_angle DECIMAL(4, 1), break_length DECIMAL(3, 1), zone int, spin_dir DECIMAL(6, 3), spin_rate
	  	DECIMAL(7, 3), nasty int)`)
	log.Println("\t-Creating pitches index")
	db.Exec("CREATE INDEX pitches_game_id ON pitches (game_id)")

	log.Println("\t-Creating players table")
	db.Exec("DROP INDEX IF EXISTS players_id")
	db.Exec("DROP TABLE IF EXISTS players")
	db.Exec(`CREATE TABLE players (playerid SERIAL PRIMARY KEY, id int, first varchar(40), last varchar(40), num int,
		boxname varchar(40), rl varchar(1), bats varchar(1), position varchar(2), current_position varchar(2), status
		varchar(1), team_abbrev varchar(3), team_id int, parent_team_abbrev varchar(3), parent_team_id int, bat_order int,
		game_position varchar(2), avg DECIMAL(4, 3), rbi int, wins int, losses int, era DECIMAL(5, 2)`)
	log.Println("\t-Creating players index")
	db.Exec("CREATE INDEX players_id ON players (id)")
	
	log.Println("Done.")
}

// ImportPitchesForTeamAndYears saves all pitch data fields for a team and season.
func ImportPitchesForTeamAndYears(teamCode string, years []int) {
	log.Println("Importing pitches for " + teamCode)

	// Assumes a pg database exists named go-gameday, a role that can access it.
	db, err := sql.Open("postgres", "user=go-gameday dbname=go-gameday sslmode=disable")
	issue := db.Ping()
	log.Println(issue)
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	fetchFunction := func(game *gamedayapi.Game) {
		log.Println(">>>> " + game.ID + " <<<<")

		var pitchCount int
		err := db.QueryRow("SELECT count(*) FROM pitches WHERE game_id = $1", game.ID).Scan(&pitchCount)
		if err != nil {
			log.Fatal(err)
		}
		if pitchCount > 0 {
			return
		}

		for _, inning := range game.AllInnings().Innings {
			half := "top"
			for _, atBat := range inning.AtBats() {
				if atBat.O == "3" {
					half = "bottom"
				}
				for _, pitch := range atBat.Pitches {
					// log.Println("> " + pitch.SzTop)

					res, err := db.Query(`INSERT INTO pitches
						(game_id, year, inning, half, at_bat_num, at_bat_b, at_bat_s, at_bat_o, at_bat_start_tfs,
						batter, stand, b_height, pitcher, p_throws, at_bat_des, at_bat_event,
						pitch_des, pitch_id, pitch_type, pitch_pitch_type, type_confidence,
						pitch_tfs, pitch_x, pitch_y, pitch_sv_id, pitch_start_speed, pitch_end_speed,
						sz_top, sz_bottom, pfx_x, pfx_z,
						px, pz, x0, y0,
						z0, vx0, vy0, vz0,
						ax, ay, az, break_y,
						break_angle, break_length, zone,
						spin_dir, spin_rate, nasty
						)
						VALUES
						($1, $2, $3, $4, $5, $6, $7, $8, $9,
						$10, $11, $12, $13, $14, $15, $16,
						$17, $18, $19, $20,
						$21, $22, $23, $24, $25, $26,
						$27, $28, $29, $30,
						$31, $32, $33, $34,
						$35, $36, $37, $38,
						$39, $40, $41, $42,
						$43, $44, $45,
						$46, $47, $48, $49)`,
						game.ID, game.Year(), inning.Num, half, atBat.Num, atBat.B, atBat.S, atBat.O, nullableString(atBat.StartTFS),
						atBat.Batter, atBat.Stand, atBat.BHeight, atBat.Pitcher, atBat.PThrows, atBat.Des, atBat.Event,
						pitch.Des, pitch.ID, pitch.Type, nullableString(pitch.PitchType), nullableString(pitch.TypeConfidence),
						nullableString(pitch.TFS), pitch.X, pitch.Y, pitch.SvID, nullableString(pitch.StartSpeed), nullableString(pitch.EndSpeed),
						nullableString(pitch.SzTop), nullableString(pitch.SzBottom), nullableString(pitch.PFXX), nullableString(pitch.PFXZ),
						nullableString(pitch.PX), nullableString(pitch.PZ), nullableString(pitch.X0), nullableString(pitch.Y0),
						nullableString(pitch.Z0), nullableString(pitch.VX0), nullableString(pitch.VY0), nullableString(pitch.VZ0),
						nullableString(pitch.AX), nullableString(pitch.AY), nullableString(pitch.AZ), nullableString(pitch.BreakY),
						nullableString(pitch.BreakAngle), nullableString(pitch.BreakLength), nullableString(pitch.Zone),
						nullableString(pitch.SpinDir), nullableString(pitch.SpinRate), nullableString(pitch.Nasty))
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

func nullableString(value string) interface{} {
	if value == "" {
		return nil
	}
	return value
}
