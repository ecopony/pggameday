package pggameday

import (
	"database/sql"
	"github.com/ecopony/gamedayapi"
	_ "github.com/lib/pq" // PostgreSQL driver
	"log"
	"strconv"
	s "strings"
)

// CreateTables creates the database tables used by the importer. For now it's just the pitches table.
// This will drop tables if they already exist.
func CreateTables() {
	log.Println("Creating database tables")
	CreatePitchesTable()
	CreatePlayersTable()
	CreateHitsTable()
	CreateGameStatsTable()
	log.Println("Done")
}

// CreatePitchesTable creates the pitches table and associated indexes. This will drop things if they already exist, causing
// data loss.
func CreatePitchesTable() {
	// Assumes a pg database exists named go-gameday, a role that can access it.
	db, err := sql.Open("postgres", "user=go-gameday dbname=go-gameday sslmode=disable")
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	log.Println("Creating pitches table")
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
	log.Println("Creating pitches index")
	db.Exec("CREATE INDEX pitches_game_id ON pitches (game_id)")
	db.Exec("CREATE UNIQUE INDEX pitches_game_id_pitch_id ON pitches (game_id, pitch_id)")
	log.Println("Done with pitchers")
}

// CreatePlayersTable creates the players table and associated indexes. This will drop things if they already exist, causing
// data loss.
func CreatePlayersTable() {
	// Assumes a pg database exists named go-gameday, a role that can access it.
	db, err := sql.Open("postgres", "user=go-gameday dbname=go-gameday sslmode=disable")
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	log.Println("Creating players table")
	db.Exec("DROP INDEX IF EXISTS players_id")
	db.Exec("DROP TABLE IF EXISTS players")
	db.Exec(`CREATE TABLE players (playerid SERIAL PRIMARY KEY, year int, id int, first varchar(40), last varchar(40), num int,
		boxname varchar(40), rl varchar(1), bats varchar(1), position varchar(2), current_position varchar(2), status
		varchar(1), team_abbrev varchar(3), team_id int, parent_team_abbrev varchar(3), parent_team_id int, bat_order int,
		game_position varchar(2), avg DECIMAL(4, 3), hr int, rbi int, wins int, losses int, era DECIMAL(5, 2))`)
	log.Println("Creating players index")
	db.Exec("CREATE UNIQUE INDEX players_year_id_team_abbrev ON players (year, id, team_abbrev)")
	log.Println("Done with players")
}

// CreateHitsTable creates the hits table and associated indexes. This will drop things if they already exist, causing
// data loss.
func CreateHitsTable() {
	// Assumes a pg database exists named go-gameday, a role that can access it.
	db, err := sql.Open("postgres", "user=go-gameday dbname=go-gameday sslmode=disable")
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	log.Println("Creating hits table")
	db.Exec("DROP INDEX IF EXISTS unique_hits")
	db.Exec("DROP TABLE IF EXISTS hits")
	db.Exec(`CREATE TABLE hits (hitid SERIAL PRIMARY KEY, game_id varchar(40), year int, des varchar(400), x DECIMAL(5, 2),
		y DECIMAL(5, 2), batter int, pitcher int, type varchar(1), team varchar(1), inning int)`)
	log.Println("Creating hits index")
	db.Exec("CREATE UNIQUE INDEX unique_hits ON hits (game_id, x, y, batter, inning)")
	log.Println("Done with hits")
}

// CreateGameStatsTable creates the hits table and associated indexes. This will drop things if they already exist,
// causing data loss.
func CreateGameStatsTable() {
	db, err := sql.Open("postgres", "user=go-gameday dbname=go-gameday sslmode=disable")
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	log.Println("Creating game_stats table")
	db.Exec("DROP TABLE IF EXISTS game_stats")
	db.Exec(`CREATE TABLE game_stats (gamestatid SERIAL PRIMARY KEY, game_id varchar(40), year int, team_code varchar(3), walk_off_loss boolean)`)
	log.Println("Done with game_stats")
}

// ImportGameStatsForTeamAndYears saves game stats fields for a team and season in the game_stats table.
func ImportGameStatsForTeamAndYears(teamCode string, years []int) {
	// Assumes a pg database exists named go-gameday, a role that can access it.
	db, err := sql.Open("postgres", "user=go-gameday dbname=go-gameday sslmode=disable")
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	for _, year := range years {
		log.Println("Importing game stats for " + strconv.Itoa(year))
	}

	fetchFunction := func(game *gamedayapi.Game) {
		gameStats := GameStatsFor(teamCode, game)

		res, err := db.Query(`INSERT INTO game_stats
						(game_id, year, team_code, walk_off_loss)
						VALUES
						($1, $2, $3, $4)`,
			game.ID, game.Year(), gameStats.StatsTeamCode, gameStats.WalkOffLoss)

		if err != nil {
			log.Fatal(err)
		} else {
			res.Close()
		}
	}

	gamedayapi.FetchByTeamAndYears(teamCode, years, fetchFunction)
}

// ImportHitsForTeamAndYears saves all hit data fields for a team and season in the hits table.
func ImportHitsForTeamAndYears(teamCode string, years []int) {
	log.Println("Importing hits for " + teamCode)

	// Assumes a pg database exists named go-gameday, a role that can access it.
	db, err := sql.Open("postgres", "user=go-gameday dbname=go-gameday sslmode=disable")
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	fetchFunction := func(game *gamedayapi.Game) {
		log.Println(">>>> " + game.ID + " <<<<")

		var hitCount int
		err := db.QueryRow("SELECT count(*) FROM hits WHERE game_id = $1", game.ID).Scan(&hitCount)
		if err != nil {
			log.Fatal(err)
		}
		if hitCount > 0 {
			return
		}

		hips := game.HitChart().Hips
		for _, hit := range hips {
			res, err := db.Query(`INSERT INTO hits
						(game_id, year, des, x, y, batter, pitcher, type, team, inning)
						VALUES
						($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)`,
				game.ID, game.Year(), hit.Des, hit.X, hit.Y, hit.Batter, hit.Pitcher, hit.Type, hit.Team, hit.Inning)

			if err != nil {
				if !s.Contains(err.Error(), "duplicate key") {
					log.Fatal(err)
				}
			} else {
				res.Close()
			}
		}
	}

	gamedayapi.FetchByTeamAndYears(teamCode, years, fetchFunction)
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
						if !s.Contains(err.Error(), "duplicate key") {
							log.Fatal(err)
						}
					} else {
						res.Close()
					}
				}
			}
		}
	}
	gamedayapi.FetchByTeamAndYears(teamCode, years, fetchFunction)
}

// ImportPlayersForTeamAndYears saves all player data fields for a team and season.
func ImportPlayersForTeamAndYears(teamCode string, years []int) {
	log.Println("Importing players for " + teamCode)

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
		var teamID string

		if game.HomeCode == teamCode {
			teamID = game.HomeTeamID
		} else {
			teamID = game.AwayTeamID
		}

		for _, team := range game.Players().Teams {
			for _, player := range team.Players {
				if player.TeamID == teamID {
					res, err := db.Query(`INSERT INTO players
					(year, id, first, last, num, boxname, rl, bats,
					position, current_position, status, team_abbrev, team_id,
					parent_team_abbrev, parent_team_id, bat_order, game_position, avg,
					hr, rbi, wins, losses, era)
					VALUES
					($1, $2, $3, $4, $5, $6, $7, $8,
					$9, $10, $11, $12, $13,
					$14, $15, $16, $17, $18,
					$19, $20, $21, $22, $23)`,
						game.Year(), player.ID, player.First, player.Last, nullableString(player.Num), player.Boxname, player.Rl, player.Bats,
						player.Position, player.CurrentPosition, player.Status, player.TeamAbbrev, player.TeamID,
						player.ParentTeamAbbrev, nullableString(player.ParentTeamID), nullableString(player.BatOrder), player.GamePosition, player.Avg,
						player.HR, player.RBI, nullableString(player.Wins), nullableString(player.Losses), nullableString(player.ERA))

					if err != nil {
						if !s.Contains(err.Error(), "duplicate key") {
							log.Fatal(err)
						}
					} else {
						res.Close()
					}
				}
			}
		}
	}
	gamedayapi.FetchByTeamAndYears(teamCode, years, fetchFunction)
}

func nullableString(value string) interface{} {
	if value == "" || value == "-" || value == "-.--" {
		return nil
	}
	return value
}
