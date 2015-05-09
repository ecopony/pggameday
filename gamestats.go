package pggameday

import (
	"github.com/ecopony/gamedayapi"
	"strconv"
)

type GameStats struct {
	StatsTeamCode string
	Game        *gamedayapi.Game
	StatsTeamWon bool
	WalkOffLoss bool
}

func GameStatsFor(statsTeamCode string, game *gamedayapi.Game) (*GameStats) {
	gameStats := GameStats{ StatsTeamCode: statsTeamCode, Game: game }
	gameStats.determineStatsTeamWon()
	gameStats.determineWalkOffLoss()
	return &gameStats
}

func (gameStats *GameStats) determineStatsTeamWon() {
	var winningTeamCode string
	homeTeamWon := gameStats.Game.Boxscore().Linescore.HomeTeamRuns > gameStats.Game.Boxscore().Linescore.AwayTeamRuns
	if(homeTeamWon) {
		winningTeamCode = gameStats.Game.HomeCode
	} else {
		winningTeamCode = gameStats.Game.AwayCode
	}

	if (winningTeamCode == gameStats.StatsTeamCode) {
		gameStats.StatsTeamWon = true
	} else {
		gameStats.StatsTeamWon = false
	}
}

func (gameStats *GameStats) determineWalkOffLoss() {
	if(gameStats.StatsTeamWon || gameStats.IsStatsTeamHomeTeam()) {
		gameStats.WalkOffLoss = false
		return
	}

	inningLinescores := gameStats.Game.Boxscore().Linescore.InningLineScores
	bottomOfLastInningRuns, err := strconv.Atoi(inningLinescores[len(inningLinescores)-1].Home)

	// If the runs can't be parsed, the home team did not bat.
	if err != nil {
		gameStats.WalkOffLoss = false
		return
	}

	if (bottomOfLastInningRuns > 0) {
		gameStats.WalkOffLoss = true
		return
	}
}

func (gameStats *GameStats) IsStatsTeamHomeTeam() bool {
	return gameStats.Game.HomeCode == gameStats.StatsTeamCode
}
