package pggameday

import (
	"github.com/ecopony/gamedayapi"
	"reflect"
	"testing"
	"time"
)

func assertEquals(t *testing.T, a interface{}, b interface{}) {
	if !reflect.DeepEqual(a, b) {
		t.Errorf("'%+v' != '%+v'", a, b)
	}
}

func TestStatsTeamWonForWin(t *testing.T) {
	date, _ := time.Parse("2006-01-02", "2015-05-08")
	game, _ := gamedayapi.GameFor("sea", date)
	gameStats := GameStatsFor("sea", game)
	assertEquals(t, true, gameStats.StatsTeamWon)
}

func TestStatsTeamWonForLoss(t *testing.T) {
	date, _ := time.Parse("2006-01-02", "2015-05-06")
	game, _ := gamedayapi.GameFor("sea", date)
	gameStats := GameStatsFor("sea", game)
	assertEquals(t, false, gameStats.StatsTeamWon)
}

func TestWalkOffLossIdentifiesLoss(t *testing.T) {
	date, _ := time.Parse("2006-01-02", "2015-05-06")
	game, _ := gamedayapi.GameFor("sea", date)
	gameStats := GameStatsFor("sea", game)
	assertEquals(t, true, gameStats.WalkOffLoss)
}

// Need to find a test case where the home team scored in their final inning but did not win

func TestWalkOffLossFalseOnWalkoffWin(t *testing.T) {
	date, _ := time.Parse("2006-01-02", "2015-05-08")
	game, _ := gamedayapi.GameFor("sea", date)
	gameStats := GameStatsFor("sea", game)
	assertEquals(t, false, gameStats.WalkOffLoss)
}

func TestWalkOffLossFalseOnRegularLoss(t *testing.T) {
	date, _ := time.Parse("2006-01-02", "2015-05-03")
	game, _ := gamedayapi.GameFor("sea", date)
	gameStats := GameStatsFor("sea", game)
	assertEquals(t, false, gameStats.WalkOffLoss)
}
