package main

import (
	"github.com/ecopony/pggameday"
	"github.com/ecopony/gamedayapi"
	"fmt"
	"os"
	"sync"
	"strconv"
)

var commands = map[string]func([]string) {
	"create-tables": createTables,
	"create-hits-table": createHitsTable,
	"create-pitches-table": createPitchesTable,
	"create-players-table": createPlayersTable,
	"import-pitches-for-year": importPitchesForYear,
	"import-pitches-for-team-and-year": importPitchesForTeamAndYear,
	"import-players-for-year": importPlayersForYear,
}

func main() {
	args := os.Args[1:]
	command := args[0]

	if function, ok := commands[command]; ok {
		function(args)
	} else {
		fmt.Println(fmt.Sprintf("%s is not a valid command. Valid commands:", command))
		printValidCommands()
		os.Exit(1)
	}
}

func printUsage() {
	fmt.Println("Usage: pgmlbgd <command> [<team code>] <years>")
}

func printValidCommands() {
	printUsage()
	fmt.Println("Valid commands:")
	for validCommand, _ := range commands {
		fmt.Println(fmt.Sprintf("\t%s", validCommand))
	}
}

func validateArgLength(args []string, validLength int) {
	if len(args) <= validLength {
		printUsage()
		os.Exit(1)
	}
}

func createTables(args []string) {
	pggameday.CreateTables()
}

func createHitsTable(args []string) {
	pggameday.CreateHitsTable()
}

func createPitchesTable(args []string) {
	pggameday.CreatePitchesTable()
}

func createPlayersTable(args []string) {
	pggameday.CreatePlayersTable()
}

func importPitchesForYear(args []string) {
	validateArgLength(args, 1)
	yearArg := args[1]
	year, err := strconv.Atoi(yearArg)
	if err != nil {
		fmt.Println("Year is not valid")
	}
	years := []int{year}
	teams := gamedayapi.TeamsForYear(year)
	var wg sync.WaitGroup
	for _, team := range teams {
		wg.Add(1)
		go func(team string) {
			defer wg.Done()
			pggameday.ImportPitchesForTeamAndYears(team, years)
		}(team)
	}
	wg.Wait()
}

func importPitchesForTeamAndYear(args []string) {
	validateArgLength(args, 2)
	teamCode := args[1]
	yearArgs := args[2:]
	var years []int
	for i := 0; i < len(yearArgs); i++ {
		year, err := strconv.Atoi(yearArgs[i])
		if err != nil {
			fmt.Println("Year is not valid")
		}
		years = append(years, year)
	}

	pggameday.ImportPitchesForTeamAndYears(teamCode, years)
}

func importPlayersForYear(args []string) {
	validateArgLength(args, 1)
	yearArg := args[1]
	year, err := strconv.Atoi(yearArg)
	if err != nil {
		fmt.Println("Year is not valid")
	}
	years := []int{year}
	teams := gamedayapi.TeamsForYear(year)
	var wg sync.WaitGroup
	for _, team := range teams {
		wg.Add(1)
		go func(team string) {
			defer wg.Done()
			pggameday.ImportPlayersForTeamAndYears(team, years)
		}(team)
	}
	wg.Wait()
}
