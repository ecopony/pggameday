package main

import (
	"github.com/ecopony/pggameday"
	"github.com/ecopony/gamedayapi"
	"fmt"
	"os"
	"sync"
	"strconv"
)

var commands = []string{
	"import-pitches-for-team-and-year",
	"import-pitches-for-year",
}

func main() {
	args := os.Args[1:]
	command := args[0]
	if !isCommandValid(command) {
		fmt.Println(fmt.Sprintf("%s is not a valid command. Valid commands:", command))
		printValidCommands()
		os.Exit(1)
	}

	if command == "import-pitches-for-team-and-year" {
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
	} else {	// import-pitches-for-year
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
}

func isCommandValid(command string) bool {
	for _, validCommand := range commands {
		if command == validCommand {
			return true
		}
	}
	return false
}

func printUsage() {
	fmt.Println("Usage: pgmlbgd <command> [<team code>] <years>")
}

func printValidCommands() {
	printUsage()
	fmt.Println("Valid commands:")
	for _, validCommand := range commands {
		fmt.Println(fmt.Sprintf("\t%s", validCommand))
	}
}

func validateArgLength(args []string, validLength int) {
	if len(args) <= validLength {
		printUsage()
		os.Exit(1)
	}
}

