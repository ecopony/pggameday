package main

import (
	"github.com/ecopony/pggameday"
	"fmt"
	"os"
	"strconv"
)

var validCommands = map[string]bool{
	"import-pitches": true,
}

func main() {
	args := os.Args[1:]

	if len(args) <= 2 {
		fmt.Println("Usage: pgmlbgd <command> <team code> <years>")
		os.Exit(1)
	}

	command := args[0]
	if !isCommandValid(command) {
		fmt.Println(fmt.Sprintf("%s is not a valid command. Valid commands:", command))

		for k := range validCommands {
			fmt.Println(fmt.Sprintf("\t%s", k))
		}

		os.Exit(1)
	}

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

func isCommandValid(command string) bool {
	return validCommands[command]
}
