pggameday
=========

A utility for saving MLB Gameday data to Postgres.

Prerequisites: go

To build and run from the command line
---------

In your go workspace, under a directory github.com/ecopony/

    git clone git@github.com:ecopony/pggameday.git
    cd pggameday
    go build pg/pggameday.go

To create the database tables:

    ./pgmlbgd create-table

    
To import all pitches for a team for a given year:

    ./pgmlbgd import-pitches-for-team-and-year sea 2014


To import all pitches for a given year:

    ./pgmlbgd import-pitches-for-year 2014


