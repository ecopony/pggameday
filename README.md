pggameday
=========

A utility for saving MLB Gameday data to Postgres.

Prerequisites: 
 * Working go installation
 * PostgreSQL running locally, with a user and database named 'go-gameday'
 * pq (go PostgreSQL driver; run 'go get github.com/lib/pq')

To build and run from the command line
---------

In your go workspace, under a directory github.com/ecopony/

    git clone git@github.com:ecopony/pggameday.git
    cd pggameday
    go build pg/pgmlbgd.go

To create the database tables:

    ./pgmlbgd create-tables
    
To import all pitches for a team for a given year:

    ./pgmlbgd import-pitches-for-team-and-year sea 2014

To import all pitches for a given year:

    ./pgmlbgd import-pitches-for-year 2014
