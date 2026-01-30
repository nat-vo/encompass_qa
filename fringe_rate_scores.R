# Load required libraries
library(tidyr)
library(readr)
library(RPostgres)
library(DBI)
library(dplyr)

# Connect to the redshift database
con <- dbConnect(
  RPostgres::Postgres(),
  dbname   = Sys.getenv("REDSHIFT_DBNAME"),
  host     = Sys.getenv("REDSHIFT_HOST"),
  port     = Sys.getenv("REDSHIFT_PORT"),
  user     = Sys.getenv("REDSHIFT_USER"),
  password = Sys.getenv("REDSHIFT_PASSWORD")
)

query <- "
  SELECT
    d.id,
    n.ein,
    d.fringe_benefits_rate
  FROM t_nonprofit_details d
  LEFT JOIN t_nonprofits n
    ON d.id = n.id
  WHERE d.fringe_benefits_rate IS NOT NULL;
"

fringe_rate <- dbGetQuery(con, query)