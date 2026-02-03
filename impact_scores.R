# Load required libraries
library(tidyr)
library(readr)
library(RPostgres)
library(DBI)
library(dplyr)
library(lubridate)
library(stringr)

# Connect to the redshift database
con <- dbConnect(
  RPostgres::Postgres(),
  dbname   = Sys.getenv("REDSHIFT_DBNAME"),
  host     = Sys.getenv("REDSHIFT_HOST"),
  port     = Sys.getenv("REDSHIFT_PORT"),
  user     = Sys.getenv("REDSHIFT_USER"),
  password = Sys.getenv("REDSHIFT_PASSWORD")
)

# ------------------------------------------------------------
# Pull impact submissions with scores
# ------------------------------------------------------------
impact_scores <- dbGetQuery(con, "
  SELECT ein, value_cem, value_ra
  FROM t_impactmatters_ratings
  WHERE (value_cem IS NOT NULL OR value_ra IS NOT NULL)
    AND archived_dt IS NULL
")

impact_data_elements <- dbGetQuery(con, "
  SELECT * FROM t_impact_submissions
  WHERE statement IS NOT NULL
  AND archived_at IS NULL
")

impact_submissions <- impact_scores %>%
  left_join(
    impact_data_elements,
    by = "ein"
  )

impact_submissions <- impact_submissions %>%
  mutate(
    is_eligible_cem = !is.na(value_cem),
    cem_score = value_cem,
    is_eligible_ra  = !is.na(value_ra),
    ra_score = value_ra
  )

impact_submissions <- impact_submissions %>%
  mutate(
    # Extract the right-hand side date
    end_date_raw = str_trim(str_extract(data_period, "(?<=to\\s).*")),
    
    # Parse it as a datetime
    submitted_at = mdy(end_date_raw),
    
    # If parsing fails (NA), use fallback date
    submitted_at = coalesce(submitted_at, as_datetime("2026-04-06 00:00:00", tz = "UTC"))
  )

# Expiration date: four years before April 6, 2026
impact_submissions <- impact_submissions %>%
  mutate(
    expired = submitted_at < (as_date("2026-04-06") %m-% years(4))
  )

table(impact_submissions$expired)
impact_psa_scores <- impact_submissions %>%
  filter(expired == FALSE) %>%
  mutate(
    cem_score = as.numeric(cem_score),
    ra_score  = as.numeric(ra_score)
  ) %>%
  select(
    ein,
    is_eligible_cem,
    cem_score,
    is_eligible_ra,
    ra_score,
    statement,
    context
  )
