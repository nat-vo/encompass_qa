# Set working directory
setwd("/Users/natalievolin/Library/CloudStorage/GoogleDrive-nvolin@charitynavigator.org/Shared drives/Encompass Rating /Beacon | Impact & Measurement/2 | Ratings/2.1 | Ratings Production/R code for ratings production/4. Other R code/Encompass2.0_scoring")

# Load required libraries
library(tidyr)
library(readr)
library(RPostgres)
library(DBI)
library(dplyr)

# Connect to the prod database
con <- dbConnect(
  RPostgres::Postgres(),
  dbname   = Sys.getenv("Postgresql_DBNAME"),
  host     = Sys.getenv("Postgresql_HOST"),
  port     = Sys.getenv("Postgresql_PORT"),
  user     = Sys.getenv("Postgresql_USER"),
  password = Sys.getenv("Postgresql_PASSWORD")
)
query <- "
  SELECT ein_rollup, encompass_version, encompass_score_total,
         fa_score_total_display, ir_score_total_display, cc_score_total_display, la_score_total_display,
         fa_pass, ir_pass, cc_pass, la_pass,
         fa_fully_eligible, ir_fully_eligible, cc_fully_eligible, la_fully_eligible,
         rating_size, is_noncash, is_museum, is_grant_making, is_donor_supported,
         encompass_star_rating, summarycytotalrevenueamt
  FROM public.v_encompass_v28_publish
  WHERE encompass_eligible = 'true' AND COALESCE(fullyearsequence, -1) < 2;
"
encompass <- dbGetQuery(con, query)


library(dplyr)

encompass_joined <- encompass %>%
  # Join WPP scores
  left_join(wpp_scores,        by = c("ein_rollup" = "ein")) %>%
  # Join Form 990 scores
  left_join(form990_scores,    by = c("ein_rollup" = "ein_rollup")) %>%
  # Join Leadership scores
  left_join(leadership_scores, by = c("ein_rollup" = "ein")) %>%
  # Join Program Planning scores
  left_join(program_planning_scores, by = c("ein_rollup" = "ein")) %>%
  # Join Measurement scores
  left_join(measurement_scores, by = c("ein_rollup" = "ein")) %>%
  # Join Learning scores
  left_join(learning_scores,   by = c("ein_rollup" = "ein"))

# List column names that start with 'is_eligible' or end with '_score'
eligible_and_score_cols <- encompass_joined %>%
  select(
    starts_with("is_eligible"),
    ends_with("_score")
  ) %>%
  colnames()

# Print
eligible_and_score_cols





encompass_joined <- encompass_joined %>%
  rowwise() %>%
  mutate(
    encompass_score = sum(c_across(all_of(metrics_table$score_var)) *
                            c_across(all_of(metrics_table$eligibility_var)) *
                            metrics_table$weight,
                          na.rm = TRUE) /
      sum(c_across(all_of(metrics_table$eligibility_var)) *
            metrics_table$weight, na.rm = TRUE)
  ) %>%
  ungroup()






write.csv(encompass_joined, file = "encompass_joined.csv", row.names = FALSE)



