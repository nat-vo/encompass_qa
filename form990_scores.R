# Set working directory
setwd("/Users/natalievolin/Library/CloudStorage/GoogleDrive-nvolin@charitynavigator.org/Shared drives/Encompass Rating /Beacon | Impact & Measurement/2 | Ratings/2.1 | Ratings Production/R code for ratings production/4. Other R code/Encompass2.0_scoring")

# Load required libraries
library(tidyr)
library(readr)
library(RPostgres)
library(DBI)
library(dplyr)

drv <- RPostgres::Postgres()
con <- dbConnect(drv, 
                 dbname = "prod", 
                 host = "cn-ratings-public-replica-db.cidpyj9wyffz.us-east-1.rds.amazonaws.com", 
                 port = 5432, 
                 user = "programs_team", 
                 password = "LG83EGYNM288N4bTrqhK")
query <- "
   SELECT 
    ein_rollup,
    fa_fully_eligible,
    has_compensates_board,
    has_ceo_compensation_policy,
    has_ceo_and_compensation_on_990,
    is_eligible_compensates_board,
    is_eligible_ceo_compensation_policy,
    is_eligible_ceo_and_compensation_on_990
    
    
    
    
  FROM public.v_encompass_v28_publish
  WHERE fa_fully_eligible = 'true' 
    AND COALESCE(fullyearsequence, -1) < 2;
"
form990_metrics <- dbGetQuery(con, query)


# ------------------------------------------------------------------
# Load data and compute compensation scores
# ------------------------------------------------------------------

form990_scores <- form990_metrics %>%
  # Compensation metrics
  mutate(
    compensates_board_score = if_else(
      is_eligible_compensates_board,
      as.numeric(!has_compensates_board),
      NA_real_
    ),
    ceo_compensation_policy_score = if_else(
      is_eligible_ceo_compensation_policy,
      as.numeric(has_ceo_compensation_policy),
      NA_real_
    ),
    ceo_on_990_score = if_else(
      is_eligible_ceo_and_compensation_on_990,
      as.numeric(has_ceo_and_compensation_on_990),
      NA_real_
    ),
  # Workplace policies metrics  
  whistleblower_policy_score = if_else(
    is_eligible_whistleblower_policy,
    as.numeric(has_whistleblower_policy),
    NA_real_
  ),
  conflict_of_interest_policy_score = if_else(
    is_eligible_conflict_of_interest_policy,
    as.numeric(has_conflict_of_interest_policy),
    NA_real_
  ),
  # Governance Metrics
  independent_board_size_score = if_else(
    is_eligible_independent_board_size,
    as.numeric(has_independent_board_size),
    NA_real_
  ),
  independent_board_size_large_score = if_else(
    is_eligible_independent_board_size_large,
    as.numeric(has_independent_board_size_large),
    NA_real_
  ),
  independent_board_score = if_else(
    is_eligible_independent_board,
    as.numeric(has_independent_board),
    NA_real_
  ),
  board_minutes_score = if_else(
    is_eligible_board_minutes,
    as.numeric(has_board_minutes),
    NA_real_
  ), 
  form990_board_provided_score = if_else(
    is_eligible_990_board_provided,
    as.numeric(has_990_board_provided),
    NA_real_
  ), 
  ) %>%
  
  # Keep only relevant new variables
  select(
    ein_rollup,
    is_eligible_compensates_board,
    compensates_board_score,
    is_eligible_ceo_compensation_policy,
    ceo_compensation_policy_score,
    is_eligible_ceo_and_compensation_on_990,
    ceo_on_990_score
  )

