# Set working directory
setwd("/Users/natalievolin/Library/CloudStorage/GoogleDrive-nvolin@charitynavigator.org/Shared drives/Encompass Rating /Beacon | Impact & Measurement/2 | Ratings/2.1 | Ratings Production/R code for ratings production/4. Other R code/Encompass2.0_scoring")

# Load required libraries
library(tidyr)
library(readr)
library(RPostgres)
library(DBI)
library(dplyr)

# Connect to the redshift database
drv <- RPostgres::Postgres()
con <- dbConnect(drv, 
                 dbname = "prod", 
                 host = "cn-ratings-public-replica-db.cidpyj9wyffz.us-east-1.rds.amazonaws.com", 
                 port = 5432, 
                 user = "programs_team", 
                 password = "LG83EGYNM288N4bTrqhK")
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



#join leadership, feedback, measurement to encompass sheet

write.csv(leadership_cleaned, file = "leadership_cleaned_5jan2026.csv", row.names = FALSE)



