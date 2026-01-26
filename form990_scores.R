# ------------------------------------------------------------
# Setup
# ------------------------------------------------------------
library(dplyr)
library(readr)
library(RPostgres)
library(DBI)

# Set working directory
setwd("/Users/natalievolin/Library/CloudStorage/GoogleDrive-nvolin@charitynavigator.org/Shared drives/Encompass Rating /Beacon | Impact & Measurement/2 | Ratings/2.1 | Ratings Production/R code for ratings production/4. Other R code/Encompass2.0_scoring")

# Connect to the Redshift database
con <- dbConnect(
  RPostgres::Postgres(),
  dbname   = Sys.getenv("Postgresql_DBNAME"),
  host     = Sys.getenv("Postgresql_HOST"),
  port     = Sys.getenv("Postgresql_PORT"),
  user     = Sys.getenv("Postgresql_USER"),
  password = Sys.getenv("Postgresql_PASSWORD")
)

# ------------------------------------------------------------
# Pull data
# ------------------------------------------------------------
query <- "
SELECT
    ein_rollup, rating_size,
    -- Financial ratios
    avg_program_expense_ratio, avg_total_expense_amt,
    calc_1_year_program_expense_ratio, calc_fund_eff_ratio,
    calc_independant_board_ratio, calc_lta_ratio, calc_wkg_cap_ratio,
    -- Form 990 booleans
    has_990_board_provided, has_990_on_website, has_audit,
    has_audit_committee, has_audit_on_website, has_board_minutes,
    has_board_on_website, has_compilation_or_review, has_compensates_board,
    has_ceo_and_compensation_on_990, has_ceo_compensation_policy,
    has_conflict_of_interest_policy, has_document_retention_policy,
    has_donor_privacy_policy, has_grants_payable, has_key_staff_on_website,
    has_loans_to_from_officers, has_material_diversion, has_passing_board_size,
    has_passing_board_size_large, has_url_on_990, has_whistleblower_policy,
    -- Eligibility flags
    is_eligible_990_board_provided, is_eligible_990_on_website,
    is_eligible_audit_on_website, is_eligible_audit_review_compilation,
    is_eligible_board_minutes, is_eligible_board_on_website,
    is_eligible_ceo_and_compensation_on_990, is_eligible_ceo_compensation_policy,
    is_eligible_compensates_board, is_eligible_conflict_of_interest_policy,
    is_eligible_document_retention_policy, is_eligible_donor_privacy_policy,
    is_eligible_fundeff, is_eligible_independent_board,
    is_eligible_independent_board_size, is_eligible_key_staff_on_website,
    is_eligible_loans_to_from_officers, is_eligible_lta,
    is_eligible_material_diversion, is_eligible_program_expense,
    is_eligible_url_on_990, is_eligible_whistleblower_policy,
    is_eligible_wkgcap,
    -- Scores and possible points
    score_fundeff, score_lta, score_program_expense, score_wkgcap,
    possible_fundeff, possible_lta, possible_program_expense, possible_wkgcap
FROM public.v_encompass_v28_publish
WHERE fa_fully_eligible = 'true'
  AND COALESCE(fullyearsequence, -1) < 2;
"

form990_metrics <- dbGetQuery(con, query)

# ------------------------------------------------------------
# Compute scores
# ------------------------------------------------------------
form990_scores <- form990_metrics %>%
  mutate(
    # Financial Health Metrics
    fundeff_score        = if_else(is_eligible_fundeff, score_fundeff / possible_fundeff, NA_real_),
    lta_score            = if_else(is_eligible_lta, score_lta / possible_lta, NA_real_),
    program_exp_score    = if_else(is_eligible_program_expense, score_program_expense / possible_program_expense, NA_real_),
    wkgcap_score         = if_else(is_eligible_wkgcap, score_wkgcap / possible_wkgcap, NA_real_),
    
    # Accountability Metrics
    url_on_990_score               = if_else(is_eligible_url_on_990, as.numeric(has_url_on_990), NA_real_),
    material_diversion_score       = if_else(is_eligible_material_diversion, as.numeric(has_material_diversion), NA_real_),
    financial_statements_score = case_when(
      rating_size == "SMALL" & (has_compilation_or_review | has_audit) ~ 1,
      rating_size %in% c("MEDIUM", "LARGE", "SUPER") & has_audit ~ 1,
      TRUE ~ 0),
    audit_committee_score          = if_else(is_eligible_audit_committee, as.numeric(has_audit_committee), NA_real_),
    loans_to_from_officers_score   = if_else(is_eligible_loans_to_from_officers, as.numeric(has_loans_to_from_officers), NA_real_),
    document_retention_policy_score = if_else(is_eligible_document_retention_policy, as.numeric(has_document_retention_policy), NA_real_),
    form990_on_website_score       = if_else(is_eligible_990_on_website, as.numeric(has_990_on_website), NA_real_),
    
    # Website Disclosures
    donor_privacy_subscore  = case_when(
      has_donor_privacy_policy == "yes"     ~ 0.13334,
      has_donor_privacy_policy == "opt-out" ~ 0.06667,
      TRUE                                  ~ 0),
    audit_online_subscore    = if_else(has_audit_on_website, 0.33334, 0),
    board_online_subscore    = if_else(has_board_on_website, 0.33334, 0),
    key_staff_online_subscore = if_else(has_key_staff_on_website, 0.2, 0),
    web_disclosure_score     = pmin(donor_privacy_subscore + audit_online_subscore + board_online_subscore + key_staff_online_subscore, 1),
    
    # Compensation Metrics
    compensates_board_score      = if_else(is_eligible_compensates_board, as.numeric(!has_compensates_board), NA_real_),
    ceo_compensation_policy_score = if_else(is_eligible_ceo_compensation_policy, as.numeric(has_ceo_compensation_policy), NA_real_),
    ceo_on_990_score             = if_else(is_eligible_ceo_and_compensation_on_990, as.numeric(has_ceo_and_compensation_on_990), NA_real_),
    
    # Workplace Policy Metrics
    whistleblower_policy_score    = if_else(is_eligible_whistleblower_policy, as.numeric(has_whistleblower_policy), NA_real_),
    conflict_of_interest_policy_score = if_else(is_eligible_conflict_of_interest_policy, as.numeric(has_conflict_of_interest_policy), NA_real_),
    
    # Governance Metrics
    independent_board_size_score       = if_else(is_eligible_independent_board_size, as.numeric(has_independent_board_size), NA_real_),
    independent_board_size_large_score = if_else(is_eligible_independent_board_size_large, as.numeric(has_independent_board_size_large), NA_real_),
    independent_board_score            = if_else(is_eligible_independent_board, as.numeric(has_independent_board), NA_real_),
    board_minutes_score                = if_else(is_eligible_board_minutes, as.numeric(has_board_minutes), NA_real_),
    form990_board_provided_score       = if_else(is_eligible_990_board_provided, as.numeric(has_990_board_provided), NA_real_)
  )

# Keep only eligibility flags and all score variables
form990_scores_subset <- form990_scores %>%
  select(
    ein_rollup, rating_size, fa_fully_eligible, encompass_score_total, 
    is_noncash, is_museum, is_grant_making, is_donor_supported,
    # ----------------------------
    # Eligibility flags
    starts_with("is_eligible"),
    # ----------------------------
    # Calculated scores
    ends_with("_score")
  )