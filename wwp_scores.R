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

# ------------------------------------------------------------
# Pull constituent feedback submissions
# ------------------------------------------------------------
query <- "
  SELECT *
  FROM t_workplace_policies_submissions
  WHERE submitted_at > DATE '2022-04-07';
"

wpp_submissions <- dbGetQuery(con, query)

wpp_submissions <- wpp_submissions %>%
  # Keep only the most recent submission per EIN
  arrange(ein, desc(submitted_at)) %>%
  group_by(ein) %>%
  slice(1) %>%
  ungroup() %>%
  # Add eligibility columns
  mutate(
    # Eligibility columns
    is_eligible_workplace_policies = has_at_least_three_employees & implements_no_response == FALSE,
    is_eligible_promotional_pay_policy = has_at_least_three_employees & implements_no_response == FALSE,
    is_eligible_reviewed_compensation_structure = has_at_least_three_employees & implements_no_response == FALSE,
    is_eligible_hiring_employee_performance_practices = has_at_least_three_employees & employee_no_response == FALSE,
    
    # Workplace policies score (proportion of TRUEs among 7 columns)
    workplace_policies_score = if_else(
      is_eligible_workplace_policies,
      rowSums(select(., employee_eeo_statement,
                     employee_ada_information,
                     employee_sexual_harassment_policy,
                     employee_offer_letter,
                     employee_benefits_statement,
                     employee_standards_of_conduct,
                     employee_resignation_and_termination_information) == TRUE, na.rm = TRUE) / 7,
      NA_real_
    ),
    
    # Promotional pay policy score (1 if implements performance-based raises, else 0)
    promotional_pay_policy_score = if_else(
      is_eligible_promotional_pay_policy,
      as.numeric(implements_performance_based_raises_and_promotions == TRUE),
      NA_real_
    ),
    
    # Regularly reviewed compensation structure score (1 if TRUE, else 0)
    reviewed_compensation_structure_score = if_else(
      is_eligible_reviewed_compensation_structure,
      as.numeric(implements_compensation_structure == TRUE),
      NA_real_
    ),
    
    # Hiring and employee performance practices score (proportion of 4 TRUE columns)
    hiring_employee_performance_practices_score = if_else(
      is_eligible_hiring_employee_performance_practices,
      rowSums(select(., implements_job_descriptions,
                     implements_performance_evaluation,
                     implements_review_of_retention_and_promotion_data,
                     implements_process_to_minimize_bias) == TRUE, na.rm = TRUE) / 4,
      NA_real_
    )
  )


library(dplyr)

# Compute averages for your new variables
averages <- wpp_submissions %>%
  summarise(
    avg_is_eligible_workplace_policies = mean(is_eligible_workplace_policies, na.rm = TRUE),
    avg_is_eligible_promotional_pay_policy = mean(is_eligible_promotional_pay_policy, na.rm = TRUE),
    avg_is_eligible_reviewed_compensation_structure = mean(is_eligible_reviewed_compensation_structure, na.rm = TRUE),
    avg_is_eligible_hiring_employee_performance_practices = mean(is_eligible_hiring_employee_performance_practices, na.rm = TRUE),
    avg_workplace_policies_score = mean(workplace_policies_score, na.rm = TRUE),
    avg_promotional_pay_policy_score = mean(promotional_pay_policy_score, na.rm = TRUE),
    avg_reviewed_compensation_structure_score = mean(reviewed_compensation_structure_score, na.rm = TRUE),
    avg_hiring_employee_performance_practices_score = mean(hiring_employee_performance_practices_score, na.rm = TRUE)
  )

# View results
averages

write.csv(wpp_submissions, "wpp_submissions_clean.csv", row.names = FALSE)

wpp_scores <- wpp_submissions %>%
rename(ein_rollup = ein) %>%
  mutate(ein_rollup = as.integer(ein_rollup)) %>%
  select(
    ein_rollup,
    is_eligible_workplace_policies,
    workplace_policies_score,
    is_eligible_promotional_pay_policy,
    promotional_pay_policy_score,
    is_eligible_reviewed_compensation_structure,
    reviewed_compensation_structure_score,
    is_eligible_hiring_employee_performance_practices,
    hiring_employee_performance_practices_score
  )


