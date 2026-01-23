library(dplyr)
library(readr)

# ------------------------------------------------------------------
# Load data and compute learning scores
# ------------------------------------------------------------------
learning_scores <- read_csv("mo_coded_2026-01-12.csv") %>%
  mutate(
    ein_rollup = as.integer(ein),
    
    # =============================================================
    # Leadership Commitment to Monitoring and Evaluation (all sizes)
    # =============================================================
    is_eligible_leadership_commitment = TRUE,
    leadership_commitment_score = case_when(
      plan_leadership_full == 1    ~ 1,
      plan_leadership_partial == 1 ~ 0.5,
      plan_leadership_none == 1    ~ 0,
      TRUE                         ~ NA_real_
    ),
    
    # =============================================================
    # Reports Results to Stakeholders (all sizes)
    # =============================================================
    is_eligible_report_to_stakeholders = TRUE,
    report_to_stakeholders_score = case_when(
      size %in% c("MICRO", "SMALL") ~ if_else(report_stakeholders_yes == 1, 1, 0),
      TRUE ~ (report_stakeholders_funders +
                report_stakeholders_staff +
                report_stakeholders_board +
                report_stakeholders_targetpop +
                report_stakeholders_otherorgs +
                report_stakeholders_public) / 6
    ),
    
    # =============================================================
    # Responsible Disclosure of Results (all sizes)
    # =============================================================
    is_eligible_results_disclosure = TRUE,
    results_disclosure_score = case_when(
      report_all_findings_yes == 1 ~ 1,
      report_all_findings_no  == 1 ~ 0,
      TRUE                         ~ NA_real_
    ),
    
    # =============================================================
    # Uses Results to Improve Programs (all sizes)
    # =============================================================
    is_eligible_use_results = TRUE,
    use_results_score = (results_use_funding +
                           results_use_planning +
                           results_use_operations +
                           results_use_impact +
                           results_use_futureprog) / 5
  ) %>%
  # Keep only the new variables
  select(
    ein_rollup,
    is_eligible_leadership_commitment,
    leadership_commitment_score,
    is_eligible_report_to_stakeholders,
    report_to_stakeholders_score,
    is_eligible_results_disclosure,
    results_disclosure_score,
    is_eligible_use_results,
    use_results_score
  )


# Full join learning_scores and cf_scores by ein_rollup
learning_scores <- full_join(learning_scores, cf_scores, by = "ein_rollup")