

# Connect to the redshift database
drv <- RPostgres::Postgres()
con <- dbConnect(drv, 
                 dbname = "ratings", 
                 host = "cn-analytics.ceatkmfl82x5.us-east-1.redshift.amazonaws.com", 
                 port = 5439, 
                 user = "programs_team", 
                 password = "LG83EGYNM288N4bTrqhK")

# ------------------------------------------------------------
# Pull constituent feedback submissions
# ------------------------------------------------------------
query <- "
  SELECT *
  FROM t_constituent_feedback_submissions
  WHERE archived_at IS NULL;
"

cf_submissions <- dbGetQuery(con, query)

# ------------------------------------------------------------
# Keep only most recent submission for each ein
# ------------------------------------------------------------
cf_submissions <- cf_submissions %>%
  arrange(ein, desc(submitted_at)) %>%
  group_by(ein) %>%
  slice(1) %>%
  ungroup()

# ------------------------------------------------------------
# Scoring
# ------------------------------------------------------------
cf_scores <- cf_submissions %>%
  
  # ----------------------------------------------------------
# Q1: Feedback Collection
# ----------------------------------------------------------
mutate(
  feedback_collection_score = case_when(
    collecting_feedback == 1 ~ 1,
    
    if_any(
      c(
        not_considered,
        not_needed,
        already_understand,
        leadership_board,
        unsure_how,
        unsure_who,
        not_useful,
        cannot_address,
        lack_tools,
        lack_funding,
        lack_time,
        lack_skills
      ),
      ~ . == 1
    ) ~ 0.6,
    
    TRUE ~ 0
  )
) %>%
  
  # ----------------------------------------------------------
# Q2: Use of Feedback (capped at 1; zero if not collecting)
# ----------------------------------------------------------
mutate(
  feedback_q2 = if_else(
    collecting_feedback == 1,
    pmin(
      (poor_service == 1) * 0.125 +
        (positive_service == 1) * 0.125 +
        (fundamental_changes == 1) * 0.125 +
        (development_of_new == 1) * 0.125 +
        (inclusion_equity == 1) * 0.25  +
        (needs_and_goals == 1) * 0.125 +
        (strengthen_relationships == 1) * 0.125,
      1
    ),
    0
  )
) %>%
  
  # ----------------------------------------------------------
# Q3: Feedback Practices (capped at 1; zero if not collecting)
# ----------------------------------------------------------
mutate(
  feedback_q3 = if_else(
    collecting_feedback == 1,
    pmin(
      if_else(collected_at_least_annually == 1, 0.059, 0) +
        if_else(feedback_marginalized_under_represented == 1, 0.118, 0) +
        if_else(feedback_from_everyone_served == 1, 0.059, 0) +
        if_else(honest_feedback_measures == 1, 0.059, 0) +
        if_else(analyze_on_demographics == 1, 0.118, 0) +
        if_else(analyze_on_interactions == 1, 0.118, 0) +
        if_else(post_feedback_engagement == 1, 0.118, 0) +
        if_else(action_taken_on_feedback == 1, 0.059, 0) +
        if_else(feedback_shared == 1, 0.059, 0) +
        if_else(feedback_given_on_actions == 1, 0.118, 0) +
        if_else(feedback_received_for_actions == 1, 0.118, 0),
      1
    ),
    0
  )
) %>%
  
  # ----------------------------------------------------------
# Final: Feedback Practices Score
# ----------------------------------------------------------
mutate(
  feedback_practices_score =
    ((feedback_q2 * 1) + (feedback_q3 * 1.8)) / 2.8
)

# ------------------------------------------------------------
# Add eligibility variables
# ------------------------------------------------------------
cf_scores <- cf_scores %>%
  mutate(
    is_eligible_feedback_collection = TRUE,
    is_eligible_feedback_practices  = TRUE
  )
# ------------------------------------------------------------
# Keep only final outputs
# ------------------------------------------------------------
cf_scores <- cf_scores %>%
  dplyr::select(
    ein,
    is_eligible_feedback_collection,
    feedback_collection_score,
    is_eligible_feedback_practices,
    feedback_practices_score
  )


cf_scores %>%
  summarise(
    avg_feedback_collection_score  = mean(feedback_collection_score, na.rm = TRUE),
    avg_feedback_practices_score   = mean(feedback_practices_score,  na.rm = TRUE)
  )