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
SELECT *
FROM t_leadership_submissions
WHERE submitted_at > DATE '2022-04-07';
"
leadership_submissions <- dbGetQuery(con, query)

leadership_scores <- leadership_submissions %>%
  # ------------------------------------------------------------
# Join rating_size from encompass
# ------------------------------------------------------------
left_join(
  encompass %>% select(ein_rollup, rating_size),
  by = c("ein" = "ein_rollup")
) %>%
  
# ------------------------------------------------------------
# Convert blank character fields to NA
# ------------------------------------------------------------
mutate(
  across(
    c(vision, goal1, goal2, goal3, leadership_investment_text,
      mobilization_efforts, adaptation_story),
    ~ na_if(trimws(.), "")
  )
) %>%
  
  # ------------------------------------------------------------
# Q1: Mission / Vision eligibility
# ------------------------------------------------------------
mutate(
  has_a_goal = !is.na(goal1) & goal1 != "NULL",
  has_a_vision = !is.na(vision) & vision != "NULL",
  is_eligible_mission_vision_goals = !is.na(vision) & !is.na(goal1),
  mission_vision_goals_score = if_else(is_eligible_mission_vision_goals,
                                       1, NA_real_)
) %>%
  
  # ------------------------------------------------------------
# Q2: Leadership Investment
# ------------------------------------------------------------
mutate(
  is_eligible_leadership_investment = if_any(
    c(mentorship_coaching, conferences_networking, educational_programs,
      succession_planning, diversity_training, leadership_investment_none),
    ~ . == TRUE
  )
) %>%
  mutate(
    across(
      c(mentorship_coaching, conferences_networking, educational_programs,
        succession_planning, diversity_training, leadership_investment_none),
      ~ if_else(is_eligible_leadership_investment & is.na(.), FALSE, .)
    )
  ) %>%
  mutate(
    leadership_investment_count = rowSums(
      across(
        c(mentorship_coaching, conferences_networking, educational_programs,
          succession_planning, diversity_training),
        ~ . == TRUE
      ),
      na.rm = TRUE
    )
  ) %>%
  mutate(
    leadership_investment_score = if_else(
      is_eligible_leadership_investment,
      case_when(
        is.na(rating_size) | rating_size %in% c("MICRO", "SMALL") ~ pmin(leadership_investment_count / 2, 1),
        rating_size %in% c("MEDIUM", "LARGE")                     ~ pmin(leadership_investment_count / 3, 1),
        rating_size == "SUPER"                                    ~ pmin(leadership_investment_count / 4, 1),
        TRUE                                                      ~ NA_real_
      ),
      NA_real_
    )
  ) %>%
  
  # ------------------------------------------------------------
# Q3: External Engagement
# ------------------------------------------------------------
mutate(
  is_eligible_external_engagement = if_any(
    c(strategic_partnerships, networks_collective, thought_leadership,
      raise_awareness, community_building, policy_advocacy, mobilization_none),
    ~ . == TRUE
  )
) %>%
  mutate(
    across(
      c(strategic_partnerships, networks_collective, thought_leadership,
        raise_awareness, community_building, policy_advocacy, mobilization_none),
      ~ if_else(is_eligible_external_engagement & is.na(.), FALSE, .)
    )
  ) %>%
  mutate(
    external_engagement_count = rowSums(
      across(
        c(strategic_partnerships, networks_collective, thought_leadership,
          raise_awareness, community_building, policy_advocacy),
        ~ . == TRUE
      ),
      na.rm = TRUE
    )
  ) %>%
  mutate(
    external_engagement_score = if_else(
      is_eligible_external_engagement,
      case_when(
        is.na(rating_size) | rating_size %in% c("MICRO")          ~ pmin(external_engagement_count / 3, 1),
        rating_size %in% c("SMALL", "MEDIUM")                    ~ pmin(external_engagement_count / 4, 1),
        rating_size %in% c("LARGE", "SUPER")                     ~ pmin(external_engagement_count / 5, 1),
        TRUE                                                      ~ NA_real_
      ),
      NA_real_
    )
  ) %>%
  
# ------------------------------------------------------------
# Q4: Adaptability
# ------------------------------------------------------------
mutate(
  is_eligible_adaptability = if_any(
    c(strategic_planning, risk_management, capacity_building, technology_integration,
      programmatic_shifts, partnerships_collaborations, diversifying_funding,
      community_engagement, adaptability_none),
    ~ . == TRUE
  )
) %>%
  mutate(
    across(
      c(strategic_planning, risk_management, capacity_building, technology_integration,
        programmatic_shifts, partnerships_collaborations, diversifying_funding,
        community_engagement),
      ~ if_else(is_eligible_adaptability & is.na(.), FALSE, .)
    )
  ) %>%
  mutate(
    adaptability_count = rowSums(
      across(
        c(strategic_planning, risk_management, capacity_building, technology_integration,
          programmatic_shifts, partnerships_collaborations, diversifying_funding,
          community_engagement),
        ~ . == TRUE
      ),
      na.rm = TRUE
    )
  ) %>%
  mutate(
    adaptability_score = if_else(
      is_eligible_adaptability,
      case_when(
        is.na(rating_size) | rating_size %in% c("MICRO", "SMALL") ~ pmin(adaptability_count / 4, 1),
        rating_size %in% c("MEDIUM", "LARGE")                      ~ pmin(adaptability_count / 5, 1),
        rating_size == "SUPER"                                     ~ pmin(adaptability_count / 6, 1),
        TRUE                                                       ~ NA_real_
      ),
      NA_real_
    )
  ) %>%
  
# ------------------------------------------------------------
# Q5: Board Approved Budget
# ------------------------------------------------------------
mutate(
  is_eligible_board_approved_budget =
    !is.na(has_board_approved_budget),
  
  board_approved_budget_score = case_when(
    is_eligible_board_approved_budget & has_board_approved_budget == TRUE  ~ 1,
    is_eligible_board_approved_budget & has_board_approved_budget == FALSE ~ 0,
    TRUE                                                                    ~ NA_real_
  )
  ) %>%
  
# ------------------------------------------------------------
# Final shaping
# ------------------------------------------------------------
select(
  ein,
  is_eligible_mission_vision_goals, mission_vision_goals_score,
  is_eligible_leadership_investment, leadership_investment_score,
  is_eligible_external_engagement, external_engagement_score,
  is_eligible_adaptability, adaptability_score,
  is_eligible_board_approved_budget, board_approved_budget_score
)

