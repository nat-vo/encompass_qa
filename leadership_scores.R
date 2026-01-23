# Load required libraries
library(tidyr)
library(readr)
library(RPostgres)
library(DBI)
library(dplyr)

# Connect to the redshift database
drv <- RPostgres::Postgres()
con <- dbConnect(drv, 
                 dbname = "ratings", 
                 host = "cn-analytics.ceatkmfl82x5.us-east-1.redshift.amazonaws.com", 
                 port = 5439, 
                 user = "programs_team", 
                 password = "LG83EGYNM288N4bTrqhK")

query <- "
SELECT *
FROM t_leadership_submissions
WHERE submitted_at > DATE '2022-04-07';
"
leadership_submissions <- dbGetQuery(con, query)


leadership_cleaned <- leadership_submissions %>%
 # ------------------------------------------------------------
# Convert blank character fields to NA
# ------------------------------------------------------------
mutate(
  across(
    c(
      vision,
      goal1,
      goal2,
      goal3,
      leadership_investment_text,
      mobilization_efforts,
      adaptation_story
    ),
    ~ na_if(trimws(.), "")
  )
) %>%
  
  # ------------------------------------------------------------
# Mission / Vision eligibility
# ------------------------------------------------------------
mutate(
  has_a_goal   = !is.na(goal1) & goal1 != "NULL",
  has_a_vision = !is.na(vision) & vision != "NULL",
  
  is_eligible_mission_vision_goals =
    !is.na(vision) & !is.na(goal1)
) %>%
  
  # ------------------------------------------------------------
# Q1: Leadership Investment
# ------------------------------------------------------------
mutate(
  # Eligible if any option is TRUE
  is_eligible_leadership_investment = if_any(
    c(
      mentorship_coaching,
      conferences_networking,
      educational_programs,
      succession_planning,
      diversity_training,
      leadership_investment_none
    ),
    ~ . == TRUE
  )
) %>%
  
  # Replace NA with FALSE once eligibility is established
  mutate(
    across(
      c(
        mentorship_coaching,
        conferences_networking,
        educational_programs,
        succession_planning,
        diversity_training,
        leadership_investment_none
      ),
      ~ if_else(is_eligible_leadership_investment & is.na(.), FALSE, .)
    )
  ) %>%
  
  # Count leadership investment selections (excluding "none")
  mutate(
    leadership_investment_count = as.integer(
      rowSums(
        across(
          c(
            mentorship_coaching,
            conferences_networking,
            educational_programs,
            succession_planning,
            diversity_training
          ),
          ~ . == TRUE
        )
      )
    )
  ) %>%
  
  # ------------------------------------------------------------
# Q2: External Engagement
# ------------------------------------------------------------
mutate(
  is_eligible_external_engagement = if_any(
    c(
      strategic_partnerships,
      networks_collective,
      thought_leadership,
      raise_awareness,
      community_building,
      policy_advocacy,
      mobilization_none
    ),
    ~ . == TRUE
  )
) %>%
  
  mutate(
    across(
      c(
        strategic_partnerships,
        networks_collective,
        thought_leadership,
        raise_awareness,
        community_building,
        policy_advocacy,
        mobilization_none
      ),
      ~ if_else(is_eligible_external_engagement & is.na(.), FALSE, .)
    )
  ) %>%
  
  mutate(
    external_engagement_count = as.integer(
      rowSums(
        across(
          c(
            strategic_partnerships,
            networks_collective,
            thought_leadership,
            raise_awareness,
            community_building,
            policy_advocacy
          ),
          ~ . == TRUE
        )
      )
    )
  ) %>%
  
  # ------------------------------------------------------------
# Q3: Adaptability
# ------------------------------------------------------------
mutate(
  is_eligible_adaptability = if_any(
    c(
      strategic_planning,
      risk_management,
      capacity_building,
      technology_integration,
      programmatic_shifts,
      partnerships_collaborations,
      diversifying_funding,
      community_engagement,
      adaptability_none
    ),
    ~ . == TRUE
  )
) %>%
  
  mutate(
    across(
      c(
        strategic_planning,
        risk_management,
        capacity_building,
        technology_integration,
        programmatic_shifts,
        partnerships_collaborations,
        diversifying_funding,
        community_engagement
      ),
      ~ if_else(is_eligible_adaptability & is.na(.), FALSE, .)
    )
  ) %>%
  
  mutate(
    adaptability_count = as.integer(
      rowSums(
        across(
          c(
            strategic_planning,
            risk_management,
            capacity_building,
            technology_integration,
            programmatic_shifts,
            partnerships_collaborations,
            diversifying_funding,
            community_engagement
          ),
          ~ . == TRUE
        )
      )
    )
  ) %>%
  
  # ------------------------------------------------------------
# Final shaping
# ------------------------------------------------------------
rename(ein_rollup = ein) %>%
  mutate(ein_rollup = as.integer(ein_rollup)) %>%
  select(
    ein_rollup,
    is_eligible_mission_vision_goals,
    is_eligible_leadership_investment,
    leadership_investment_count,
    is_eligible_external_engagement,
    external_engagement_count,
    is_eligible_adaptability,
    adaptability_count
  )



