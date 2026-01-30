library(dplyr)
library(readr)

# ------------------------------------------------------------------
# Load data and compute Program Planning scores
# ------------------------------------------------------------------

program_planning_scores <- read_csv("mo_coded_2026-01-12.csv") %>%
  mutate(
    ein = as.character(ein),
    submitted_at = as.POSIXct(submitted_at)  # make sure it's a proper datetime
  ) %>%
  group_by(ein) %>%
  slice_max(submitted_at, n = 1, with_ties = FALSE) %>%  # keep the latest row per EIN
  ungroup()

program_planning_scores <- program_planning_scores %>%
  mutate(
    # =============================================================
    # Shared Understanding of Theory of Change (MICRO/SMALL)
    # =============================================================
    is_eligible_understand_toc = size %in% c("MICRO", "SMALL"),
    understand_toc_score = if_else(
      is_eligible_understand_toc,
      case_when(
        plan_understand_toc_full == 1 ~ 1,
        plan_understand_toc_partial == 1 ~ 0.5,
        plan_understand_toc_none == 1 ~ 0,
        TRUE ~ NA_real_
      ),
      NA_real_
    ),
    
    # =============================================================
    # Theory of Change Practices (all sizes)
    # =============================================================
    is_eligible_toc_practices = TRUE,
    plan_documents_toc_subscore = as.numeric(plan_documents_toc_yes),
    plan_researched_toc_count = rowSums(
      across(
        c(
          plan_researched_toc_lit,
          plan_researched_toc_npconsult,
          plan_researched_toc_npprogram,
          plan_researched_toc_govtprogram,
          plan_researched_toc_privsector,
          plan_researched_toc_targetpop,
          plan_researched_toc_compliance
        ),
        ~ .x == TRUE
      ),
      na.rm = TRUE
    ),
    plan_researched_toc_subscore = case_when(
      size == "SMALL"  ~ pmin(plan_researched_toc_count / 5, 1),
      size == "MEDIUM" ~ pmin(plan_researched_toc_count / 6, 1),
      size %in% c("LARGE", "SUPER") ~ pmin(plan_researched_toc_count / 7, 1),
      TRUE ~ NA_real_
    ),
    plan_reviews_toc_subscore = as.numeric(plan_reviews_toc_yes),
    toc_practices_score = case_when(
      size == "MICRO" ~ ((plan_documents_toc_subscore * 2) + (plan_reviews_toc_subscore * 1)) / 3,
      size %in% c("SMALL", "MEDIUM", "LARGE", "SUPER") ~
        ((plan_documents_toc_subscore * 2) +
           (plan_researched_toc_subscore * 2) +
           (plan_reviews_toc_subscore * 1)) / 5,
      TRUE ~ NA_real_
    ),
    
    # =============================================================
    # Monitoring and Evaluation Practices (SMALL+)
    # =============================================================
      is_eligible_me_practices = size %in% c("SMALL", "MEDIUM", "LARGE", "SUPER"),
      
      me_practices_score = if_else(
        is_eligible_me_practices,
        case_when(
          size == "SMALL" ~ rowSums(
            across(
              c(plan_tracks_toc_activities,
                plan_tracks_toc_outcomes,
                plan_tracks_toc_timeline,
                plan_tracks_toc_mission),
              ~ as.numeric(.x)
            ),
            na.rm = TRUE
          ) / 4,
          
          size %in% c("MEDIUM", "LARGE", "SUPER") ~ rowSums(
            across(
              c(plan_tracks_toc_activities,
                plan_tracks_toc_outcomes,
                plan_tracks_toc_timeline,
                plan_tracks_toc_mission,
                dev_similarorgs_yes),
              ~ as.numeric(.x)
            ),
            na.rm = TRUE
          ) / 5
        ),
        NA_real_
      ),
    
    # ==============================================================
    # Reports Results to Stakeholders (MEDIUM+)
    # ==============================================================
    is_eligible_dev_similarorgs =
      size %in% c("MEDIUM", "LARGE", "SUPER"),
    
    dev_similarorgs_score = if_else(
      is_eligible_dev_similarorgs,
      ifelse(dev_similarorgs_yes == 1, 1,
             ifelse(dev_similarorgs_no == 1, 0, NA)),
      NA_real_
    ),
    
    # =============================================================
    # SMART Objectives (all sizes)
    # =============================================================
    is_eligible_smartgoals = TRUE,
    smartgoals_score = case_when(
      dev_smartgoals_full == 1 ~ 1,
      dev_smartgoals_partial == 1 ~ 0.5,
      dev_smartgoals_none == 1 ~ 0,
      TRUE ~ NA_real_
    ),
    
    # =============================================================
    # Needs Assessment (all sizes)
    # =============================================================
    is_eligible_needs_assessment = TRUE,
    needs_assessment_score = case_when(
      size %in% c("MEDIUM", "LARGE", "SUPER") ~ pmin(
        rowSums(
          across(
            c(
              dev_needs_npconsult,
              dev_needs_me,
              dev_needs_lit,
              dev_needs_interviews,
              dev_needs_assessment,
              dev_needs_baseline
            ),
            as.numeric
          ),
          na.rm = TRUE
        ) * 0.25,
        1
      ),
      size %in% c("MICRO", "SMALL") ~
        (dev_needs_targetpop * 0.5) +
        (dev_needs_commleaders * 0.25) +
        (dev_needs_localnps * 0.25),
      TRUE ~ NA_real_
    )
  ) %>%
  # Keep only the new variables
  select(
    ein,
    is_eligible_understand_toc,
    understand_toc_score,
    is_eligible_toc_practices,
    toc_practices_score,
    is_eligible_me_practices,
    me_practices_score,
    is_eligible_smartgoals,
    smartgoals_score,
    is_eligible_needs_assessment,
    needs_assessment_score
  )
