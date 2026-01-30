library(dplyr)
library(readr)

# Load data and create scores
measurement_scores <- read_csv("mo_coded_2026-01-12.csv") %>%
  mutate(
    ein = as.character(ein),
    submitted_at = as.POSIXct(submitted_at)  # make sure it's a proper datetime
  ) %>%
  group_by(ein) %>%
  slice_max(submitted_at, n = 1, with_ties = FALSE) %>%  # keep the latest row per EIN
  ungroup()

measurement_scores <- measurement_scores %>%
  mutate(
    # =============================================================
    # Invested in Monitoring and Evaluation Capacity (MEDIUM+)
    # =============================================================
    is_eligible_invests_me_capacity = size %in% c("MEDIUM", "LARGE", "SUPER"),
    invests_me_capacity_score = if_else(
      is_eligible_invests_me_capacity,
      case_when(
        plan_training_full == 1 ~ 1,
        plan_training_partial == 1 ~ 0.5,
        plan_training_none == 1 ~ 0,
        TRUE ~ NA_real_
      ),
      NA_real_
    ),
    # =============================================================
    # Established Monitoring and Evaluation System (LARGE+)
    # =============================================================
    is_eligible_me_staff = size %in% c("LARGE", "SUPER"),
    me_staff_score = if_else(
      is_eligible_me_staff,
      case_when(
        plan_staff_full == 1 ~ 1,
        plan_staff_partial == 1 ~ 0.5,
        plan_staff_none == 1 ~ 0,
        TRUE ~ NA_real_
      ),
      NA_real_
    ),
    # =============================================================
    # Understood Barriers to Access (all sizes)
    # =============================================================
    is_eligible_barriers_to_access = TRUE,
    barriers_to_access_score = case_when(
      dev_responsive_full == 1 ~ 1,
      dev_responsive_partial == 1 ~ 0.5,
      dev_responsive_none == 1 ~ 0,
      TRUE ~ NA_real_
    ),
    # =============================================================
    # Defined Target Population (all sizes)
    # =============================================================
    is_eligible_defined_target_population = TRUE,
    defined_target_population_score = (
      collect_program_data_count +
        collect_program_data_demog +
        collect_program_data_outcomes +
        collect_program_data_quality
    ) / 4,
    # =============================================================
    # Data Collected Across Program Lifecycle (all sizes)
    # =============================================================
    is_eligible_program_lifecycle_data = TRUE,
    program_lifecycle_data_score = (
      collect_tracking_before +
        collect_tracking_during +
        collect_tracking_complete
    ) / 3,
    # =============================================================
    # Data Analysis Practices
    # =============================================================
    is_eligible_analysis_practices = TRUE,
    analysis_practices_score = case_when(
      size %in% c("MICRO", "SMALL") ~ case_when(
        collect_frequency_full == 1  ~ 1,
        collect_frequency_partial == 1 ~ 0.5,
        collect_frequency_none == 1 ~ 0,
        TRUE ~ NA_real_
      ),
      size == "MEDIUM" ~ rowSums(
        across(
          c(
            collect_analysis_sumstat,
            collect_analysis_instit,
            collect_analysis_cgroup,
            collect_analysis_multiple,
            collect_analysis_similar,
            collect_analysis_samegeo
          ),
          ~ as.numeric(.x)
        ),
        na.rm = TRUE
      ) / 4,
      size == "LARGE" ~ rowSums(
        across(
          c(
            collect_analysis_sumstat,
            collect_analysis_instit,
            collect_analysis_cgroup,
            collect_analysis_multiple,
            collect_analysis_similar,
            collect_analysis_samegeo
          ),
          ~ as.numeric(.x)
        ),
        na.rm = TRUE
      ) / 5,
      size == "SUPER" ~ rowSums(
        across(
          c(
            collect_analysis_sumstat,
            collect_analysis_instit,
            collect_analysis_cgroup,
            collect_analysis_multiple,
            collect_analysis_similar,
            collect_analysis_samegeo
          ),
          ~ as.numeric(.x)
        ),
        na.rm = TRUE
      ) / 6,
      TRUE ~ NA_real_
    )
  ) %>%
  # Keep only the new variables
  select(
    ein,
    is_eligible_invests_me_capacity,
    invests_me_capacity_score,
    is_eligible_me_staff,
    me_staff_score,
    is_eligible_barriers_to_access,
    barriers_to_access_score,
    is_eligible_defined_target_population,
    defined_target_population_score,
    is_eligible_program_lifecycle_data,
    program_lifecycle_data_score,
    is_eligible_analysis_practices,
    analysis_practices_score
  )