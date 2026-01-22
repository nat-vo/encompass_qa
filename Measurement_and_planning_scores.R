##Measurement data
getwd()
mo_scores <- readr::read_csv("mo_coded_2026-01-12.csv")

mo_scores <- mo_scores %>%
  mutate(
    # Q1
    plan_leadership_score = ifelse(
      plan_leadership_full == 1, 1,
      ifelse(plan_leadership_partial == 1, 0.5,
             ifelse(plan_leadership_none == 1, 0, NA)
      )
    ),
    
    # Q2
    plan_training_score = ifelse(
      size %in% c("MEDIUM", "LARGE", "SUPER"),
      ifelse(plan_training_full == 1, 1,
             ifelse(plan_training_partial == 1, 0.5,
                    ifelse(plan_training_none == 1, 0, NA)
             )
      ),
      NA
    ),
    
    # Q3
    plan_staff_score = ifelse(
      size %in% c("LARGE", "SUPER"),
      ifelse(plan_staff_full == 1, 1,
             ifelse(plan_staff_partial == 1, 0.5,
                    ifelse(plan_staff_none == 1, 0, NA)
             )
      ),
      NA
    ),
    
    # Q4 - shared understanding
    plan_understand_toc_score = ifelse(
      size %in% c("MICRO", "SMALL"),
      ifelse(plan_understand_toc_full == 1, 1,
             ifelse(plan_understand_toc_partial == 1, 0.5,
                    ifelse(plan_understand_toc_none == 1, 0, NA)
             )
      ),
      NA
    ),
    
    # Q4a
    plan_documents_toc_score = ifelse(plan_documents_toc_yes == 1, 1, 0),
    
    # Q4b - raw sum
    plan_researched_toc_score_raw = rowSums(
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
        ~ as.integer(.x)  # Convert TRUE to 1, FALSE/NA to 0
      ),
      na.rm = TRUE
    ),
    
    # Q4b - scaled
    plan_researched_toc_score = ifelse(
      size == "SMALL", plan_researched_toc_score_raw / 5,
      ifelse(size %in% c("MEDIUM", "LARGE", "SUPER"), plan_researched_toc_score_raw / 6, NA)
    ),
    plan_researched_toc_score = ifelse(plan_researched_toc_score > 1, 1, plan_researched_toc_score),
    
    # Q4c
    plan_reviews_toc_score = ifelse(plan_reviews_toc_yes == 1, 1, 0),
    plan_reviews_toc_score = ifelse(
      size %in% c("MEDIUM", "LARGE", "SUPER") & plan_documents_toc_yes == 0 & plan_reviews_toc_yes == 1,
      0,
      ifelse(
        size == "SMALL" & plan_documents_toc_yes == 0 & plan_reviews_toc_yes == 1,
        0.5,
        plan_reviews_toc_score
      )
    ),
    
    # Q5
    plan_tracks_toc_score = plan_tracks_toc_activities +
      plan_tracks_toc_outcomes +
      plan_tracks_toc_timeline +
      plan_tracks_toc_mission,
    plan_tracks_toc_score = ifelse(
      size %in% c("SMALL", "MEDIUM", "LARGE", "SUPER"),
      plan_tracks_toc_score / 4, NA
    ),
    
    # Q6
    dev_needs_score = case_when(
      size %in% c("MEDIUM", "LARGE", "SUPER") ~ pmin(
        rowSums(across(c(
          dev_needs_npconsult,
          dev_needs_me,
          dev_needs_lit,
          dev_needs_interviews,
          dev_needs_assessment,
          dev_needs_baseline
        )), na.rm = TRUE) * 0.25, 
        1
      ),
      
      size %in% c("MICRO", "SMALL") ~
        (dev_needs_targetpop * 0.5) +
        (dev_needs_commleaders * 0.25) +
        (dev_needs_localnps * 0.25),
      
      TRUE ~ NA_real_
    ),
    
    # Cap score at 1
    dev_needs_score = ifelse(dev_needs_score > 1, 1, dev_needs_score),
    
    # Q7
    dev_responsive_score = ifelse(
      dev_responsive_full == 1, 1,
      ifelse(dev_responsive_partial == 1, 0.5,
             ifelse(dev_responsive_none == 1, 0, NA)
      )
    ),
    
    # Q8
    dev_similarorgs_score = ifelse(
      size %in% c("MEDIUM", "LARGE", "SUPER"),
      ifelse(dev_similarorgs_yes == 1, 1,
             ifelse(dev_similarorgs_no == 1, 0, NA)
      ),
      NA
    ),
    
    # Q9
    dev_smartgoals_score = ifelse(
      dev_smartgoals_full == 1, 1,
      ifelse(dev_smartgoals_partial == 1, 0.5,
             ifelse(dev_smartgoals_none == 1, 0, NA)
      )
    ),
    
    # Q10
    collect_tracking_score = (collect_tracking_before +
                                collect_tracking_during +
                                collect_tracking_complete) / 3,
    
    # Q11
    collect_program_data_score = (collect_program_data_count +
                                    collect_program_data_demog +
                                    collect_program_data_outcomes +
                                    collect_program_data_quality) / 4,
    
    # Q12 - Micro/Small
    collect_frequency_score = ifelse(
      size %in% c("MICRO", "SMALL"),
      ifelse(collect_frequency_full == 1, 1,
             ifelse(collect_frequency_partial == 1, 0.5,
                    ifelse(collect_frequency_none == 1, 0, NA)
             )
      ),
      NA
    ),
    
    # Q12 - Medium/Large/Super
    collect_analysis_score = collect_analysis_sumstat +
      collect_analysis_instit +
      collect_analysis_cgroup +
      collect_analysis_multiple +
      collect_analysis_similar +
      collect_analysis_samegeo,
    
    collect_analysis_score = ifelse(
      size %in% c("MICRO", "SMALL"), NA,
      ifelse(size == "MEDIUM", collect_analysis_score / 4,
             ifelse(size == "LARGE", collect_analysis_score / 5,
                    ifelse(size == "SUPER", collect_analysis_score / 6, NA)
             )
      )
    ),
    collect_analysis_score = ifelse(collect_analysis_score > 1, 1, collect_analysis_score),
    
    # Q13
    report_stakeholders_score = ifelse(
      size %in% c("MICRO", "SMALL"),
      ifelse(report_stakeholders_yes == 1, 1, 0),
      ifelse(
        size %in% c("MEDIUM", "LARGE", "SUPER"),
        (
          report_stakeholders_funders +
            report_stakeholders_staff +
            report_stakeholders_board +
            report_stakeholders_targetpop +
            report_stakeholders_otherorgs +
            report_stakeholders_public
        ) / 6,
        NA
      )
    ),
    
    # Q14
    report_all_findings_score = ifelse(
      report_all_findings_yes == 1, 1,
      ifelse(report_all_findings_no == 1, 0, NA)
    ),
    
    # Q15
    results_use_score = (
      results_use_funding +
        results_use_planning +
        results_use_operations +
        results_use_impact +
        results_use_futureprog
    ) / 5
  ) %>%
  arrange(size) %>%
  select(
    ein, size, mo_outcome_area, mo_selected_program, submitted_at,
    ends_with("_score")
  )

######################################