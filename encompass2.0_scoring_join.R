# Set working directory
setwd("/Users/natalievolin/Library/CloudStorage/GoogleDrive-nvolin@charitynavigator.org/Shared drives/Encompass Rating /Beacon | Impact & Measurement/2 | Ratings/2.1 | Ratings Production/R code for ratings production/4. Other R code/Encompass2.0_scoring")

# Load required libraries
library(tidyr)
library(readr)
library(RPostgres)
library(DBI)
library(dplyr)

# Connect to the prod database
con <- dbConnect(
  RPostgres::Postgres(),
  dbname   = Sys.getenv("Postgresql_DBNAME"),
  host     = Sys.getenv("Postgresql_HOST"),
  port     = Sys.getenv("Postgresql_PORT"),
  user     = Sys.getenv("Postgresql_USER"),
  password = Sys.getenv("Postgresql_PASSWORD")
)
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


library(dplyr)

encompass_joined <- encompass %>%
  # Join Form 990 scores
  left_join(form990_scores,    by = c("ein_rollup" = "ein_rollup")) %>%
  # Join Leadership scores
  left_join(leadership_scores, by = c("ein_rollup" = "ein")) %>%
  # Join WPP scores
  left_join(wpp_scores,        by = c("ein_rollup" = "ein")) %>%
  # Join Program Planning scores
  left_join(program_planning_scores, by = c("ein_rollup" = "ein")) %>%
  # Join Measurement scores
  left_join(measurement_scores, by = c("ein_rollup" = "ein")) %>%
  # Join Learning scores
  left_join(learning_scores,   by = c("ein_rollup" = "ein")) %>%
  # Join Impact PSA scores
  left_join(impact_psa_scores, by = c("ein_rollup" = "ein"))

# List column names that start with 'is_eligible' or end with '_score'
eligible_and_score_cols <- encompass_joined %>%
  select(
    starts_with("is_eligible"),
    ends_with("_score")
  ) %>%
  colnames()

# Print
eligible_and_score_cols

library(tibble)

enc2_ref_weight_metrics <- tribble(
  ~enc2_ref_weight_metrics, ~weight,
  "mission_vision_goals_weight", 1.00,
  "leadership_investment_weight", 2.00,
  "external_engagement_weight", 2.00,
  "adaptability_weight", 0.50,
  "understand_toc_weight", 2.00,
  "toc_practices_weight", 3.00,
  "smartgoals_weight", 3.00,
  "me_practices_weight", 2.00,
  "needs_assessment_weight", 2.00,
  "board_approved_budget_weight", 2.00,
  "independent_board_size_weight", 3.00,
  "independent_board_weight", 3.00,
  "board_minutes_weight", 1.50,
  "form990_board_provided_weight", 1.50,
  "program_exp_weight", 12.50,
  "lta_weight", 12.50,
  "wkgcap_weight", 12.50,
  "fundeff_weight", 12.50,
  "url_on_990_weight", 1.50,
  "material_diversion_weight", 6.00,
  "financial_statements_weight", 4.50,
  "audit_committee_weight", 1.50,
  "loans_to_from_officers_weight", 2.50,
  "document_retention_policy_weight", 2.50,
  "form990_on_website_weight", 2.50,
  "web_disclosure_weight", 7.50,
  "compensates_board_weight", 2.50,
  "ceo_on_990_weight", 2.50,
  "ceo_compensation_policy_weight", 2.50,
  "fringe_rate_weight", 2.00,
  "promotional_pay_policy_weight", 2.00,
  "reviewed_compensation_structure_weight", 1.00,
  "whistleblower_policy_weight", 2.50,
  "conflict_of_interest_policy_weight", 2.50,
  "workplace_policies_weight", 5.00,
  "hiring_employee_performance_practices_weight", 5.00,
  "invests_me_capacity_weight", 0.50,
  "me_staff_weight", 1.00,
  "barriers_to_access_weight", 3.00,
  "defined_target_population_weight", 2.00,
  "program_lifecycle_data_weight", 2.00,
  "analysis_practices_weight", 2.00,
  "leadership_commitment_weight", 1.50,
  "report_to_stakeholders_weight", 3.00,
  "results_disclosure_weight", 3.00,
  "use_results_weight", 3.00,
  "feedback_collection_weight", 4.00,
  "feedback_practices_weight", 1.71,
  "cem_weight", 25.00,
  "ra_weight", 35.00
)

library(dplyr)
library(tibble)

# Create named weight vector
weight_vec <- setNames(enc2_ref_weight_metrics$weight,
                       enc2_ref_weight_metrics$enc2_ref_weight_metrics)

# Compute subscores
encompass_temp <- encompass_joined %>%
  mutate(
    workplace_policies_subscore = is_eligible_workplace_policies * workplace_policies_score * weight_vec["workplace_policies_weight"],
    promotional_pay_policy_subscore = is_eligible_promotional_pay_policy * promotional_pay_policy_score * weight_vec["promotional_pay_policy_weight"],
    reviewed_compensation_structure_subscore = is_eligible_reviewed_compensation_structure * reviewed_compensation_structure_score * weight_vec["reviewed_compensation_structure_weight"],
    hiring_employee_performance_practices_subscore = is_eligible_hiring_employee_performance_practices * hiring_employee_performance_practices_score * weight_vec["hiring_employee_performance_practices_weight"],
    fundeff_subscore = is_eligible_fundeff * fundeff_score * weight_vec["fundeff_weight"],
    lta_subscore = is_eligible_lta * lta_score * weight_vec["lta_weight"],
    program_exp_subscore = is_eligible_program_expense * program_exp_score * weight_vec["program_exp_weight"],
    wkgcap_subscore = is_eligible_wkgcap * wkgcap_score * weight_vec["wkgcap_weight"],
    url_on_990_subscore = is_eligible_url_on_990 * url_on_990_score * weight_vec["url_on_990_weight"],
    material_diversion_subscore = is_eligible_material_diversion * material_diversion_score * weight_vec["material_diversion_weight"],
    financial_statements_subscore = is_eligible_financial_statements * financial_statements_score * weight_vec["financial_statements_weight"],
    audit_committee_subscore = is_eligible_audit_committee * audit_committee_score * weight_vec["audit_committee_weight"],
    loans_to_from_officers_subscore = is_eligible_loans_to_from_officers * loans_to_from_officers_score * weight_vec["loans_to_from_officers_weight"],
    document_retention_policy_subscore = is_eligible_document_retention_policy * document_retention_policy_score * weight_vec["document_retention_policy_weight"],
    form990_on_website_subscore = is_eligible_990_on_website * form990_on_website_score * weight_vec["form990_on_website_weight"],
    web_disclosure_subscore = is_eligible_web_disclosure * web_disclosure_score * weight_vec["web_disclosure_weight"],
    compensates_board_subscore = is_eligible_compensates_board * compensates_board_score * weight_vec["compensates_board_weight"],
    ceo_compensation_policy_subscore = is_eligible_ceo_compensation_policy * ceo_compensation_policy_score * weight_vec["ceo_compensation_policy_weight"],
    ceo_on_990_subscore = is_eligible_ceo_and_compensation_on_990 * ceo_on_990_score * weight_vec["ceo_on_990_weight"],
    whistleblower_policy_subscore = is_eligible_whistleblower_policy * whistleblower_policy_score * weight_vec["whistleblower_policy_weight"],
    conflict_of_interest_policy_subscore = is_eligible_conflict_of_interest_policy * conflict_of_interest_policy_score * weight_vec["conflict_of_interest_policy_weight"],
    independent_board_size_subscore = is_eligible_independent_board_size * independent_board_size_score * weight_vec["independent_board_size_weight"],
    independent_board_subscore = is_eligible_independent_board * independent_board_score * weight_vec["independent_board_weight"],
    board_minutes_subscore = is_eligible_board_minutes * board_minutes_score * weight_vec["board_minutes_weight"],
    form990_board_provided_subscore = is_eligible_990_board_provided * form990_board_provided_score * weight_vec["form990_board_provided_weight"],
    fringe_rate_subscore = is_eligible_fringe_rate * fringe_rate_score * weight_vec["fringe_rate_weight"],
    mission_vision_goals_subscore = is_eligible_mission_vision_goals * mission_vision_goals_score * weight_vec["mission_vision_goals_weight"],
    leadership_investment_subscore = is_eligible_leadership_investment * leadership_investment_score * weight_vec["leadership_investment_weight"],
    external_engagement_subscore = is_eligible_external_engagement * external_engagement_score * weight_vec["external_engagement_weight"],
    adaptability_subscore = is_eligible_adaptability * adaptability_score * weight_vec["adaptability_weight"],
    board_approved_budget_subscore = is_eligible_board_approved_budget * board_approved_budget_score * weight_vec["board_approved_budget_weight"],
    understand_toc_subscore = is_eligible_understand_toc * understand_toc_score * weight_vec["understand_toc_weight"],
    toc_practices_subscore = is_eligible_toc_practices * toc_practices_score * weight_vec["toc_practices_weight"],
    me_practices_subscore = is_eligible_me_practices * me_practices_score * weight_vec["me_practices_weight"],
    smartgoals_subscore = is_eligible_smartgoals * smartgoals_score * weight_vec["smartgoals_weight"],
    needs_assessment_subscore = is_eligible_needs_assessment * needs_assessment_score * weight_vec["needs_assessment_weight"],
    invests_me_capacity_subscore = is_eligible_invests_me_capacity * invests_me_capacity_score * weight_vec["invests_me_capacity_weight"],
    me_staff_subscore = is_eligible_me_staff * me_staff_score * weight_vec["me_staff_weight"],
    barriers_to_access_subscore = is_eligible_barriers_to_access * barriers_to_access_score * weight_vec["barriers_to_access_weight"],
    defined_target_population_subscore = is_eligible_defined_target_population * defined_target_population_score * weight_vec["defined_target_population_weight"],
    program_lifecycle_data_subscore = is_eligible_program_lifecycle_data * program_lifecycle_data_score * weight_vec["program_lifecycle_data_weight"],
    analysis_practices_subscore = is_eligible_analysis_practices * analysis_practices_score * weight_vec["analysis_practices_weight"],
    leadership_commitment_subscore = is_eligible_leadership_commitment * leadership_commitment_score * weight_vec["leadership_commitment_weight"],
    report_to_stakeholders_subscore = is_eligible_report_to_stakeholders * report_to_stakeholders_score * weight_vec["report_to_stakeholders_weight"],
    results_disclosure_subscore = is_eligible_results_disclosure * results_disclosure_score * weight_vec["results_disclosure_weight"],
    use_results_subscore = is_eligible_use_results * use_results_score * weight_vec["use_results_weight"],
    feedback_collection_subscore = is_eligible_feedback_collection * feedback_collection_score * weight_vec["feedback_collection_weight"],
    feedback_practices_subscore = is_eligible_feedback_practices * feedback_practices_score * weight_vec["feedback_practices_weight"],
    cem_subscore = is_eligible_cem * cem_score * weight_vec["cem_weight"],
    ra_subscore = is_eligible_ra * ra_score * weight_vec["ra_weight"]
  )

# Compute possible points
encompass_temp <- encompass_temp %>%
  mutate(
    workplace_policies_possible = is_eligible_workplace_policies * weight_vec["workplace_policies_weight"],
    promotional_pay_policy_possible = is_eligible_promotional_pay_policy * weight_vec["promotional_pay_policy_weight"],
    reviewed_compensation_structure_possible = is_eligible_reviewed_compensation_structure * weight_vec["reviewed_compensation_structure_weight"],
    hiring_employee_performance_practices_possible = is_eligible_hiring_employee_performance_practices * weight_vec["hiring_employee_performance_practices_weight"],
    fundeff_possible = is_eligible_fundeff * weight_vec["fundeff_weight"],
    lta_possible = is_eligible_lta * lta_score * weight_vec["lta_weight"],
    program_exp_possible = is_eligible_program_expense * weight_vec["program_exp_weight"],
    wkgcap_possible = is_eligible_wkgcap * weight_vec["wkgcap_weight"],
    url_on_990_possible = is_eligible_url_on_990 * weight_vec["url_on_990_weight"],
    material_diversion_possible = is_eligible_material_diversion * weight_vec["material_diversion_weight"],
    financial_statements_possible = is_eligible_financial_statements * weight_vec["financial_statements_weight"],
    audit_committee_possible = is_eligible_audit_committee * weight_vec["audit_committee_weight"],
    loans_to_from_officers_possible = is_eligible_loans_to_from_officers * weight_vec["loans_to_from_officers_weight"],
    document_retention_policy_possible = is_eligible_document_retention_policy * weight_vec["document_retention_policy_weight"],
    form990_on_website_possible = is_eligible_990_on_website * weight_vec["form990_on_website_weight"],
    web_disclosure_possible = is_eligible_web_disclosure * weight_vec["web_disclosure_weight"],
    compensates_board_possible = is_eligible_compensates_board * weight_vec["compensates_board_weight"],
    ceo_compensation_policy_possible = is_eligible_ceo_compensation_policy * weight_vec["ceo_compensation_policy_weight"],
    ceo_on_990_possible = is_eligible_ceo_and_compensation_on_990 * weight_vec["ceo_on_990_weight"],
    whistleblower_policy_possible = is_eligible_whistleblower_policy * weight_vec["whistleblower_policy_weight"],
    conflict_of_interest_policy_possible = is_eligible_conflict_of_interest_policy * weight_vec["conflict_of_interest_policy_weight"],
    independent_board_size_possible = is_eligible_independent_board_size * weight_vec["independent_board_size_weight"],
    independent_board_possible = is_eligible_independent_board * weight_vec["independent_board_weight"],
    board_minutes_possible = is_eligible_board_minutes * weight_vec["board_minutes_weight"],
    form990_board_provided_possible = is_eligible_990_board_provided * weight_vec["form990_board_provided_weight"],
    fringe_rate_possible = is_eligible_fringe_rate * weight_vec["fringe_rate_weight"],
    mission_vision_goals_possible = is_eligible_mission_vision_goals * weight_vec["mission_vision_goals_weight"],
    leadership_investment_possible = is_eligible_leadership_investment * weight_vec["leadership_investment_weight"],
    external_engagement_possible = is_eligible_external_engagement * weight_vec["external_engagement_weight"],
    adaptability_possible = is_eligible_adaptability * weight_vec["adaptability_weight"],
    board_approved_budget_possible = is_eligible_board_approved_budget * weight_vec["board_approved_budget_weight"],
    understand_toc_possible = is_eligible_understand_toc * weight_vec["understand_toc_weight"],
    toc_practices_possible = is_eligible_toc_practices * weight_vec["toc_practices_weight"],
    me_practices_possible = is_eligible_me_practices * weight_vec["me_practices_weight"],
    smartgoals_possible = is_eligible_smartgoals * weight_vec["smartgoals_weight"],
    needs_assessment_possible = is_eligible_needs_assessment * weight_vec["needs_assessment_weight"],
    invests_me_capacity_possible = is_eligible_invests_me_capacity * weight_vec["invests_me_capacity_weight"],
    me_staff_possible = is_eligible_me_staff * weight_vec["me_staff_weight"],
    barriers_to_access_possible = is_eligible_barriers_to_access * weight_vec["barriers_to_access_weight"],
    defined_target_population_possible = is_eligible_defined_target_population * weight_vec["defined_target_population_weight"],
    program_lifecycle_data_possible = is_eligible_program_lifecycle_data * weight_vec["program_lifecycle_data_weight"],
    analysis_practices_possible = is_eligible_analysis_practices * weight_vec["analysis_practices_weight"],
    leadership_commitment_possible = is_eligible_leadership_commitment * weight_vec["leadership_commitment_weight"],
    report_to_stakeholders_possible = is_eligible_report_to_stakeholders * weight_vec["report_to_stakeholders_weight"],
    results_disclosure_possible = is_eligible_results_disclosure * weight_vec["results_disclosure_weight"],
    use_results_possible = is_eligible_use_results * weight_vec["use_results_weight"],
    feedback_collection_possible = is_eligible_feedback_collection * weight_vec["feedback_collection_weight"],
    feedback_practices_possible = is_eligible_feedback_practices * weight_vec["feedback_practices_weight"],
    cem_possible = is_eligible_cem * weight_vec["cem_weight"],
    ra_possible = is_eligible_ra * weight_vec["ra_weight"]
  )

encompass_temp <- encompass_temp %>%
  mutate(
    e2_total_score =
      rowSums(dplyr::select(., ends_with("_subscore")), na.rm = TRUE) /
      rowSums(dplyr::select(., ends_with("_possible")), na.rm = TRUE)
  )





ein_keep <- stringr::str_pad(
  c(
    134148824, 133433452, 273521132, 951831116, 530196605, 363673599, 131644147,
    135660870, 351044585, 854316822, 136213516, 931057665, 20554654,
    131760110, 202370934, 61008595, 132654926, 630598743, 581437002,
    990086524, 116107128, 631135091, 620646012, 131790719, 530242652,
    990315110, 271661997, 262414132, 237069110, 521521276, 133039601,
    131685039, 522396428, 363241033, 362193608, 131623829, 43512550,
    131656634, 133843435, 134141945, 946069890, 362971864, 954681287,
    300108263, 581454716, 351019477, 311348100, 521693387, 133500609,
    131788491, 941730465, 133661416, 10471949, 363256096, 943074600,
    270819276, 133859563, 113158401, 951922279, 800587086, 943041517,
    520851555, 996000953, 201407520, 260086305, 911792864, 571116978,
    135613797, 131837442, 133327220, 133156445, 911148123, 431201653,
    471442997, 954453134, 113533002, 131818723, 131624102, 881006161,
    846038762, 135633307, 840772672, 912154267, 481108359, 521167581,
    455257937, 520882226, 990261283, 930785786, 300148338, 133669731,
    815132355, 362586390, 133170676, 930792021, 521573446, 42535767,
    953135649, 510172429, 133240109, 453782061, 593097333, 521224516,
    112494808, 42263040, 133072967, 521388917, 742345786, 942614101,
    521838756, 751785357, 237377505, 311728910, 320077563, 330412751,
    237098123, 812187309, 202923277, 60646959, 521100361, 203069841,
    460414390, 541254491, 133727250, 300108469, 135563422, 237147797,
    521086761, 742181456, 200471604, 237245152, 135562351, 521257057,
    363145764, 133393329, 131659627, 42121377, 530225390, 135644916,
    131655255, 811401967, 237420660, 953313195, 510401308, 593051533,
    582060131, 222584370, 411601449, 133969389, 521322317, 203414952,
    743008249, 530242962, 364476244, 132621497, 311628040, 464638549,
    364219778, 542033897, 471837509, 204374795, 411750692, 463614979,
    382156255, 133271855, 133779611, 910826037, 131740011, 931140967,
    20539734, 330004099, 680051386, 273943866, 581376648, 521135690,
    461657101, 133179546, 133127972, 203021444, 986001029, 300060905,
    770155782, 861536473, 131845455, 853973880, 870212453, 363292607,
    911275815, 454985621, 330841281, 870569356, 542076145, 131866796,
    236393344, 237447812, 223936753, 208625442, 530225165, 870212465,
    770459884, 521238307, 561283426, 135655952, 850203522, 900874591
  ),
  width = 9,
  pad = "0"
)

encompass_subset <- encompass_temp %>%
  mutate(
    ein_rollup = stringr::str_pad(as.character(ein_rollup), 9, pad = "0")
  ) %>%
  filter(ein_rollup %in% ein_keep)



write.csv(encompass_subset, file = "e2_scores_sample.csv", row.names = FALSE)



