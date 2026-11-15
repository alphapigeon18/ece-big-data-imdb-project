# IMDb big data project - report

<!--
  Result tables between "RESULT" markers are generated:
      docker exec imdb_spark python scripts/update_report.py
  Never edit inside the markers by hand: the script overwrites them.

  All explanations, methodology and analysis in this file must be written
  by the team WITHOUT any AI tool (course rule).
-->

## Dataset

<!-- TODO (team): IMDb snapshot date, file sizes, number of rows. -->

## 1. Data preparation

### 1.1 Missing values per column

<!-- RESULT:preparation/null_profile -->
<!-- /RESULT -->

### 1.2 Five columns with the highest percentage of missing values

<!-- RESULT:preparation/top5_null_columns -->
<!-- /RESULT -->

### 1.3 Other data quality issues

<!-- RESULT:preparation/quality_issues -->
<!-- /RESULT -->

### 1.4 Handling of the issues

<!-- RESULT:preparation/cleaning_summary -->
<!-- /RESULT -->

### Methodological explanation - preparation

<!-- TODO (team): methodology and why it was chosen. -->

<!-- TODO (team): alternatives considered and trade-offs. -->

## 2. Data exploration

### 2.1 Feature films with an atypical runtime for their genre

<!-- RESULT:exploration/q1_genre_runtime_stats -->
<!-- /RESULT -->

<!-- RESULT:exploration/q1_runtime_outliers -->
<!-- /RESULT -->

### 2.2 Top three genres per year (1980-2025)

<!-- RESULT:exploration/q2_top3_genres_per_year -->
<!-- /RESULT -->

### 2.3 Top three genres compared with the other genres

<!-- RESULT:exploration/q3_top3_vs_rest_by_decade -->
<!-- /RESULT -->

<!-- RESULT:exploration/q3_genre_top3_frequency -->
<!-- /RESULT -->

<!-- RESULT:exploration/q3_top3_vs_rest_per_year -->
<!-- /RESULT -->

### 2.4 Directors with exactly one film rated >= 8.0 and several <= 6.0

<!-- RESULT:exploration/q4_one_hit_directors -->
<!-- /RESULT -->

### 2.5 Series: best episode early, worst episode in the last season

<!-- RESULT:exploration/q5_series_best_early_worst_last -->
<!-- /RESULT -->

### 2.6 Actors successful in several languages

<!-- RESULT:exploration/q6_actors_2plus_languages -->
<!-- /RESULT -->

<!-- RESULT:exploration/q6_actors_3plus_languages -->
<!-- /RESULT -->

<!-- RESULT:exploration/q6_language_pairs -->
<!-- /RESULT -->

### 2.7 Long careers that peaked early

<!-- RESULT:exploration/q7_early_peak_share -->
<!-- /RESULT -->

<!-- RESULT:exploration/q7_early_peak_careers -->
<!-- /RESULT -->

### 2.8 Multi-talented professionals

<!-- RESULT:exploration/q8_role_combination_performance -->
<!-- /RESULT -->

<!-- RESULT:exploration/q8_paired_comparison -->
<!-- /RESULT -->

<!-- RESULT:exploration/q8_top_multi_talented -->
<!-- /RESULT -->

### Methodological explanation - exploration

<!-- TODO (team): methodology and why it was chosen, per question. -->

<!-- TODO (team): alternatives considered and trade-offs. -->

## 3. Data enrichment

<!-- RESULT:enrichment/coverage -->
<!-- /RESULT -->

<!-- RESULT:enrichment/notes_breakdown -->
<!-- /RESULT -->

<!-- RESULT:enrichment/sample_films -->
<!-- /RESULT -->

<!-- RESULT:enrichment/sample_principals -->
<!-- /RESULT -->

### Methodological explanation - enrichment

<!-- TODO (team): source choice, rate limits, lineage, Spark steps. -->

<!-- TODO (team): alternatives considered and trade-offs. -->

## 4. Streaming

<!-- RESULT:streaming/run_metrics -->
<!-- /RESULT -->

<!-- RESULT:streaming/query_progress -->
<!-- /RESULT -->

<!-- RESULT:streaming/match_rate_by_window -->
<!-- /RESULT -->

<!-- RESULT:streaming/window_counts -->
<!-- /RESULT -->

<!-- RESULT:streaming/top_entities -->
<!-- /RESULT -->

<!-- RESULT:streaming/sample_matches -->
<!-- /RESULT -->

### Handling of late-arriving data

<!-- TODO (team). -->

### Methodological explanation - streaming

<!-- TODO (team): methodology and why it was chosen. -->

<!-- TODO (team): alternatives considered and trade-offs. -->

## 5. Open question

### 5.1 Question, motivation and data

<!-- TODO (team). -->

### 5.2 Results

<!-- RESULT:open_question/director_paired_comparison -->
<!-- /RESULT -->

<!-- RESULT:open_question/lift_by_collaboration_index -->
<!-- /RESULT -->

<!-- RESULT:open_question/lift_by_start_decade -->
<!-- /RESULT -->

<!-- RESULT:open_question/top_partnerships -->
<!-- /RESULT -->

### 5.3 Key takeaways

<!-- TODO (team). -->

### 5.4 Methodology

<!-- TODO (team): methodology, why it was chosen, alternatives. -->
