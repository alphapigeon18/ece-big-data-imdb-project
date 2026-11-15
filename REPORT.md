# IMDb big data project - report

<!--
  Result tables between "RESULT" markers are generated:
      docker exec imdb_spark python scripts/update_report.py
  Never edit inside the markers by hand: the script overwrites them.
-->

## Dataset

For this project, we worked with the seven files of the IMDb non-commercial
datasets (`name.basics`, `title.basics`, `title.akas`, `title.crew`,
`title.episode`, `title.principals`, `title.ratings`), downloaded on
14 November 2026. The row counts for each table are given in the
`total_rows` column of the table in section 1.1. All Spark jobs ran on a
single-node Spark 4 container with 2 GB of driver memory (out of 3.7 GB
available to Docker on our laptop), which shaped several technical choices
described below.

## 1. Data preparation

### 1.1 Missing values per column

<!-- RESULT:preparation/null_profile -->

### Missing values per column

| table | column | total_rows | missing | pct_missing |
| --- | --- | --- | --- | --- |
| name_basics | birthYear | 15814379 | 15128508 | 95.663 |
| name_basics | deathYear | 15814379 | 15548850 | 98.321 |
| name_basics | knownForTitles | 15814379 | 1917390 | 12.124 |
| name_basics | nconst | 15814379 | 0 | 0.000 |
| name_basics | primaryName | 15814379 | 272 | 0.002 |
| name_basics | primaryProfession | 15814379 | 3220155 | 20.362 |
| title_akas | attributes | 60424702 | 60107437 | 99.475 |
| title_akas | isOriginalTitle | 60424702 | 0 | 0.000 |
| title_akas | language | 60424702 | 19665115 | 32.545 |
| title_akas | ordering | 60424702 | 0 | 0.000 |
| title_akas | region | 60424702 | 13002884 | 21.519 |
| title_akas | title | 60424702 | 0 | 0.000 |
| title_akas | titleId | 60424702 | 0 | 0.000 |
| title_akas | types | 60424702 | 40393110 | 66.849 |
| title_basics | endYear | 12963111 | 12798642 | 98.731 |
| title_basics | genres | 12963111 | 548144 | 4.228 |
| title_basics | isAdult | 12963111 | 0 | 0.000 |
| title_basics | originalTitle | 12963111 | 0 | 0.000 |
| title_basics | primaryTitle | 12963111 | 0 | 0.000 |
| title_basics | runtimeMinutes | 12963111 | 8050574 | 62.104 |
| title_basics | startYear | 12963111 | 1487320 | 11.473 |
| title_basics | tconst | 12963111 | 0 | 0.000 |
| title_basics | titleType | 12963111 | 0 | 0.000 |
| title_crew | directors | 12963111 | 5793144 | 44.689 |
| title_crew | tconst | 12963111 | 0 | 0.000 |
| title_crew | writers | 12963111 | 6434239 | 49.635 |
| title_episode | episodeNumber | 10034862 | 2126237 | 21.189 |
| title_episode | parentTconst | 10034862 | 0 | 0.000 |
| title_episode | seasonNumber | 10034862 | 2126237 | 21.189 |
| title_episode | tconst | 10034862 | 0 | 0.000 |
| title_principals | category | 102756358 | 0 | 0.000 |
| title_principals | characters | 102756358 | 52691057 | 51.278 |
| title_principals | job | 102756358 | 82882023 | 80.659 |
| title_principals | nconst | 102756358 | 0 | 0.000 |
| title_principals | ordering | 102756358 | 0 | 0.000 |
| title_principals | tconst | 102756358 | 0 | 0.000 |
| title_ratings | averageRating | 1732169 | 0 | 0.000 |
| title_ratings | numVotes | 1732169 | 0 | 0.000 |
| title_ratings | tconst | 1732169 | 0 | 0.000 |

_39 of 39 rows shown._

<!-- /RESULT -->

### 1.2 Five columns with the highest percentage of missing values

<!-- RESULT:preparation/top5_null_columns -->

### Top 5 columns by % missing

| table | column | total_rows | missing | pct_missing |
| --- | --- | --- | --- | --- |
| title_akas | attributes | 60424702 | 60107437 | 99.475 |
| title_basics | endYear | 12963111 | 12798642 | 98.731 |
| name_basics | deathYear | 15814379 | 15548850 | 98.321 |
| name_basics | birthYear | 15814379 | 15128508 | 95.663 |
| title_principals | job | 102756358 | 82882023 | 80.659 |

_5 of 5 rows shown._

<!-- /RESULT -->

### 1.3 Other data quality issues

<!-- RESULT:preparation/quality_issues -->

### Data quality issues (staged layer)

| rule | table | kind | description | n_rows | pct_of_table | examples |
| --- | --- | --- | --- | --- | --- | --- |
| death_before_birth | name_basics | row | deathYear < birthYear | 28 | 0.000 | nm0012634, nm0012635, nm0034922, nm0174595, nm0202552 |
| birth_in_future | name_basics | row | birthYear > 2026 | 0 | 0.000 |  |
| death_in_future | name_basics | row | deathYear > 2026 | 0 | 0.000 |  |
| lifespan_over_120 | name_basics | row | deathYear - birthYear > 120, or alive and born > 120 years ago | 2796 | 0.018 | nm0005637, nm0007417, nm0007643, nm0008495, nm0009123 |
| end_before_start | title_basics | row | endYear < startYear | 0 | 0.000 |  |
| start_in_future | title_basics | row | startYear > 2026 (announced titles) | 6238 | 0.048 | tt0418104, tt0578944, tt0578945, tt0578947, tt10275878 |
| runtime_non_positive | title_basics | row | runtimeMinutes <= 0 | 4 | 0.000 | tt0348126, tt0454640, tt0830188, tt0926344 |
| runtime_over_1000 | title_basics | row | runtimeMinutes > 1000 | 442 | 0.003 | tt0004052, tt0096679, tt0140739, tt0144722, tt0165127 |
| invalid_is_adult | title_basics | row | isAdult not in (0, 1) | 0 | 0.000 |  |
| rating_out_of_range | title_ratings | row | averageRating not in [1, 10] | 0 | 0.000 |  |
| votes_non_positive | title_ratings | row | numVotes <= 0 | 0 | 0.000 |  |
| missing_season_number | title_episode | row | seasonNumber is null | 2126237 | 21.189 | tt0031458, tt0042889, tt0046150, tt0053966, tt0058302 |
| ratings_orphan_title | title_ratings | foreign_key | rows of title_ratings.tconst missing in title_basics.tconst | 0 | 0.000 |  |
| principals_orphan_title | title_principals | foreign_key | rows of title_principals.tconst missing in title_basics.tconst | 0 | 0.000 |  |
| principals_orphan_person | title_principals | foreign_key | rows of title_principals.nconst missing in name_basics.nconst | 8115 | 0.008 | nm16528874, nm17752473, nm17752582, nm17752595, nm17752595 |
| crew_orphan_title | title_crew | foreign_key | rows of title_crew.tconst missing in title_basics.tconst | 0 | 0.000 |  |
| crew_orphan_director | title_crew | foreign_key | values of title_crew.directors missing in name_basics.nconst | 483 | 0.004 | nm17752595, nm17754813, nm17754887, nm17756181, nm17756181 |
| crew_orphan_writer | title_crew | foreign_key | values of title_crew.writers missing in name_basics.nconst | 1635 | 0.013 | nm17754813, nm17755016, nm17756250, nm17756392, nm17756826 |
| episode_orphan_title | title_episode | foreign_key | rows of title_episode.tconst missing in title_basics.tconst | 0 | 0.000 |  |
| episode_orphan_parent | title_episode | foreign_key | rows of title_episode.parentTconst missing in title_basics.tconst | 5 | 0.000 | tt28606619, tt28606619, tt28606619, tt28606619, tt28606619 |
| akas_orphan_title | title_akas | foreign_key | rows of title_akas.titleId missing in title_basics.tconst | 8 | 0.000 | tt51017171, tt51013141, tt51013141, tt51013141, tt51013410 |
| known_for_orphan_title | name_basics | foreign_key | values of name_basics.knownForTitles missing in title_basics.tconst | 49 | 0.000 | tt50969120, tt50969120, tt50969120, tt50969120, tt50969120 |
| credited_before_birth | title_principals | cross_table | title startYear < person birthYear (archive footage excluded) | 3099 | 0.003 | tt0004194, tt0004210, tt0004712, tt0005313, tt0005314 |
| credited_long_after_death | title_principals | cross_table | title startYear > deathYear + 5 (archive footage excluded) | 206982 | 0.201 | tt0000443, tt0000609, tt0000668, tt0000776, tt0000844 |
| episode_before_series | title_episode | cross_table | episode startYear < parent series startYear | 3582 | 0.036 | tt0720292, tt10948728, tt10963686, tt10965276, tt10978860 |
| episode_parent_not_series | title_episode | cross_table | parent title is not a tvSeries / tvMiniSeries | 0 | 0.000 |  |
| episode_not_tv_episode | title_episode | cross_table | episode tconst whose titleType is not tvEpisode | 0 | 0.000 |  |
| duplicate_name_basics_key | name_basics | duplicate | primary key (nconst) not unique | 0 | 0.000 |  |
| duplicate_title_akas_key | title_akas | duplicate | primary key (titleId, ordering) not unique | 0 | 0.000 |  |
| duplicate_title_basics_key | title_basics | duplicate | primary key (tconst) not unique | 0 | 0.000 |  |
| duplicate_title_crew_key | title_crew | duplicate | primary key (tconst) not unique | 0 | 0.000 |  |
| duplicate_title_episode_key | title_episode | duplicate | primary key (tconst) not unique | 0 | 0.000 |  |
| duplicate_title_principals_key | title_principals | duplicate | primary key (tconst, ordering) not unique | 0 | 0.000 |  |
| duplicate_title_ratings_key | title_ratings | duplicate | primary key (tconst) not unique | 0 | 0.000 |  |

_34 of 34 rows shown._

<!-- /RESULT -->

### 1.4 Handling of the issues

During execution, `clean.py` successfully produced the cleaned layer for
`name_basics`, `title_basics`, `title_ratings`, and `title_principals`,
before running out of memory on `title_crew` and stopping. As a result, its
final summary table could not be generated (see the methodological details
below).

<!-- RESULT:preparation/cleaning_summary -->

_Result not generated yet._

<!-- /RESULT -->

### Methodological explanation - preparation

To avoid losing information during ingestion, we first read the raw TSV
files with every column typed as a string. Doing so lets us catch missing
values in all their forms (real nulls, the IMDb `\N` marker, or empty
strings) before any type casting hides them. We compute the missing-value
profile with a single aggregation per table so that each file is scanned
only once.

Since gzipped TSV files are not splittable, Spark can only read them on a
single thread. Rather than paying that cost on every query, we convert the
raw files once into typed Parquet (`stage.py`), and all downstream jobs
read from this faster columnar layer.

All data quality rules are centralised in `jobs/common/quality.py` and
shared by both the diagnostic script (`quality_checks.py`) and the cleaning
pipeline (`clean.py`), ensuring that what we report is strictly what we fix.
These checks cover four areas:
- single-row checks (death year earlier than birth year, future years,
  non-positive runtimes, ratings out of range);
- foreign-key checks (orphan titles or people in child tables and inside
  comma-separated list columns);
- cross-table checks (people credited on a title released before their birth
  or long after their death, episodes older than their series);
- duplicate primary keys.

Depending on the nature of the anomaly, we apply four cleaning actions:
- clearly impossible values are set to `null` and tagged in a `dq_flags`
  column (for example, both `birthYear` and `deathYear` when
  `deathYear < birthYear`, because we cannot know which one is wrong);
- suspicious but plausible values (announced titles in the future, runtimes
  over 1,000 minutes, people older than 120) are kept unchanged and simply
  flagged;
- rows whose foreign key has no parent are moved to `data/quarantine/`
  along with the rejection reason instead of being silently deleted;
- duplicate primary keys are removed.

We considered dropping every row containing an error, which would have been
simpler, but the share of affected rows is very small and their remaining
columns are still useful (a person with an invalid birth year still has
valid film credits). Imputing values, such as filling missing birth years
with a median, was also rejected because it would create artificial data.
Moving invalid rows to a quarantine folder uses extra disk space, but it
keeps the cleaning process auditable and reversible.

In practice, memory limits forced us to refine our implementation. Our first
version of the foreign-key checks ran an anti-join across the full rows of
`title.principals` (around 95 million rows) and crashed the JVM with 2 GB of
memory. Rewriting the check to operate on distinct key values first cut down
the shuffle size while giving the exact same result. Even so, `clean.py`
could not finish `title_crew`, `title_episode`, and `title_akas` in time on
our laptop. For those three tables, the exploratory queries read directly
from the typed staged layer, which has the same schema without the cleaning
step. This is a hardware limitation of our local environment rather than a
flaw in the pipeline itself.

## 2. Data exploration

### 2.1 Feature films with an atypical runtime for their genre

<!-- RESULT:exploration/q1_genre_runtime_stats -->

### Runtime per genre (outlier: |z| >= 2.0)

| genre | n_films | avg_runtime | std_runtime | median_runtime | n_longer | n_shorter | n_outliers_z | n_outliers_iqr | pct_outliers_z |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| Drama | 196252 | 96.100 | 37.000 | 93 | 1964 | 0 | 1964 | 8730 | 1.000 |
| Documentary | 116290 | 78.000 | 223.300 | 74 | 58 | 0 | 58 | 2079 | 0.050 |
| Comedy | 95806 | 93.300 | 28.700 | 91 | 1617 | 0 | 1617 | 7249 | 1.690 |
| Romance | 42229 | 99.200 | 25.100 | 96 | 1806 | 268 | 2074 | 2176 | 4.910 |
| Action | 40414 | 101.200 | 33.800 | 94 | 843 | 0 | 843 | 1897 | 2.090 |
| Crime | 34432 | 95.700 | 25.600 | 93 | 1177 | 12 | 1189 | 2102 | 3.450 |
| Thriller | 34079 | 96.100 | 21.800 | 93 | 1392 | 704 | 2096 | 2531 | 6.150 |
| Horror | 30579 | 88.700 | 22.300 | 88 | 694 | 14 | 708 | 2067 | 2.320 |
| Adventure | 24030 | 93.000 | 30.600 | 90 | 634 | 0 | 634 | 1469 | 2.640 |
| Mystery | 16436 | 94.400 | 50.400 | 92 | 52 | 0 | 52 | 497 | 0.320 |
| Biography | 16112 | 89.200 | 29.400 | 88 | 469 | 0 | 469 | 324 | 2.910 |
| Family | 15973 | 92.300 | 31.400 | 90 | 329 | 0 | 329 | 889 | 2.060 |
| Music | 13984 | 88.400 | 57.700 | 87 | 97 | 0 | 97 | 385 | 0.690 |
| History | 13640 | 94.300 | 52.900 | 90 | 143 | 0 | 143 | 419 | 1.050 |
| Fantasy | 12602 | 94.100 | 31.900 | 91 | 237 | 0 | 237 | 515 | 1.880 |
| Sci-Fi | 9782 | 91.200 | 30.500 | 90 | 146 | 0 | 146 | 588 | 1.490 |
| Musical | 8503 | 100.100 | 33.300 | 94 | 240 | 0 | 240 | 410 | 2.820 |
| Animation | 8338 | 82.700 | 26.800 | 81 | 175 | 0 | 175 | 221 | 2.100 |
| War | 8049 | 94.800 | 31.300 | 92 | 217 | 0 | 217 | 358 | 2.700 |
| Sport | 6821 | 88.900 | 37.400 | 90 | 99 | 0 | 99 | 185 | 1.450 |
| Western | 6354 | 76.500 | 28.700 | 70 | 148 | 0 | 148 | 133 | 2.330 |
| News | 1409 | 77.600 | 32.200 | 75 | 27 | 0 | 27 | 28 | 1.920 |
| Film-Noir | 878 | 81.900 | 13.800 | 81 | 28 | 3 | 31 | 3 | 3.530 |
| Reality-TV | 297 | 92.100 | 85.200 | 90 | 2 | 0 | 2 | 13 | 0.670 |
| Talk-Show | 102 | 88.100 | 33.700 | 81 | 5 | 0 | 5 | 0 | 4.900 |
| Game-Show | 7 | 79.600 | 35.400 | 67 | 0 | 0 | 0 | 0 | 0.000 |

_26 of 26 rows shown._

<!-- /RESULT -->

<!-- RESULT:exploration/q1_runtime_outliers -->

### Feature films with an atypical runtime for their genre

| genre | tconst | title | year | runtime | genre_avg_runtime | z_score | direction |
| --- | --- | --- | --- | --- | --- | --- | --- |
| Action | tt7321476 | h36: | 2017 | 2160 | 101.200 | 60.990 | longer |
| Action | tt0165127 | Burning of the Red Lotus Temple | 1928 | 1620 | 101.200 | 44.990 | longer |
| Action | tt45643419 | Ooio Aifujairea: Lifedesign | 2025 | 1440 | 101.200 | 39.660 | longer |
| Action | tt0004052 | The Hazards of Helen | 1914 | 1428 | 101.200 | 39.300 | longer |
| Action | tt0131597 | Split | 2004 | 700 | 101.200 | 17.740 | longer |
| Action | tt12669540 | Marathon Wars |  | 600 | 101.200 | 14.770 | longer |
| Action | tt39392193 | The Longest Movie on Earth!?: The Narwallus Movie! | 2016 | 600 | 101.200 | 14.770 | longer |
| Action | tt37937538 | Pedazo del Alma | 2024 | 540 | 101.200 | 13.000 | longer |
| Action | tt42728310 | Cinematic Carnage: The Movie | 2026 | 486 | 101.200 | 11.400 | longer |
| Action | tt0006206 | Les Vampires | 1915 | 421 | 101.200 | 9.470 | longer |
| Action | tt0009204 | The House of Hate | 1918 | 410 | 101.200 | 9.150 | longer |
| Action | tt0007932 | The Fatal Ring | 1917 | 400 | 101.200 | 8.850 | longer |
| Action | tt0006917 | Liberty | 1916 | 400 | 101.200 | 8.850 | longer |
| Action | tt36375509 | Los Monjes Guerreros De Zardak | 2025 | 397 | 101.200 | 8.760 | longer |
| Action | tt0337998 | Ek Hindustani | 2003 | 370 | 101.200 | 7.960 | longer |
| Action | tt0045382 | Commando Cody: Sky Marshal of the Universe | 1953 | 361 | 101.200 | 7.690 | longer |
| Action | tt0011367 | King of the Circus | 1920 | 360 | 101.200 | 7.670 | longer |
| Action | tt2769354 | Ringsend |  | 360 | 101.200 | 7.670 | longer |
| Action | tt0009304 | The Lion's Claws | 1918 | 360 | 101.200 | 7.670 | longer |
| Action | tt0010361 | The Lion Man | 1919 | 360 | 101.200 | 7.670 | longer |
| Action | tt0013773 | With Stanley in Africa | 1922 | 360 | 101.200 | 7.670 | longer |
| Action | tt0008353 | The Mystery Ship | 1917 | 360 | 101.200 | 7.670 | longer |
| Action | tt0012746 | Terror Trail | 1921 | 360 | 101.200 | 7.670 | longer |
| Action | tt9534772 | Kid Fights from Around the World | 2018 | 360 | 101.200 | 7.670 | longer |
| Action | tt9485592 | Girl Blood Sport | 2019 | 354 | 101.200 | 7.490 | longer |
| Action | tt2148482 | The Ultimate Civil War Series: 150th Anniversary Edition | 2012 | 353 | 101.200 | 7.460 | longer |
| Action | tt28331320 | Cruella: Live Action Novelization | 2021 | 352 | 101.200 | 7.430 | longer |
| Action | tt0008355 | The Mystery of the Double Cross | 1917 | 350 | 101.200 | 7.370 | longer |
| Action | tt0199066 | Tao | 1923 | 342 | 101.200 | 7.130 | longer |
| Action | tt20870494 | Glitchtale the Movie (Season 2 Compilation) | 2022 | 338 | 101.200 | 7.010 | longer |
| Action | tt0009825 | Wolves of Kultur | 1918 | 330 | 101.200 | 6.780 | longer |
| Action | tt0011093 | Daredevil Jack | 1920 | 330 | 101.200 | 6.780 | longer |
| Action | tt0008476 | The Purple Mask | 1916 | 330 | 101.200 | 6.780 | longer |
| Action | tt0027365 | The Black Coin | 1936 | 323 | 101.200 | 6.570 | longer |
| Action | tt1954470 | Gangs of Wasseypur | 2012 | 321 | 101.200 | 6.510 | longer |
| Action | tt0007598 | The Yellow Menace | 1916 | 320 | 101.200 | 6.480 | longer |
| Action | tt36374829 | Ferria: El despertar de un mundo (Audiolibro) | 2025 | 317 | 101.200 | 6.390 | longer |
| Action | tt0139739 | William the Conqueror | 1982 | 316 | 101.200 | 6.360 | longer |
| Action | tt0047058 | Gunfighters of the Northwest | 1954 | 315 | 101.200 | 6.330 | longer |
| Action | tt4615264 | Akin | 2014 | 310 | 101.200 | 6.180 | longer |

_40 of 13600 rows shown._

<!-- /RESULT -->

### 2.2 Top three genres per year (1980-2025)

<!-- RESULT:exploration/q2_top3_genres_per_year -->

### Top 3 genres per year (by total votes)

| year | rank | genre | n_films | total_votes | avg_rating | vote_share_pct |
| --- | --- | --- | --- | --- | --- | --- |
| 1980 | 1 | Drama | 1049 | 3159039 | 6.140 | 18.180 |
| 1980 | 2 | Adventure | 134 | 2482677 | 5.920 | 14.280 |
| 1980 | 3 | Horror | 133 | 2071207 | 5.260 | 11.920 |
| 1981 | 1 | Adventure | 151 | 2251598 | 6.110 | 20.020 |
| 1981 | 2 | Drama | 1010 | 1710147 | 6.220 | 15.200 |
| 1981 | 3 | Horror | 153 | 1274946 | 5.430 | 11.340 |
| 1982 | 1 | Drama | 1094 | 3069920 | 6.170 | 18.140 |
| 1982 | 2 | Sci-Fi | 51 | 2459866 | 5.470 | 14.540 |
| 1982 | 3 | Action | 295 | 1874767 | 5.760 | 11.080 |
| 1983 | 1 | Drama | 1110 | 2920187 | 6.190 | 19.340 |
| 1983 | 2 | Action | 296 | 2025833 | 5.660 | 13.420 |
| 1983 | 3 | Adventure | 157 | 1985687 | 5.720 | 13.150 |
| 1984 | 1 | Action | 288 | 3670013 | 5.830 | 16.840 |
| 1984 | 2 | Drama | 1096 | 3023282 | 6.170 | 13.870 |
| 1984 | 3 | Adventure | 144 | 2933463 | 5.910 | 13.460 |
| 1985 | 1 | Comedy | 583 | 4563661 | 5.870 | 21.290 |
| 1985 | 2 | Adventure | 147 | 3413114 | 5.850 | 15.920 |
| 1985 | 3 | Drama | 1055 | 3076597 | 6.180 | 14.350 |
| 1986 | 1 | Drama | 1140 | 4001281 | 6.110 | 18.960 |
| 1986 | 2 | Adventure | 162 | 2967730 | 5.850 | 14.060 |
| 1986 | 3 | Comedy | 604 | 2785255 | 5.780 | 13.200 |
| 1987 | 1 | Drama | 1232 | 4139330 | 6.110 | 17.320 |
| 1987 | 2 | Comedy | 615 | 3688691 | 5.750 | 15.430 |
| 1987 | 3 | Action | 426 | 2362674 | 5.480 | 9.880 |
| 1988 | 1 | Drama | 1164 | 4468377 | 6.100 | 18.010 |
| 1988 | 2 | Comedy | 620 | 3990335 | 5.800 | 16.090 |
| 1988 | 3 | Action | 445 | 2716629 | 5.410 | 10.950 |
| 1989 | 1 | Comedy | 658 | 5016044 | 5.880 | 20.800 |
| 1989 | 2 | Drama | 1200 | 3964200 | 6.180 | 16.440 |
| 1989 | 3 | Adventure | 164 | 2945501 | 5.640 | 12.220 |
| 1990 | 1 | Drama | 1166 | 6326607 | 6.080 | 20.690 |
| 1990 | 2 | Comedy | 614 | 4018702 | 5.840 | 13.140 |
| 1990 | 3 | Crime | 356 | 3591414 | 5.730 | 11.750 |
| 1991 | 1 | Drama | 1190 | 5589284 | 6.150 | 19.800 |
| 1991 | 2 | Crime | 263 | 3677405 | 5.930 | 13.030 |
| 1991 | 3 | Comedy | 618 | 3196111 | 5.910 | 11.320 |
| 1992 | 1 | Drama | 1080 | 6101547 | 6.180 | 20.490 |
| 1992 | 2 | Comedy | 619 | 4076528 | 5.850 | 13.690 |
| 1992 | 3 | Crime | 280 | 3612294 | 5.870 | 12.130 |
| 1993 | 1 | Drama | 1088 | 9002808 | 6.150 | 24.340 |
| 1993 | 2 | Comedy | 564 | 4502974 | 5.850 | 12.170 |
| 1993 | 3 | Crime | 287 | 3048806 | 5.750 | 8.240 |
| 1994 | 1 | Drama | 1145 | 15752440 | 6.150 | 32.840 |
| 1994 | 2 | Crime | 277 | 6509651 | 5.790 | 13.570 |
| 1994 | 3 | Comedy | 637 | 5681665 | 5.840 | 11.850 |
| 1995 | 1 | Drama | 1176 | 12532042 | 6.140 | 23.250 |
| 1995 | 2 | Crime | 257 | 7771102 | 5.870 | 14.420 |
| 1995 | 3 | Comedy | 592 | 6085975 | 5.920 | 11.290 |
| 1996 | 1 | Drama | 1151 | 7866777 | 6.220 | 20.650 |
| 1996 | 2 | Comedy | 666 | 4792656 | 5.920 | 12.580 |
| 1996 | 3 | Crime | 246 | 4448022 | 5.930 | 11.670 |
| 1997 | 1 | Drama | 1251 | 12639260 | 6.210 | 22.260 |
| 1997 | 2 | Action | 342 | 6772493 | 5.510 | 11.930 |
| 1997 | 3 | Comedy | 728 | 5608908 | 5.880 | 9.880 |
| 1998 | 1 | Drama | 1271 | 11790200 | 6.180 | 21.770 |
| 1998 | 2 | Comedy | 760 | 9869650 | 5.860 | 18.220 |
| 1998 | 3 | Crime | 304 | 6222450 | 5.930 | 11.490 |
| 1999 | 1 | Drama | 1398 | 15534016 | 6.140 | 21.900 |
| 1999 | 2 | Comedy | 822 | 8227388 | 5.790 | 11.600 |
| 1999 | 3 | Crime | 301 | 7187664 | 5.860 | 10.140 |
| 2000 | 1 | Drama | 1476 | 13031917 | 6.140 | 20.660 |
| 2000 | 2 | Comedy | 815 | 8340538 | 5.830 | 13.220 |
| 2000 | 3 | Adventure | 144 | 6996013 | 5.600 | 11.090 |
| 2001 | 1 | Drama | 1624 | 14747172 | 6.180 | 18.640 |
| 2001 | 2 | Comedy | 901 | 9519374 | 5.910 | 12.030 |
| 2001 | 3 | Adventure | 160 | 9260197 | 5.710 | 11.700 |
| 2002 | 1 | Drama | 1667 | 17072201 | 6.220 | 21.190 |
| 2002 | 2 | Adventure | 158 | 8699019 | 5.760 | 10.800 |
| 2002 | 3 | Action | 404 | 8400360 | 5.620 | 10.430 |
| 2003 | 1 | Drama | 1706 | 13264871 | 6.160 | 16.880 |
| 2003 | 2 | Action | 374 | 11717322 | 5.610 | 14.910 |
| 2003 | 3 | Comedy | 1032 | 10766208 | 5.920 | 13.700 |
| 2004 | 1 | Drama | 1803 | 16483755 | 6.210 | 17.480 |
| 2004 | 2 | Action | 417 | 11527898 | 5.650 | 12.220 |
| 2004 | 3 | Comedy | 1051 | 10954974 | 5.870 | 11.620 |
| 2005 | 1 | Drama | 1986 | 13698399 | 6.220 | 16.780 |
| 2005 | 2 | Comedy | 1157 | 10199108 | 5.900 | 12.490 |
| 2005 | 3 | Action | 399 | 9065615 | 5.540 | 11.100 |
| 2006 | 1 | Drama | 2217 | 20897951 | 6.210 | 21.580 |
| 2006 | 2 | Comedy | 1267 | 11782866 | 5.820 | 12.170 |
| 2006 | 3 | Thriller | 429 | 8670532 | 5.400 | 8.950 |
| 2007 | 1 | Drama | 2256 | 18546261 | 6.210 | 18.100 |
| 2007 | 2 | Comedy | 1251 | 11723268 | 5.850 | 11.440 |
| 2007 | 3 | Action | 442 | 8999236 | 5.610 | 8.780 |
| 2008 | 1 | Drama | 2528 | 17584314 | 6.160 | 16.770 |
| 2008 | 2 | Action | 507 | 13415714 | 5.470 | 12.800 |
| 2008 | 3 | Comedy | 1430 | 11334272 | 5.750 | 10.810 |
| 2009 | 1 | Drama | 2849 | 17601346 | 6.210 | 16.840 |
| 2009 | 2 | Comedy | 1582 | 13470822 | 5.860 | 12.890 |
| 2009 | 3 | Action | 609 | 12123001 | 5.560 | 11.600 |
| 2010 | 1 | Drama | 3036 | 17563710 | 6.240 | 15.920 |
| 2010 | 2 | Action | 703 | 13341313 | 5.690 | 12.100 |
| 2010 | 3 | Adventure | 359 | 13187329 | 6.110 | 11.960 |
| 2011 | 1 | Drama | 3245 | 20756676 | 6.220 | 18.430 |
| 2011 | 2 | Action | 725 | 17156057 | 5.780 | 15.240 |
| 2011 | 3 | Comedy | 1762 | 13988037 | 5.880 | 12.420 |
| 2012 | 1 | Drama | 3381 | 20125815 | 6.290 | 18.090 |
| 2012 | 2 | Action | 710 | 14903073 | 5.820 | 13.400 |
| 2012 | 3 | Comedy | 1879 | 12515727 | 5.900 | 11.250 |
| 2013 | 1 | Drama | 3736 | 21995304 | 6.270 | 16.870 |
| 2013 | 2 | Action | 796 | 17194791 | 5.700 | 13.180 |
| 2013 | 3 | Comedy | 1977 | 14963722 | 5.910 | 11.470 |
| 2014 | 1 | Drama | 3874 | 24529346 | 6.270 | 18.240 |
| 2014 | 2 | Action | 837 | 20948069 | 5.750 | 15.580 |
| 2014 | 3 | Adventure | 555 | 17751200 | 6.150 | 13.200 |
| 2015 | 1 | Drama | 4007 | 20500609 | 6.210 | 19.020 |
| 2015 | 2 | Action | 875 | 15570179 | 5.620 | 14.440 |
| 2015 | 3 | Adventure | 536 | 14171450 | 6.070 | 13.150 |
| 2016 | 1 | Drama | 4025 | 20898917 | 6.210 | 18.250 |
| 2016 | 2 | Action | 929 | 17217420 | 5.660 | 15.030 |
| 2016 | 3 | Adventure | 557 | 12370868 | 6.020 | 10.800 |
| 2017 | 1 | Drama | 4175 | 20816836 | 6.190 | 19.190 |
| 2017 | 2 | Action | 932 | 16718075 | 5.680 | 15.420 |
| 2017 | 3 | Comedy | 2189 | 11311032 | 5.810 | 10.430 |
| 2018 | 1 | Drama | 4256 | 17420556 | 6.150 | 16.830 |
| 2018 | 2 | Action | 914 | 15088413 | 5.600 | 14.570 |
| 2018 | 3 | Adventure | 527 | 13356640 | 5.830 | 12.900 |
| 2019 | 1 | Drama | 4336 | 22037281 | 6.110 | 20.600 |
| 2019 | 2 | Action | 926 | 12368667 | 5.630 | 11.560 |
| 2019 | 3 | Comedy | 2174 | 12273831 | 5.760 | 11.470 |
| 2020 | 1 | Drama | 3231 | 10484028 | 6.010 | 20.260 |
| 2020 | 2 | Comedy | 1587 | 6519035 | 5.690 | 12.600 |
| 2020 | 3 | Action | 628 | 6134794 | 5.500 | 11.850 |
| 2021 | 1 | Drama | 3675 | 14407879 | 6.090 | 17.840 |
| 2021 | 2 | Action | 859 | 12882768 | 5.550 | 15.950 |
| 2021 | 3 | Adventure | 415 | 10197209 | 5.870 | 12.630 |
| 2022 | 1 | Drama | 4177 | 15981269 | 6.250 | 18.740 |
| 2022 | 2 | Action | 951 | 13362990 | 5.730 | 15.670 |
| 2022 | 3 | Comedy | 2251 | 10211207 | 5.870 | 11.970 |
| 2023 | 1 | Drama | 4367 | 14779047 | 6.240 | 18.900 |
| 2023 | 2 | Action | 1071 | 10592954 | 5.830 | 13.540 |
| 2023 | 3 | Comedy | 2316 | 9781004 | 5.960 | 12.510 |
| 2024 | 1 | Drama | 4091 | 11684384 | 6.260 | 18.260 |
| 2024 | 2 | Action | 1019 | 8660597 | 5.840 | 13.530 |
| 2024 | 3 | Comedy | 2162 | 7102315 | 5.880 | 11.100 |
| 2025 | 1 | Drama | 3614 | 8665598 | 6.390 | 15.280 |
| 2025 | 2 | Action | 968 | 7707374 | 5.920 | 13.590 |
| 2025 | 3 | Comedy | 1965 | 5919816 | 6.050 | 10.440 |

_138 of 138 rows shown._

<!-- /RESULT -->

### 2.3 Top three genres compared with the other genres

<!-- RESULT:exploration/q3_top3_vs_rest_by_decade -->

### Top 3 vs rest, averaged per decade

| decade | avg_top3_vote_share_pct | votes_per_film_ratio_top3_vs_rest | avg_top3_film_share_pct | top3_avg_rating | rest_avg_rating |
| --- | --- | --- | --- | --- | --- |
| 1980 | 46.000 | 1.040 | 45.600 | 6.020 | 5.970 |
| 1990 | 47.200 | 0.890 | 50.700 | 6.020 | 5.970 |
| 2000 | 42.000 | 0.860 | 46.100 | 6.020 | 6.170 |
| 2010 | 44.100 | 1.400 | 36.900 | 6.080 | 6.220 |
| 2020 | 44.100 | 1.230 | 39.800 | 6.050 | 6.130 |

_5 of 5 rows shown._

<!-- /RESULT -->

<!-- RESULT:exploration/q3_genre_top3_frequency -->

### How often each genre is in the yearly top 3

| genre | years_in_top3 | years_ranked_1 | first_year | last_year |
| --- | --- | --- | --- | --- |
| Drama | 46 | 42 | 1980 | 2025 |
| Comedy | 34 | 2 | 1985 | 2025 |
| Action | 29 | 1 | 1982 | 2025 |
| Adventure | 16 | 1 | 1980 | 2021 |
| Crime | 9 | 0 | 1990 | 1999 |
| Horror | 2 | 0 | 1980 | 1981 |
| Sci-Fi | 1 | 0 | 1982 | 1982 |
| Thriller | 1 | 0 | 2006 | 2006 |

_8 of 8 rows shown._

<!-- /RESULT -->

<!-- RESULT:exploration/q3_top3_vs_rest_per_year -->

### Top 3 genres vs all other genres, per year

| year | top3_vote_share_pct | top3_films | rest_films | rest_genres | top3_votes_per_film | rest_votes_per_film | top3_votes_per_genre | rest_votes_per_genre | top3_avg_rating | rest_avg_rating |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| 1980 | 44.380 | 1316 | 2054 | 20 | 5861 | 4707 | 2570974 | 483379 | 6.030 | 6.000 |
| 1981 | 46.560 | 1314 | 2106 | 18 | 3985 | 2854 | 1745564 | 333932 | 6.110 | 6.100 |
| 1982 | 43.760 | 1440 | 2079 | 18 | 5142 | 4578 | 2468184 | 528738 | 6.060 | 5.940 |
| 1983 | 45.910 | 1563 | 2037 | 18 | 4435 | 4010 | 2310569 | 453821 | 6.040 | 5.970 |
| 1984 | 44.170 | 1528 | 2105 | 18 | 6300 | 5780 | 3208919 | 675894 | 6.080 | 6.020 |
| 1985 | 51.560 | 1785 | 1828 | 18 | 6192 | 5681 | 3684457 | 576965 | 6.050 | 5.960 |
| 1986 | 46.220 | 1906 | 1971 | 18 | 5118 | 5758 | 3251422 | 630481 | 5.980 | 5.960 |
| 1987 | 42.630 | 2273 | 1881 | 18 | 4483 | 7291 | 3396898 | 761930 | 5.890 | 5.950 |
| 1988 | 45.050 | 2229 | 2011 | 18 | 5014 | 6777 | 3725114 | 757147 | 5.880 | 5.920 |
| 1989 | 49.460 | 2022 | 2317 | 18 | 5898 | 5259 | 3975248 | 676979 | 6.040 | 5.830 |
| 1990 | 45.580 | 2136 | 2010 | 18 | 6525 | 8277 | 4645574 | 924271 | 5.950 | 5.800 |
| 1991 | 44.150 | 2071 | 2012 | 18 | 6018 | 7833 | 4154267 | 875553 | 6.050 | 5.930 |
| 1992 | 46.310 | 1979 | 1911 | 18 | 6968 | 8369 | 4596790 | 888481 | 6.030 | 6.030 |
| 1993 | 44.750 | 1939 | 1941 | 18 | 8538 | 10530 | 5518196 | 1135462 | 6.000 | 6.020 |
| 1994 | 58.260 | 2059 | 1972 | 18 | 13572 | 10153 | 9314585 | 1112369 | 6.000 | 5.920 |
| 1995 | 48.960 | 2025 | 2135 | 18 | 13032 | 12886 | 8796373 | 1528420 | 6.040 | 5.950 |
| 1996 | 44.900 | 2063 | 2108 | 19 | 8293 | 9958 | 5702485 | 1104848 | 6.090 | 6.050 |
| 1997 | 44.070 | 2321 | 2110 | 18 | 10780 | 15051 | 8340220 | 1764294 | 6.000 | 6.110 |
| 1998 | 51.480 | 2335 | 2248 | 18 | 11941 | 11691 | 9294100 | 1460092 | 6.040 | 5.950 |
| 1999 | 43.640 | 2521 | 2388 | 18 | 12277 | 16737 | 10316356 | 2220463 | 6.000 | 5.960 |
| 2000 | 44.970 | 2435 | 2852 | 18 | 11650 | 12171 | 9456156 | 1928463 | 6.000 | 5.990 |
| 2001 | 42.370 | 2685 | 3059 | 18 | 12487 | 14906 | 11175581 | 2533196 | 6.060 | 6.000 |
| 2002 | 42.420 | 2229 | 3652 | 21 | 15330 | 12707 | 11390527 | 2209796 | 6.080 | 6.130 |
| 2003 | 45.490 | 3112 | 3028 | 20 | 11487 | 14144 | 11916134 | 2141328 | 6.010 | 6.140 |
| 2004 | 41.320 | 3271 | 3416 | 20 | 11913 | 16198 | 12988876 | 2766578 | 6.030 | 6.230 |
| 2005 | 40.370 | 3542 | 3821 | 20 | 9306 | 12744 | 10987707 | 2434653 | 6.040 | 6.220 |
| 2006 | 42.700 | 3913 | 4385 | 19 | 10568 | 12656 | 13783783 | 2920840 | 6.000 | 6.180 |
| 2007 | 38.320 | 3949 | 4814 | 20 | 9944 | 13126 | 13089588 | 3159512 | 6.030 | 6.250 |
| 2008 | 40.380 | 4465 | 5376 | 19 | 9481 | 11627 | 14111433 | 3289919 | 5.950 | 6.240 |
| 2009 | 41.330 | 5040 | 6207 | 19 | 8570 | 9883 | 14398390 | 3228475 | 6.020 | 6.270 |
| 2010 | 39.980 | 4098 | 8320 | 20 | 10759 | 7957 | 14697451 | 3310153 | 6.130 | 6.250 |
| 2011 | 46.090 | 5732 | 7673 | 20 | 9055 | 7911 | 17300257 | 3034875 | 6.060 | 6.350 |
| 2012 | 42.740 | 5970 | 8318 | 20 | 7964 | 7658 | 15848205 | 3185137 | 6.110 | 6.330 |
| 2013 | 41.520 | 6509 | 9328 | 20 | 8320 | 8175 | 18051272 | 3812907 | 6.090 | 6.300 |
| 2014 | 47.020 | 5266 | 11827 | 22 | 12007 | 6022 | 21076205 | 3237236 | 6.170 | 6.280 |
| 2015 | 46.610 | 5418 | 11631 | 20 | 9273 | 4948 | 16747413 | 2877396 | 6.100 | 6.200 |
| 2016 | 44.080 | 5511 | 11661 | 20 | 9161 | 5491 | 16829068 | 3201562 | 6.100 | 6.150 |
| 2017 | 45.040 | 7296 | 10181 | 21 | 6695 | 5854 | 16281981 | 2838265 | 6.010 | 6.170 |
| 2018 | 44.300 | 5697 | 11829 | 21 | 8051 | 4875 | 15288536 | 2746161 | 6.030 | 6.040 |
| 2019 | 43.630 | 7436 | 10320 | 21 | 6278 | 5845 | 15559926 | 2872328 | 5.950 | 6.120 |
| 2020 | 44.710 | 5446 | 8528 | 21 | 4249 | 3355 | 7712619 | 1362573 | 5.860 | 6.040 |
| 2021 | 46.420 | 4949 | 10389 | 21 | 7575 | 4164 | 12495952 | 2059958 | 5.980 | 6.030 |
| 2022 | 46.380 | 7379 | 10330 | 21 | 5361 | 4427 | 13185155 | 2177848 | 6.070 | 6.160 |
| 2023 | 44.950 | 7754 | 10326 | 21 | 4534 | 4170 | 11717668 | 2050459 | 6.100 | 6.170 |
| 2024 | 42.890 | 7272 | 10326 | 22 | 3774 | 3539 | 9149099 | 1660925 | 6.090 | 6.130 |
| 2025 | 39.310 | 6547 | 9270 | 21 | 3405 | 3715 | 7430929 | 1639772 | 6.220 | 6.230 |

_46 of 46 rows shown._

<!-- /RESULT -->

### 2.4 Directors with exactly one film rated >= 8.0 and several <= 6.0

<!-- RESULT:exploration/q4_one_hit_directors -->

### Directors with exactly one film >= 8.0 and >= 2 films <= 6.0 (films with >= 100 votes)

| nconst | name | n_films | hit_title | hit_year | hit_rating | n_flops | avg_other_rating | worst_flops |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| nm0861703 | Richard Thorpe | 94 | The Wizard of Oz | 1939 | 8.100 | 42 | 6.090 | Murder at Dawn (4.3),The Lone Defender (4.7),Bedevilled (4.7),The Tartars (4.7),The Devil Plays (5.1) |
| nm0851537 | Norman Taurog | 70 | The Wizard of Oz | 1939 | 8.100 | 25 | 6.160 | Follow the Leader (4.5),Sergeant Dead Head (4.8),Double Trouble (5.1),Dr. Goldfoot and the Bikini Machine (5.1),Sunny Skies (5.2) |
| nm0002031 | Michael Curtiz | 103 | Casablanca | 1942 | 8.500 | 23 | 6.490 | A Soldier's Plaything (4.5),Under a Texas Moon (5.1),Labyrinth of Horror (5.1),The Vagabond King (5.2),God's Gift to Women (5.2) |
| nm0080315 | Mahesh Bhatt | 36 | Saaransh | 1984 | 8.100 | 23 | 5.690 | Sadak 2 (1.2),Yeh Hai Mumbai Meri Jaan (4.2),Gunaah (4.3),Aaj (4.6),Naaraaz (4.7) |
| nm0490487 | Wai Keung Lau | 44 | Infernal Affairs | 2002 | 8.000 | 22 | 5.720 | From Vegas to Macau III (2.7),The Park (3.0),Kung Fu Monster (4.0),The Wesley's Mysterious File (4.0),The Avenging Fist (4.3) |
| nm0700872 | Puri Jagannadh | 34 | Pokiri | 2006 | 8.000 | 21 | 5.520 | Liger (2.6),Double Ismart (2.9),Andhrawala (3.5),Shart: The Challenge (3.6),Devudu Chesina Manushulu (3.8) |
| nm0909825 | Raoul Walsh | 81 | White Heat | 1949 | 8.100 | 20 | 6.380 | Marines, Let's Go (4.4),Women of All Nations (4.6),Hitting a New High (4.9),The Man Who Came Back (5.2),The Cock-Eyed World (5.3) |
| nm3359146 | Rabi Kinagi | 24 | Suna Panjuri | 1995 | 9.000 | 15 | 5.770 | Jamai Badal (4.5),Jamai 420 (4.6),Ami Je Ke Tomar (4.7),Herogiri (4.7),Jio Pagla (5.1) |
| nm0030762 | Franz Antel | 20 | The Stubborn Mule | 1981 | 8.100 | 15 | 5.020 | Musik, Musik - da wackelt die Penne (3.1),Außer Rand und Band am Wolfgangsee (3.7),Blau blüht der Enzian (3.8),Sexy Susan Sins Again (4.4),The Countess Died of Laughter (4.5) |
| nm0435050 | Shaji Kailas | 25 | Aaram Thamburan | 1997 | 8.100 | 15 | 5.350 | Jana (3.3),Simhasanam 2021 (3.4),The King and the Commissioner (3.4),Alone (3.4),Hunt (3.7) |
| nm0836328 | John Sturges | 42 | The Great Escape | 1963 | 8.200 | 14 | 6.390 | Fast Company (5.0),Underwater! (5.3),By Love Possessed (5.4),For the Love of Rusty (5.5),The Man Who Dared (5.6) |
| nm0000814 | John G. Avildsen | 22 | Rocky | 1976 | 8.100 | 14 | 5.780 | Foreplay (3.9),Guess What We Learned in School Today? (4.1),A Night in Heaven (4.3),Cry Uncle (4.9),The Stoolie (5.2) |
| nm0002030 | George Cukor | 60 | The Wizard of Oz | 1939 | 8.100 | 13 | 6.580 | Grumpy (4.4),Justine (5.4),The Blue Bird (5.4),The Chapman Report (5.5),The Virtuous Sin (5.6) |
| nm0132709 | Martin Campbell | 21 | Casino Royale | 2006 | 8.000 | 13 | 5.630 | Three for All (4.2),Just Play Dead (4.2),Eskimo Nell (4.4),Sex Thief (4.5),Dirty Angels (4.6) |
| nm0920074 | William A. Wellman | 68 | The Ox-Bow Incident | 1943 | 8.000 | 13 | 6.460 | Maybe It's Love (5.0),Ring of Fear (5.0),The Man I Love (5.1),The Boob (5.4),Woman Trap (5.5) |
| nm1212770 | Wenn V. Deramas | 21 | Bekikang: Ang nanay kong beki | 2013 | 8.500 | 12 | 5.980 | The Amazing Praybeyt Benjamin (4.4),BFF: Best Friends Forever (4.8),Beauty and the Bestie (5.0),Praybeyt Benjamin (5.1),Maria, Leonora, Teresa (5.3) |
| nm0000361 | Brian De Palma | 30 | Scarface | 1983 | 8.300 | 12 | 6.190 | Domino (4.5),The Wedding Party (4.7),Home Movies (4.9),Dionysus in '69 (5.0),Get to Know Your Rabbit (5.2) |
| nm1777967 | A.L. Vijay | 21 | Deiva Thirumagal | 2011 | 8.300 | 12 | 5.890 | Devi 2 (3.9),Kadhal Reset Repeat (4.9),Boo (5.0),Watchman (5.3),Tutak Tutak Tutiya (5.4) |
| nm0503777 | Mervyn LeRoy | 68 | I Am a Fugitive from a Chain Gang | 1932 | 8.200 | 12 | 6.580 | Numbered Men (4.7),Sweet Adeline (5.5),Latin Lovers (5.5),Broadminded (5.6),The Green Berets (5.6) |
| nm2321771 | Smeep Kang | 23 | Carry on Jatta | 2012 | 8.300 | 11 | 5.850 | Second Hand Husband (3.7),Saunkan Saunkanay 2 (3.7),Golgappe (4.3),Carry on Jatta 4 (4.6),Jhootha Kahin Ka (4.7) |
| nm0001675 | Robert Rodriguez | 24 | Sin City | 2005 | 8.000 | 11 | 5.900 | Spy Kids 4: All the Time in the World (3.6),The Adventures of Sharkboy and Lavagirl 3-D (3.9),Spy Kids: Armageddon (4.3),Spy Kids 3: Game Over (4.4),We Can Be Heroes (4.7) |
| nm0814469 | Sion Sono | 36 | Love Exposure | 2008 | 8.000 | 11 | 6.160 | Teachers of Sexual Play: Modelling Urns with the Female Body (4.0),Prisoners of the Ghostland (4.2),A Man's Flower Road (4.8),The Bastard and the Beautiful World (4.9),Bicycle Sighs (5.4) |
| nm0062614 | Harry Baweja | 12 | Chaar Sahibzaade 2: Rise of Banda Singh Bahadur | 2016 | 8.600 | 10 | 4.910 | Love Story 2050 (2.7),Teesri Aankh: The Hidden Camera (2.8),Karz: The Burden of Truth (4.0),Deewane (4.7),Qayamat: City Under Threat (4.8) |
| nm0471452 | Suresh Krishna | 25 | Baasha | 1995 | 8.300 | 10 | 5.980 | Parattai Engira Azhagu Sundaram (2.8),Gajendra (3.4),Rocky (4.1),The Prince (4.4),Asthram (4.6) |
| nm0430782 | Joshiy | 35 | Dhruvam | 1993 | 8.100 | 10 | 6.190 | Salaam Kashmier (2.7),Lokpal (3.2),Lailaa O Lailaa (3.3),Dubai (4.2),Praja (4.3) |
| nm0677951 | Daniel Petrie | 19 | A Raisin in the Sun | 1961 | 8.000 | 10 | 5.910 | The Neptune Factor (4.4),The Main Attraction (4.9),The Idol (5.3),The Betsy (5.3),Cocoon: The Return (5.4) |
| nm0948159 | Wilson Yip | 22 | Ip Man | 2008 | 8.000 | 10 | 6.070 | Triumph in the Skies (4.3),Happy Magic (4.7),Skyline Cruisers (5.1),01:00 A.M. (5.2),Leaving Me, Loving You (5.4) |
| nm11412226 | Sigrid Polon | 12 | Boys in Service | 2025 | 8.000 | 10 | 4.850 | Kulong (4.1),Eks (4.1),Teacher's Pet (4.1),Sorority (4.3),Kabitan (4.4) |
| nm0695937 | Otto Preminger | 40 | Anatomy of a Murder | 1959 | 8.000 | 10 | 6.480 | Skidoo (4.7),Rosebud (5.2),The Girl on the Roof (5.5),In the Meantime, Darling (5.5),The Great Love (5.7) |
| nm1583171 | R. Kannan | 12 | Jayam Kondaan | 2008 | 8.000 | 10 | 5.130 | Vanthaan Vendraan (4.0),Oru Oorla Rendu Raja (4.5),Biskoth (4.6),Kasethan Kadavulada (4.8),Settai (4.8) |
| nm0002045 | Stanley Donen | 28 | Singin' in the Rain | 1952 | 8.300 | 9 | 6.390 | Saturn 3 (5.1),Staircase (5.3),Lucky Lady (5.3),Surprise Package (5.6),Love Is Better Than Ever (5.6) |
| nm2538739 | Hanung Bramantyo | 33 | Love Is Never on Time | 2024 | 8.100 | 9 | 6.480 | Benyamin the Troublemaker (3.1),Trinil (4.5),Satria Dewa: Gatotkaca (5.4),Catatan Si Boy (5.5),Get Married 2 (5.8) |
| nm0155280 | Kaige Chen | 23 | Farewell My Concubine | 1993 | 8.100 | 9 | 6.330 | Killing Me Softly (5.4),Monk Comes Down the Mountain (5.5),The Battle at Lake Changjin (5.5),Water Gate Bridge (5.6),The Promise (5.6) |
| nm0517768 | Dimitri Logothetis | 11 | Champions Forever | 1989 | 8.000 | 9 | 4.340 | Jiu Jitsu (2.9),Gunner (3.2),Hungry for You (3.4),The Lost Angel (3.7),Slaughterhouse Rock (4.4) |
| nm0567680 | Bruce McDonald | 16 | Music from the Big House | 2010 | 8.400 | 8 | 6.030 | Hellions (4.2),Dreamland (4.7),Hard Core Logo 2 (5.2),Picture Claire (5.4),This Movie Is Broken (5.7) |
| nm1688464 | Pedro Antonio | 11 | New Year's Special | 2023 | 8.400 | 8 | 5.610 | Christmas Full of Grace (4.6),Um Tio Quase Perfeito (5.3),Agentes Muito Especiais (5.4),Tô Ryca! 2 (5.4),Get the Grift (5.4) |
| nm1318819 | Ezhil | 11 | Thulladha Manamum Thullum | 1999 | 8.300 | 8 | 4.770 | Desingu Raja 2 (2.8),Saravanan Irukka Bayamaen (3.2),Raja (3.7),Yutha Satham (3.8),Vellaikaara Durai (3.9) |
| nm0000490 | Spike Lee | 35 | David Byrne's American Utopia | 2020 | 8.200 | 8 | 6.650 | Da Sweet Blood of Jesus (4.5),She Hate Me (5.3),Red Hook Summer (5.4),Girl 6 (5.4),Highest 2 Lowest (5.5) |
| nm0796117 | M. Night Shyamalan | 16 | The Sixth Sense | 1999 | 8.200 | 8 | 5.870 | The Last Airbender (3.9),Praying with Anger (4.4),After Earth (4.8),The Happening (5.0),Lady in the Water (5.5) |
| nm0002120 | Lasse Hallström | 28 | Hachi: A Dog's Tale | 2009 | 8.100 | 8 | 6.510 | Happy We (5.3),Father to Be (5.4),The Hypnotist (5.6),The Nutcracker and the Four Realms (5.6),A Guy and a Gal (5.7) |
| nm0001469 | Barry Levinson | 26 | Rain Man | 1988 | 8.000 | 8 | 6.330 | Envy (4.8),Toys (5.2),Jimmy Hollywood (5.3),The Humbling (5.5),Rock the Kasbah (5.5) |
| nm0149134 | Sinan Çetin | 11 | Abbas in Flower | 1982 | 8.000 | 8 | 4.890 | Çanakkale Çocuklari (1.8),Bay E (3.1),Romantik (4.4),Ugly But in Love (4.5),Story of a Day (5.2) |
| nm1500124 | M. Padmakumar | 17 | Joseph | 2018 | 8.000 | 8 | 5.590 | Parunthu (2.9),Thiruvambadi Thamban (4.1),Polytechnic (4.3),Queen Elizabeth (4.5),Kanal (4.6) |
| nm0096539 | António Borges Correia | 8 | Antes de a Vida Começar | 2006 | 9.300 | 7 | 5.310 | Parto (4.5),Na Mata dos Medos (4.7),A Mulher sem Corpo (4.8),The Hours of Light (5.6),André's Eyes (5.6) |
| nm0580669 | James Merendino | 11 | The Invisible Life of Thomas Lynch | 2009 | 9.100 | 7 | 5.300 | Terrified (3.5),Evil Remains (3.9),Hard Drive (4.3),Punk's Dead: SLC Punk 2 (4.6),The Real Thing (4.9) |
| nm3221483 | Navaniat Singh | 16 | Singh vs Kaur 2 | 2026 | 8.500 | 7 | 6.010 | Sidhus of Southall (4.4),Yamla Pagla Deewana Phir Se (4.4),Singham (4.7),Romeo Ranjha (4.8),Rangeelay (5.1) |
| nm0846092 | Kamal Tabrizi | 15 | The Lizard | 2004 | 8.400 | 7 | 5.760 | Sly (4.1),We Are All Together (4.1),A Bumpy Story (4.7),Mina's Choice (4.7),There's Always a Woman in Between (5.0) |
| nm0803350 | Sangeeth Sivan | 11 | Yoddha | 1992 | 8.400 | 7 | 5.110 | Yamla Pagla Deewana 2 (3.5),Click (3.6),Captured by You (3.8),Zor: Never Underestimate the Force (4.4),Ek: The Power of One (4.7) |
| nm0001379 | John Huston | 44 | The Treasure of the Sierra Madre | 1948 | 8.200 | 7 | 6.680 | Phobia (4.3),Casino Royale (5.0),Love and Bullets (5.6),Sinful Davey (5.7),On Our Merry Way (5.7) |
| nm0576554 | Hansal Mehta | 12 | Shahid | 2012 | 8.200 | 7 | 5.750 | Woodstock Villa (3.5),Raakh: A Poem Masked in Blood (4.1),Faraaz (5.0),Dil Pe Mat Le Yaar!! (5.1),Simran (5.2) |

_50 of 347 rows shown._

<!-- /RESULT -->

### 2.5 Series: best episode early, worst episode in the last season

<!-- RESULT:exploration/q5_series_best_early_worst_last -->

### Series whose best episode is in seasons 1-2 and worst episode in the last season

| parentTconst | series_title | series_start | n_seasons | n_big_seasons | last_season | best_episode | best_rating | worst_episode | worst_rating | s1_avg_rating | s2_avg_rating | last_season_avg_rating |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| tt4508902 | One Punch Man | 2015 | 3 | 3 | 3 | S1E12 | 9.500 | S3E6 | 1.600 | 8.780 | 8.150 | 3.800 |
| tt8560918 | The Queen of Flow | 2018 | 3 | 3 | 3 | S1E82 | 9.600 | S3E65 | 1.900 | 8.700 | 8.770 | 3.840 |
| tt1856010 | House of Cards | 2013 | 6 | 5 | 6 | S2E1 | 9.500 | S6E8 | 2.500 | 8.380 | 8.620 | 4.150 |
| tt0200337 | Family Law | 1999 | 3 | 3 | 3 | S2E6 | 9.400 | S3E12 | 2.700 | 8.000 | 8.160 | 7.010 |
| tt0118252 | All Dogs Go to Heaven: The Series | 1996 | 3 | 3 | 3 | S2E10 | 9.200 | S3E12 | 2.600 | 6.750 | 7.020 | 6.090 |
| tt33145195 | Margarita | 2024 | 3 | 3 | 3 | S1E26 | 9.400 | S3E20 | 2.900 | 6.970 | 6.290 | 5.930 |
| tt0050022 | The Gumby Show | 1956 | 3 | 3 | 3 | S1E3 | 8.100 | S3E70 | 1.700 | 6.930 | 6.480 | 5.190 |
| tt5189554 | Soy Luna | 2016 | 4 | 4 | 4 | S2E80 | 9.100 | S4E12 | 2.800 | 7.240 | 7.050 | 5.480 |
| tt0108804 | High Tide | 1994 | 3 | 3 | 3 | S1E17 | 8.400 | S3E15 | 2.200 | 8.400 |  | 4.750 |
| tt0319930 | American Dreams | 2002 | 3 | 3 | 3 | S2E11 | 9.700 | S3E15 | 3.500 | 8.160 | 8.890 | 8.010 |
| tt0856757 | Leaf Cast | 2006 | 5 | 5 | 5 | S1E38 | 8.300 | S5E3 | 2.400 | 7.010 | 6.920 | 2.830 |
| tt0122813 | Bob and Margaret | 1993 | 4 | 4 | 4 | S1E2 | 8.600 | S4E1 | 2.700 | 7.900 | 7.640 | 2.700 |
| tt23735438 | Love Is Blind: Sweden | 2024 | 3 | 3 | 3 | S1E10 | 7.700 | S3E9 | 1.900 | 6.330 | 6.880 | 4.490 |
| tt3107288 | The Flash | 2014 | 9 | 9 | 9 | S1E15 | 9.600 | S9E6 | 3.800 | 8.560 | 8.450 | 6.020 |
| tt22199742 | Rykter | 2022 | 4 | 4 | 4 | S2E15 | 9.200 | S4E5 | 3.600 | 7.450 | 7.610 | 4.160 |
| tt3520702 | Bloodline | 2015 | 3 | 3 | 3 | S1E12 | 9.100 | S3E9 | 3.600 | 8.130 | 8.220 | 6.920 |
| tt8439812 | Asouf | 2018 | 3 | 3 | 3 | S1E16 | 9.700 | S3E15 | 4.300 | 8.750 | 9.220 | 5.460 |
| tt0154063 | Chiquititas | 1995 | 6 | 6 | 6 | S1E1 | 9.200 | S6E1 | 3.800 | 9.200 |  | 3.800 |
| tt2039304 | Your Favorite Martian | 2010 | 3 | 3 | 3 | S2E14 | 9.200 | S3E13 | 4.000 | 8.090 | 7.780 | 7.810 |
| tt0233067 | Kaverille ei jätetä | 1999 | 3 | 3 | 3 | S2E4 | 8.300 | S3E5 | 3.200 | 7.430 | 6.790 | 5.050 |
| tt0369179 | Two and a Half Men | 2003 | 12 | 12 | 12 | S2E23 | 8.700 | S12E15 | 3.600 | 7.910 | 7.880 | 5.370 |
| tt0126179 | Women: Stories of Passion | 1996 | 3 | 3 | 3 | S2E4 | 7.700 | S3E12 | 2.600 | 6.350 | 7.700 | 2.600 |
| tt0125622 | Honey, I Shrunk the Kids: The TV Show | 1997 | 3 | 3 | 3 | S1E5 | 7.500 | S3E9 | 2.500 | 5.840 | 5.010 | 4.810 |
| tt0805905 | My Friends Tigger & Pooh | 2007 | 3 | 3 | 3 | S1E23 | 8.700 | S3E7 | 3.800 | 6.470 | 6.730 | 6.390 |
| tt6407712 | Norm Macdonald Live | 2013 | 3 | 3 | 3 | S1E1 | 9.800 | S3E10 | 5.100 | 8.860 | 8.300 | 7.950 |
| tt0075579 | Secret Army | 1977 | 3 | 3 | 3 | S2E4 | 8.800 | S3E14 | 4.200 | 8.320 | 8.140 | 7.940 |
| tt21105088 | Golden Boy | 2022 | 3 | 3 | 3 | S2E31 | 8.700 | S3E1 | 4.100 | 6.790 | 7.420 | 6.550 |
| tt1909015 | Undercover | 2011 | 6 | 6 | 6 | S2E12 | 9.200 | S6E3 | 4.600 | 8.380 | 8.750 | 5.460 |
| tt11318332 | Bachelor Point | 2018 | 5 | 5 | 5 | S1E50 | 9.500 | S5E6 | 4.900 | 8.330 | 8.270 | 5.790 |
| tt0070975 | Chico and the Man | 1974 | 4 | 4 | 4 | S1E13 | 8.000 | S4E22 | 3.500 | 7.400 | 6.880 | 5.430 |
| tt26692417 | Pokémon Horizons: The Series | 2023 | 8 | 7 | 8 | S1E18 | 9.000 | S8E13 | 4.500 | 7.950 | 7.630 | 6.820 |
| tt0380869 | The Collector | 2004 | 3 | 3 | 3 | S2E8 | 8.900 | S3E5 | 4.500 | 7.360 | 7.480 | 7.120 |
| tt1622696 | Ben 10: Ultimate Alien | 2010 | 3 | 3 | 3 | S1E16 | 9.300 | S3E9 | 5.100 | 8.030 | 7.600 | 7.730 |
| tt0421482 | Winx Club | 2004 | 8 | 8 | 8 | S2E26 | 8.900 | S8E18 | 4.700 | 7.830 | 7.940 | 6.630 |
| tt1122725 | Ghost Hunters International | 2008 | 3 | 3 | 3 | S1E10 | 7.800 | S3E10 | 3.600 | 6.630 | 6.400 | 5.340 |
| tt1312171 | The Umbrella Academy | 2019 | 4 | 3 | 4 | S2E10 | 8.900 | S4E6 | 4.800 | 8.130 | 8.320 | 6.230 |
| tt4953128 | Animals. | 2016 | 3 | 3 | 3 | S1E7 | 8.400 | S3E9 | 4.300 | 7.620 | 7.840 | 7.210 |
| tt1837492 | 13 Reasons Why | 2017 | 4 | 4 | 4 | S1E13 | 9.200 | S4E4 | 5.100 | 8.330 | 7.410 | 6.110 |
| tt1480684 | The League | 2009 | 7 | 6 | 7 | S2E10 | 8.700 | S7E11 | 4.600 | 8.050 | 8.130 | 6.780 |
| tt0047758 | The Millionaire | 1955 | 6 | 6 | 6 | S2E21 | 9.400 | S6E14 | 5.400 | 7.160 | 8.390 | 7.410 |
| tt0367379 | Eve | 2003 | 3 | 3 | 3 | S1E17 | 8.500 | S3E8 | 4.500 | 6.990 | 7.590 | 6.770 |
| tt1725902 | Fish Hooks | 2010 | 3 | 3 | 3 | S1E7 | 8.900 | S3E9 | 4.900 | 7.390 | 6.710 | 7.060 |
| tt6392930 | Swar Shuaib | 2014 | 3 | 3 | 3 | S2E1 | 9.500 | S3E3 | 5.500 | 8.490 | 8.530 | 6.430 |
| tt2662656 | Made in Iran | 2011 | 3 | 3 | 3 | S1E1 | 7.600 | S3E4 | 3.600 | 6.440 | 6.210 | 4.300 |
| tt0060028 | Star Trek | 1966 | 3 | 3 | 3 | S1E28 | 9.200 | S3E4 | 5.200 | 7.640 | 7.450 | 6.870 |
| tt3659018 | That's My Life | 2014 | 4 | 4 | 4 | S2E32 | 9.700 | S4E31 | 5.700 | 7.160 | 8.420 | 7.350 |
| tt1788634 | The Boat | 2011 | 3 | 3 | 3 | S2E5 | 8.900 | S3E16 | 5.000 | 8.290 | 8.290 | 7.940 |
| tt14494938 | Fake Profile | 2023 | 3 | 3 | 3 | S2E2 | 7.100 | S3E10 | 3.200 | 6.540 | 6.820 | 5.460 |
| tt0054552 | The Joey Bishop Show | 1961 | 4 | 4 | 4 | S2E19 | 8.800 | S4E17 | 5.000 | 7.940 | 7.840 | 6.790 |
| tt9113406 | Wu-Tang: An American Saga | 2019 | 3 | 3 | 3 | S2E6 | 9.300 | S3E3 | 5.500 | 8.170 | 8.390 | 7.460 |

_50 of 475 rows shown._

<!-- /RESULT -->

### 2.6 Actors successful in several languages

<!-- RESULT:exploration/q6_actors_2plus_languages -->

_Result not generated yet._

<!-- /RESULT -->

<!-- RESULT:exploration/q6_actors_3plus_languages -->

_Result not generated yet._

<!-- /RESULT -->

<!-- RESULT:exploration/q6_language_pairs -->

_Result not generated yet._

<!-- /RESULT -->

### 2.7 Long careers that peaked early

<!-- RESULT:exploration/q7_early_peak_share -->

_Result not generated yet._

<!-- /RESULT -->

<!-- RESULT:exploration/q7_early_peak_careers -->

_Result not generated yet._

<!-- /RESULT -->

### 2.8 Multi-talented professionals

<!-- RESULT:exploration/q8_role_combination_performance -->

_Result not generated yet._

<!-- /RESULT -->

<!-- RESULT:exploration/q8_paired_comparison -->

_Result not generated yet._

<!-- /RESULT -->

<!-- RESULT:exploration/q8_top_multi_talented -->

_Result not generated yet._

<!-- /RESULT -->

### Methodological explanation - exploration

Across all exploratory queries, we rely on a shared definition of a *feature
film* (`jobs/common/films.py`): a non-adult title of type `movie` with a
runtime of at least 40 minutes, following the AMPAS / BFI standard. Films
tagged with multiple genres are counted once in each of them.

For **Q1 (atypical runtimes)**, we compute the mean and standard deviation of
runtimes per genre and flag films with `|z-score| >= 2`. Using the z-score
fits the question's focus on deviation from the *average*. However, runtime
distributions are heavily right-skewed due to very long experimental films,
which inflates the standard deviation. To account for this, we also flag
outliers using the IQR rule (outside `Q1 - 1.5 * IQR` and `Q3 + 1.5 * IQR`),
which is much less sensitive to extreme values, and report both counts per
genre. Using a fixed threshold in minutes across the whole dataset was
rejected because typical runtimes vary considerably from one genre to
another.

For **Q2 and Q3 (most popular genres)**, popularity is measured by the total
number of IMDb votes accumulated by films of a genre released in a given
year, as votes reflect actual audience reach and engagement. Film counts
only measure production volume and mean ratings measure perceived quality
rather than popularity, though both metrics are still shown in the output
tables. Because recent releases naturally gather far more votes in absolute
terms than older ones, Q3 compares relative shares within each year (the
top 3 genres' share of votes, votes per film, and mean rating) instead of
raw vote totals.

For **Q4 (one-hit directors)**, directors are extracted from `title.crew`.
We read "multiple other movies with an average rating <= 6.0" as having at
least two other feature films each rated `<= 6.0` (since `averageRating` is
stored per film), while also reporting the average rating across those
other films. Only films with at least 100 votes are included so that ratings
based on a handful of users do not skew the results, at the cost of leaving
out very small productions.

For **Q5 (TV series)**, season size counts all episodes listed in
`title.episode`. The best and worst episodes are selected among episodes
with at least 10 votes (breaking ties by vote count), and the last season
is taken as the highest season number. Window functions (`row_number`
partitioned by series) were preferred over self-joins because they resolve
the ranking in a single pass.

For **Q6 (multilingual actors)**, IMDb does not publish the original
language of a title. We therefore infer it from `title.akas` by matching
alternative titles equal to `originalTitle`, mapping them via their
`language` column (or falling back to the primary language of their
`region`), and keeping the most frequent language. A *successful* film is
defined as having a rating `>= 7.0` and at least 1,000 votes. This
heuristic can misclassify certain international co-productions or
multilingual countries, while querying an external source such as Wikidata
(property P364) was kept out of scope.
*Execution status: the job (`jobs/exploration/q6_multilingual_actors.py`) is fully implemented, but could not be executed before the deadline within the 3.7 GB memory limit allocated to Docker on our machine, so no result table is available.*

For **Q7 (careers that peaked early)**, a career is defined as the list of
feature films (`>= 100` votes) where a person is credited as `actor` or
`actress`, restricted to careers spanning at least 20 years and 15 films. A
career "peaked early" when two conditions hold together: the highest-rated
film falls in the first third of the career, *and* the average rating of the
first half is higher than that of the second half. Requiring both
conditions avoids selecting careers with just one lucky early hit.
*Execution status: the job (`jobs/exploration/q7_early_peak_careers.py`) is fully implemented, but could not be executed before the deadline within the 3.7 GB memory limit allocated to Docker on our machine, so no result table is available.*

For **Q8 (multi-talented people)**, roles are combined from
`title.principals` and `title.crew`, merging `actor` and `actress` into a
single role. A person qualifies as multi-talented when they hold at least
two of the roles (actor, director, writer) on at least three films each.
Performance is evaluated using a *paired* design: for the same person, we
compare films where they held multiple roles against films where they held
only one role. This controls for the individual's baseline talent, which a
simple comparison between separate groups of people would not.
*Execution status: the job (`jobs/exploration/q8_multi_talented.py`) is fully implemented, but could not be executed before the deadline within the 3.7 GB memory limit allocated to Docker on our machine, so no result table is available.*

## 3. Data enrichment

<!-- RESULT:enrichment/coverage -->

### Enrichment coverage

| entity | n_targets | n_enriched | n_with_plot_section | n_corrupt_raw_records | pct_enriched |
| --- | --- | --- | --- | --- | --- |
| films | 6821 | 3332 | 3115 | 0 | 48.800 |
| principals | 10147 | 7626 |  | 0 | 75.200 |

_2 of 2 rows shown._

<!-- /RESULT -->

<!-- RESULT:enrichment/notes_breakdown -->

### Why some records are not enriched

| entity | note | count |
| --- | --- | --- |
| film | Not fetched yet. | 3321 |
| film |  | 3115 |
| film | Article has no plot section; lead section used instead. | 217 |
| film | No English Wikipedia article for this item. | 117 |
| film | No Wikidata item linked to this IMDb id. | 51 |
| principal |  | 7626 |
| principal | No Wikidata item linked to this IMDb id. | 1698 |
| principal | No English Wikipedia article for this item. | 822 |
| principal | Wikipedia article could not be retrieved. | 1 |

_9 of 9 rows shown._

<!-- /RESULT -->

<!-- RESULT:enrichment/sample_films -->

### Sample of enriched films (rank 1 per genre and year)

| year | genre | rank_in_genre_year | tconst | title | rating | votes | synopsis | synopsis_origin | lineage_source_url | lineage_api_url | lineage_wikidata_uri | lineage_revision_id | lineage_retrieved_at | license | note |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| 2009 | Drama | 1 | tt0361748 | Inglourious Basterds | 8.400 | 1830232 | In 1941, Austrian SS-Standartenführer Hans Landa, nicknamed "The Jew Hunter," interrogates French dairy farmer Perrier LaPadite about a fugitive Jewish family, the Dreyfuses. Suspecting he is hiding t... | plot section ('plot') | https://en.wikipedia.org/wiki/Inglourious_Basterds | https://en.wikipedia.org/w/api.php?action=query&format=json&formatversion=2&prop=extracts%7Cinfo&inprop=url&explaintext=1&exsectionformat=wiki&redirects=1&maxlag=5&titles=Inglourious+Basterds | http://www.wikidata.org/entity/Q153723 | 1384559716 | 2026-11-15T21:10:01+00:00 | CC BY-SA 4.0 (Wikipedia contributors) |  |
| 2009 | War | 1 | tt0361748 | Inglourious Basterds | 8.400 | 1830232 | In 1941, Austrian SS-Standartenführer Hans Landa, nicknamed "The Jew Hunter," interrogates French dairy farmer Perrier LaPadite about a fugitive Jewish family, the Dreyfuses. Suspecting he is hiding t... | plot section ('plot') | https://en.wikipedia.org/wiki/Inglourious_Basterds | https://en.wikipedia.org/w/api.php?action=query&format=json&formatversion=2&prop=extracts%7Cinfo&inprop=url&explaintext=1&exsectionformat=wiki&redirects=1&maxlag=5&titles=Inglourious+Basterds | http://www.wikidata.org/entity/Q153723 | 1384559716 | 2026-11-15T21:10:01+00:00 | CC BY-SA 4.0 (Wikipedia contributors) |  |
| 2008 | Action | 1 | tt0371746 | Iron Man | 7.900 | 1258846 | Tony Stark, who has inherited the defense contractor Stark Industries from his late father Howard Stark, tours in war-torn Afghanistan with his best friend and military liaison, James "Rhodey" Rhodes,... | plot section ('plot') | https://en.wikipedia.org/wiki/Iron_Man_(2008_film) | https://en.wikipedia.org/w/api.php?action=query&format=json&formatversion=2&prop=extracts%7Cinfo&inprop=url&explaintext=1&exsectionformat=wiki&redirects=1&maxlag=5&titles=Iron+Man+%282008+film%29 | http://www.wikidata.org/entity/Q192724 | 1384692269 | 2026-11-15T21:10:20+00:00 | CC BY-SA 4.0 (Wikipedia contributors) |  |
| 2008 | Sci-Fi | 1 | tt0371746 | Iron Man | 7.900 | 1258846 | Tony Stark, who has inherited the defense contractor Stark Industries from his late father Howard Stark, tours in war-torn Afghanistan with his best friend and military liaison, James "Rhodey" Rhodes,... | plot section ('plot') | https://en.wikipedia.org/wiki/Iron_Man_(2008_film) | https://en.wikipedia.org/w/api.php?action=query&format=json&formatversion=2&prop=extracts%7Cinfo&inprop=url&explaintext=1&exsectionformat=wiki&redirects=1&maxlag=5&titles=Iron+Man+%282008+film%29 | http://www.wikidata.org/entity/Q192724 | 1384692269 | 2026-11-15T21:10:20+00:00 | CC BY-SA 4.0 (Wikipedia contributors) |  |
| 2007 | Family | 1 | tt0373889 | Harry Potter and the Order of the Phoenix | 7.500 | 716460 | While staying at the Dursleys' in 1995, Harry Potter and Dudley Dursley are attacked by Dementors. Harry repels them using a Patronus spell. The Ministry of Magic detects Harry using magic and expels ... | plot section ('plot') | https://en.wikipedia.org/wiki/Harry_Potter_and_the_Order_of_the_Phoenix_(film) | https://en.wikipedia.org/w/api.php?action=query&format=json&formatversion=2&prop=extracts%7Cinfo&inprop=url&explaintext=1&exsectionformat=wiki&redirects=1&maxlag=5&titles=Harry+Potter+and+the+Order+of+the+Phoenix+%28film%29 | http://www.wikidata.org/entity/Q102235 | 1385280216 | 2026-11-15T21:10:24+00:00 | CC BY-SA 4.0 (Wikipedia contributors) |  |
| 2006 | Animation | 1 | tt0317219 | Cars | 7.300 | 554318 | In a world populated by anthropomorphic vehicles, the Dinoco 400 race marks the end of the Piston Cup race season. Heading into the event, retiring seven-time champion Strip "The King" Weathers, the c... | plot section ('plot') | https://en.wikipedia.org/wiki/Cars_(film) | https://en.wikipedia.org/w/api.php?action=query&format=json&formatversion=2&prop=extracts%7Cinfo&inprop=url&explaintext=1&exsectionformat=wiki&redirects=1&maxlag=5&titles=Cars+%28film%29 | http://www.wikidata.org/entity/Q182153 | 1385766781 | 2026-11-15T21:08:12+00:00 | CC BY-SA 4.0 (Wikipedia contributors) |  |
| 2005 | Adventure | 1 | tt0121766 | Star Wars: Episode III - Revenge of the Sith | 7.700 | 938689 | Three years after the start of the Clone Wars, Jedi Master Obi-Wan Kenobi and Knight Anakin Skywalker lead a mission to rescue Supreme Chancellor Palpatine from the cyborg Separatist commander General... | plot section ('plot') | https://en.wikipedia.org/wiki/Star_Wars:_Episode_III_%E2%80%93_Revenge_of_the_Sith | https://en.wikipedia.org/w/api.php?action=query&format=json&formatversion=2&prop=extracts%7Cinfo&inprop=url&explaintext=1&exsectionformat=wiki&redirects=1&maxlag=5&titles=Star+Wars%3A+Episode+III+%E2%80%93+Revenge+of+the+Sith | http://www.wikidata.org/entity/Q42051 | 1384589081 | 2026-11-15T21:02:06+00:00 | CC BY-SA 4.0 (Wikipedia contributors) |  |
| 2005 | Animation | 1 | tt0351283 | Madagascar | 7.000 | 490245 | In New York City, a lion named Alex lives in the Central Park Zoo as a star attraction known as the "King of New York" with his best friends, a zebra named Marty, a giraffe named Melman, and a hippopo... | plot section ('plot') | https://en.wikipedia.org/wiki/Madagascar_(2005_film) | https://en.wikipedia.org/w/api.php?action=query&format=json&formatversion=2&prop=extracts%7Cinfo&inprop=url&explaintext=1&exsectionformat=wiki&redirects=1&maxlag=5&titles=Madagascar+%282005+film%29 | http://www.wikidata.org/entity/Q174555 | 1385871689 | 2026-11-15T21:09:48+00:00 | CC BY-SA 4.0 (Wikipedia contributors) |  |
| 2005 | Biography | 1 | tt0358273 | Walk the Line | 7.800 | 283559 | The film begins in 1968 with Johnny Cash performing at Folsom State Prison. As the audience of inmates cheer him on, Cash waits backstage near a table saw, which reminds him of his early life. Two dec... | plot section ('plot') | https://en.wikipedia.org/wiki/Walk_the_Line | https://en.wikipedia.org/w/api.php?action=query&format=json&formatversion=2&prop=extracts%7Cinfo&inprop=url&explaintext=1&exsectionformat=wiki&redirects=1&maxlag=5&titles=Walk+the+Line | http://www.wikidata.org/entity/Q154077 | 1381631292 | 2026-11-15T21:09:56+00:00 | CC BY-SA 4.0 (Wikipedia contributors) |  |
| 2005 | Comedy | 1 | tt0356910 | Mr. & Mrs. Smith | 6.500 | 605830 | John Smith, a construction executive, and his wife Jane, an IT recruiter, are at marriage counseling. They have been married for about five or six years, and their marriage is suffering from a lack of... | plot section ('plot') | https://en.wikipedia.org/wiki/Mr._%26_Mrs._Smith_(2005_film) | https://en.wikipedia.org/w/api.php?action=query&format=json&formatversion=2&prop=extracts%7Cinfo&inprop=url&explaintext=1&exsectionformat=wiki&redirects=1&maxlag=5&titles=Mr.+%26+Mrs.+Smith+%282005+film%29 | http://www.wikidata.org/entity/Q191040 | 1385614410 | 2026-11-15T21:09:54+00:00 | CC BY-SA 4.0 (Wikipedia contributors) |  |
| 2005 | Crime | 1 | tt0372784 | Batman Begins | 8.200 | 1754308 | In Gotham City, young Bruce Wayne falls down a well and develops a fear of bats. One evening, Bruce visits the opera with his parents; the performers masquerading as bats upset him, and he asks to lea... | plot section ('plot') | https://en.wikipedia.org/wiki/Batman_Begins | https://en.wikipedia.org/w/api.php?action=query&format=json&formatversion=2&prop=extracts%7Cinfo&inprop=url&explaintext=1&exsectionformat=wiki&redirects=1&maxlag=5&titles=Batman+Begins | http://www.wikidata.org/entity/Q166262 | 1385269981 | 2026-11-15T21:10:21+00:00 | CC BY-SA 4.0 (Wikipedia contributors) |  |
| 2005 | Family | 1 | tt0330373 | Harry Potter and the Goblet of Fire | 7.700 | 770275 | In 1994, Harry Potter has a nightmare in which a Muggle caretaker named Frank Bryce is murdered at the Riddle House after overhearing a plot by Lord Voldemort, Peter Pettigrew, and another man whom Ha... | plot section ('plot') | https://en.wikipedia.org/wiki/Harry_Potter_and_the_Goblet_of_Fire_(film) | https://en.wikipedia.org/w/api.php?action=query&format=json&formatversion=2&prop=extracts%7Cinfo&inprop=url&explaintext=1&exsectionformat=wiki&redirects=1&maxlag=5&titles=Harry+Potter+and+the+Goblet+of+Fire+%28film%29 | http://www.wikidata.org/entity/Q102225 | 1385403024 | 2026-11-15T21:08:37+00:00 | CC BY-SA 4.0 (Wikipedia contributors) |  |
| 2005 | Fantasy | 1 | tt0121766 | Star Wars: Episode III - Revenge of the Sith | 7.700 | 938689 | Three years after the start of the Clone Wars, Jedi Master Obi-Wan Kenobi and Knight Anakin Skywalker lead a mission to rescue Supreme Chancellor Palpatine from the cyborg Separatist commander General... | plot section ('plot') | https://en.wikipedia.org/wiki/Star_Wars:_Episode_III_%E2%80%93_Revenge_of_the_Sith | https://en.wikipedia.org/w/api.php?action=query&format=json&formatversion=2&prop=extracts%7Cinfo&inprop=url&explaintext=1&exsectionformat=wiki&redirects=1&maxlag=5&titles=Star+Wars%3A+Episode+III+%E2%80%93+Revenge+of+the+Sith | http://www.wikidata.org/entity/Q42051 | 1384589081 | 2026-11-15T21:02:06+00:00 | CC BY-SA 4.0 (Wikipedia contributors) |  |
| 2005 | Horror | 1 | tt0360486 | Constantine | 7.000 | 416235 | In Mexico, a scavenger discovers the tip of the spear that pierced Jesus Christ and, after becoming possessed, takes it to Los Angeles. There, cynical occult expert John Constantine exorcises a demon ... | plot section ('plot') | https://en.wikipedia.org/wiki/Constantine_(film) | https://en.wikipedia.org/w/api.php?action=query&format=json&formatversion=2&prop=extracts%7Cinfo&inprop=url&explaintext=1&exsectionformat=wiki&redirects=1&maxlag=5&titles=Constantine+%28film%29 | http://www.wikidata.org/entity/Q219150 | 1385126152 | 2026-11-15T21:09:59+00:00 | CC BY-SA 4.0 (Wikipedia contributors) |  |
| 2005 | Music | 1 | tt0358273 | Walk the Line | 7.800 | 283559 | The film begins in 1968 with Johnny Cash performing at Folsom State Prison. As the audience of inmates cheer him on, Cash waits backstage near a table saw, which reminds him of his early life. Two dec... | plot section ('plot') | https://en.wikipedia.org/wiki/Walk_the_Line | https://en.wikipedia.org/w/api.php?action=query&format=json&formatversion=2&prop=extracts%7Cinfo&inprop=url&explaintext=1&exsectionformat=wiki&redirects=1&maxlag=5&titles=Walk+the+Line | http://www.wikidata.org/entity/Q154077 | 1381631292 | 2026-11-15T21:09:56+00:00 | CC BY-SA 4.0 (Wikipedia contributors) |  |

_15 of 500 rows shown._

<!-- /RESULT -->

<!-- RESULT:enrichment/sample_principals -->

### Sample of enriched principals

| tconst | principal_rank | nconst | name | category | birthYear | deathYear | biography | lineage_source_url | lineage_api_url | lineage_wikidata_uri | lineage_retrieved_at | license | note |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| tt0035423 | 1 | nm0000212 | Meg Ryan | actress | 1961 |  | Margaret Mary Emily Anne Hyra (born November 19, 1961), known professionally as Meg Ryan, is an American actress. Known for playing quirky, charismatic women since the late 1980s, Ryan is particularly... | https://en.wikipedia.org/wiki/Meg_Ryan | https://en.wikipedia.org/w/api.php?action=query&format=json&formatversion=2&prop=extracts%7Cinfo&inprop=url&exintro=1&explaintext=1&exlimit=20&redirects=1&maxlag=5&titles=Demi+Moore%7CJulianne+Moore%7CBill+Murray%7CMike+Myers%7CJack+Nicholson%7CGary+Oldman%7CAl+Pacino%7CBill+Paxton%7CMichelle+Pfeiffer%7CRyan+Phillippe%7CRiver+Phoenix%7CNatalie+Portman%7CParker+Posey%7CKeanu+Reeves%7CChristina+Ricci%7CMolly+Ringwald%7CTim+Robbins%7CJulia+Roberts%7CMimi+Rogers%7CMeg+Ryan | http://www.wikidata.org/entity/Q167498 | 2026-11-15T21:11:12+00:00 | CC BY-SA 4.0 (Wikipedia contributors) |  |
| tt0035423 | 2 | nm0413168 | Hugh Jackman | actor | 1968 |  | Hugh Michael Jackman (born 12 October 1968) is an Australian and British actor, singer, and producer. Prominent on both screen and stage, he has received various accolades, including a Primetime Emmy ... | https://en.wikipedia.org/wiki/Hugh_Jackman | https://en.wikipedia.org/w/api.php?action=query&format=json&formatversion=2&prop=extracts%7Cinfo&inprop=url&exintro=1&explaintext=1&exlimit=20&redirects=1&maxlag=5&titles=Tsutomu+Isobe%7CLeon+Isaac+Kennedy%7CMaia+Morgenstern%7CAyumi+Ito%7CHideaki+It%C5%8D%7CMarcel+Iure%C8%99%7CJunko+Iwao%7CMitsuo+Iwata%7CEddie+Izzard%7CViolent+J%7CHugh+Jackman%7CRobert+Jacks+%28actor%29%7CNatalie+Mendoza%7CPhil+Jackson%7CVictoria+Jackson%7CErnst+Jacobi%7CJagadish%7CJai+%28actor%29%7CClifton+James%7CKevin+James | http://www.wikidata.org/entity/Q129591 | 2026-11-15T21:13:21+00:00 | CC BY-SA 4.0 (Wikipedia contributors) |  |
| tt0035423 | 3 | nm0000630 | Liev Schreiber | actor | 1967 |  | Isaac Liev Schreiber ( LEE-ev SHRY-bər; born October 4, 1967) is an American actor. He has received several accolades including a Tony Award as well as nominations for nine Primetime Emmy Awards and f... | https://en.wikipedia.org/wiki/Liev_Schreiber | https://en.wikipedia.org/w/api.php?action=query&format=json&formatversion=2&prop=extracts%7Cinfo&inprop=url&exintro=1&explaintext=1&exlimit=20&redirects=1&maxlag=5&titles=Theresa+Russell%7CRene+Russo%7CFred+Savage%7CGreta+Scacchi%7CClaudia+Schiffer%7CLiev+Schreiber%7CJerry+Seinfeld%7CTom+Selleck%7CPeter+Sellers%7CTupac+Shakur%7CWilliam+Shatner%7CAlly+Sheedy%7CMartin+Sheen%7CGary+Sinise%7CTom+Skerritt%7CHelen+Slater%7CWesley+Snipes%7CPaul+Sorvino%7CTalisa+Soto%7CSissy+Spacek | http://www.wikidata.org/entity/Q298368 | 2026-11-15T21:11:31+00:00 | CC BY-SA 4.0 (Wikipedia contributors) |  |
| tt0061876 | 1 | nm0603314 | Nonna Mordyukova | actress | 1925 | 2008 | Noyabrina Viktorovna Mordyukova (Russian: Ноябри́на (Но́нна) Ви́кторовна Мордюко́ва; 25 November 1925 – 6 July 2008) was a Soviet and Russian actress and People's Artist of the USSR (1974). She was th... | https://en.wikipedia.org/wiki/Nonna_Mordyukova | https://en.wikipedia.org/w/api.php?action=query&format=json&formatversion=2&prop=extracts%7Cinfo&inprop=url&exintro=1&explaintext=1&exlimit=20&redirects=1&maxlag=5&titles=Sheri+Moon+Zombie%7CDeborah+Moore%7CFabio+Testi%7CMandy+Moore%7CMichael+Moore%7CNick+Moran%7CNonna+Mordyukova%7CDino+Morea%7CJeanne+Moreau%7CMarguerite+Moreau%7CDror+Moreh%7CMarc+Moreland%7CNanni+Moretti%7CTobias+Moretti%7CJeffrey+Dean+Morgan%7CNancy+Morgan%7CClaudia+Mori%7CMichael+Moriarty%7CTara+Morice%7CShowtaro+Morikubo | http://www.wikidata.org/entity/Q242138 | 2026-11-15T21:14:24+00:00 | CC BY-SA 4.0 (Wikipedia contributors) |  |
| tt0061876 | 2 | nm0125909 | Rolan Bykov | actor | 1929 | 1998 | Rolan Antonovich Bykov (October 12, 1929 – October 6, 1998) was a Soviet and Russian stage and film actor, director, screenwriter and pedagogue. He was named People's Artist of the USSR in 1990.... | https://en.wikipedia.org/wiki/Rolan_Bykov | https://en.wikipedia.org/w/api.php?action=query&format=json&formatversion=2&prop=extracts%7Cinfo&inprop=url&exintro=1&explaintext=1&exlimit=20&redirects=1&maxlag=5&titles=Clarissa+Burt%7CCorey+Burton%7CJames+Burton%7CBen+Burtt%7CZoya+Buryak%7CTimothy+Busfield%7CBilly+Green+Bush%7CGeorge+H.+W.+Bush%7CGeorge+W.+Bush%7CRaymond+Bussi%C3%A8res%7CAdam+Butcher%7CGerard+Butler%7CJean+Butler%7CViktor+Bychkov+%28actor%29%7CCarsten+Byhring%7CRolan+Bykov%7CDan+Byrd%7CDavid+Byrne%7CRose+Byrne%7CIldik%C3%B3+B%C3%A1ns%C3%A1gi | http://www.wikidata.org/entity/Q378333 | 2026-11-15T21:12:43+00:00 | CC BY-SA 4.0 (Wikipedia contributors) |  |
| tt0061876 | 3 | nm0623970 | Raisa Nedashkovskaya | actress | 1943 |  | Rayisa Nedashkivska (Ukrainian: Раїса Недашківська Rayisa Nedashkivska, Russian: Раиса Недашковская; born 17 February 1943) is a Ukrainian and Soviet-era theater and cinema actress. In 1993 she was aw... | https://en.wikipedia.org/wiki/Rayisa_Nedashkivska | https://en.wikipedia.org/w/api.php?action=query&format=json&formatversion=2&prop=extracts%7Cinfo&inprop=url&exintro=1&explaintext=1&exlimit=20&redirects=1&maxlag=5&titles=Giorgi+Nakashidze%7CMiki+Nakatani%7CNanda+%28actress%29%7CCharles+Napier+%28actor%29%7CYvette+Nipar%7CKen+Narita%7CNassar+%28actor%29%7CAli+Nassirian%7CAlok+Nath%7CYui+Natsukawa%7CMari+Natsuki%7CDavid+Naughton%7CDeepti+Naval%7CGor%C5%8D+Naya%7CGennady+Nazarov%7CMehdi+Nebbou%7CRayisa+Nedashkivska%7CToshie+Negishi%7CStacey+Nelkin%7CKate+Nelligan | http://www.wikidata.org/entity/Q2382979 | 2026-11-15T21:14:25+00:00 | CC BY-SA 4.0 (Wikipedia contributors) |  |
| tt0067625 | 1 | nm0125909 | Rolan Bykov | actor | 1929 | 1998 | Rolan Antonovich Bykov (October 12, 1929 – October 6, 1998) was a Soviet and Russian stage and film actor, director, screenwriter and pedagogue. He was named People's Artist of the USSR in 1990.... | https://en.wikipedia.org/wiki/Rolan_Bykov | https://en.wikipedia.org/w/api.php?action=query&format=json&formatversion=2&prop=extracts%7Cinfo&inprop=url&exintro=1&explaintext=1&exlimit=20&redirects=1&maxlag=5&titles=Clarissa+Burt%7CCorey+Burton%7CJames+Burton%7CBen+Burtt%7CZoya+Buryak%7CTimothy+Busfield%7CBilly+Green+Bush%7CGeorge+H.+W.+Bush%7CGeorge+W.+Bush%7CRaymond+Bussi%C3%A8res%7CAdam+Butcher%7CGerard+Butler%7CJean+Butler%7CViktor+Bychkov+%28actor%29%7CCarsten+Byhring%7CRolan+Bykov%7CDan+Byrd%7CDavid+Byrne%7CRose+Byrne%7CIldik%C3%B3+B%C3%A1ns%C3%A1gi | http://www.wikidata.org/entity/Q378333 | 2026-11-15T21:12:43+00:00 | CC BY-SA 4.0 (Wikipedia contributors) |  |
| tt0067625 | 2 | nm0813463 | Anatoliy Solonitsyn | actor | 1934 | 1982 | Anatoly (Otto) Alekseyevich Solonitsyn (Russian: Анатолий (Отто) Алексеевич Солоницын; 30 August 1934 – 11 June 1982) was a Soviet actor known for his spiritually profound and deeply philosophical per... | https://en.wikipedia.org/wiki/Anatoly_Solonitsyn | https://en.wikipedia.org/w/api.php?action=query&format=json&formatversion=2&prop=extracts%7Cinfo&inprop=url&exintro=1&explaintext=1&exlimit=20&redirects=1&maxlag=5&titles=T.+Ryder+Smith%7CWillie+Mae+Ford+Smith%7CSonja+Smits%7CJurnee+Smollett%7CAndrey+Smolyakov%7CArtur+Smolyaninov%7CCarrie+Snodgress%7CBrittany+Snow%7CSuzanne+Snyder%7CRobin+Soans%7CShobana%7CAna+Sofrenovi%C4%87%7CEkta+Sohini%7CPeter+Sohn%7CMarilyn+Sokol%7CLaura+del+Sol%7CAmparo+Soler+Leal%7CCristian+Solimeno%7CAnatoly+Solonitsyn%7CElena+Solovey | http://www.wikidata.org/entity/Q787038 | 2026-11-15T21:14:46+00:00 | CC BY-SA 4.0 (Wikipedia contributors) |  |
| tt0067625 | 3 | nm0952519 | Vladimir Zamanskiy | actor | 1926 |  | Vladimir Petrovich Zamansky (Russian: Владимир Петрович Заманский; born 6 February 1926) is a Soviet and Russian stage and film actor. He was named a People's Artist of the RSFSR in 1989, the highest ... | https://en.wikipedia.org/wiki/Vladimir_Zamansky | https://en.wikipedia.org/w/api.php?action=query&format=json&formatversion=2&prop=extracts%7Cinfo&inprop=url&exintro=1&explaintext=1&exlimit=20&redirects=1&maxlag=5&titles=Yoo+Ji-tae%7CMicha%C3%ABl+Youn%7CAden+Young%7CAlan+Young%7CBurt+Young%7CNeil+Young%7CPeter+Youngblood+Hills%7CYuen+Biao%7CYuen+Wah%7CTran+Nu+Yen+Khe%7CU%C4%9Fur+Y%C3%BCcel%7CWilliam+Zabka%7CZabou+Breitman%7CAndrejs+%C5%BDagars%7CAlex+Zahara%7CAhmed+Zaki+%28actor%29%7CZbigniew+Zamachowski%7CVladimir+Zamansky%7CAnjala+Zaveri%7CWiktor+Zborowski | http://www.wikidata.org/entity/Q3561885 | 2026-11-15T21:15:32+00:00 | CC BY-SA 4.0 (Wikipedia contributors) |  |
| tt0075788 | 2 | nm0389888 | Hoi-Chuen Lee | archive_footage | 1901 | 1965 | Lee Moon-shuen (Chinese: 李滿船; 4 February 1901 – 7 February 1965), known professionally as Lee Hoi-chuen (Chinese: 李海泉), was a Chinese opera singer and film actor in Hong Kong. He was the father of Bru... | https://en.wikipedia.org/wiki/Lee_Hoi-chuen | https://en.wikipedia.org/w/api.php?action=query&format=json&formatversion=2&prop=extracts%7Cinfo&inprop=url&exintro=1&explaintext=1&exlimit=20&redirects=1&maxlag=5&titles=Thom+Hoffman%7CFrank+Hoffmann+%28actor%29%7CMarco+Hofschneider%7CStephen+Hogan%7CLee+Hoi-chuen%7CLaurie+Holden%7CTom+Hollander%7CJosh+Holloway%7CSverre+Holm%7CTina+Holmes%7CErik+Holmey%7CMark+Holton%7CJames+Hong%7CY%C5%8Dko+Honna%7CNikita+Hopkins%7CPatrick+Horgan+%28actor%29%7CHideyuki+Hori%7CJunko+Hori%7CAd-Rock%7CMichael+Horse | http://www.wikidata.org/entity/Q712093 | 2026-11-15T21:13:19+00:00 | CC BY-SA 4.0 (Wikipedia contributors) |  |
| tt0075788 | 3 | nm0000045 | Bruce Lee | archive_footage | 1940 | 1973 | Bruce Lee (born Lee Jun-fan; November 27, 1940 – July 20, 1973) was a Hong Kong and American martial artist, actor, filmmaker, and philosopher. He was the founder of Jeet Kune Do, a hybrid martial art... | https://en.wikipedia.org/wiki/Bruce_Lee | https://en.wikipedia.org/w/api.php?action=query&format=json&formatversion=2&prop=extracts%7Cinfo&inprop=url&exintro=1&explaintext=1&exlimit=20&redirects=1&maxlag=5&titles=Lauren+Bacall%7CJohn+Belushi%7CIngmar+Bergman%7CMarlon+Brando%7CRichard+Burton%7CBette+Davis%7CMarlene+Dietrich%7CKirk+Douglas%7CHenry+Fonda%7CJohn+Gielgud%7CKatharine+Hepburn%7CCharlton+Heston%7CGene+Kelly%7CStanley+Kubrick%7CAkira+Kurosawa%7CAlan+Ladd%7CBurt+Lancaster%7CBruce+Lee%7CJames+Mason%7CMarcello+Mastroianni | http://www.wikidata.org/entity/Q16397 | 2026-11-15T21:11:03+00:00 | CC BY-SA 4.0 (Wikipedia contributors) |  |
| tt0076917 | 1 | nm0488814 | Keith Larsen | actor | 1924 | 2006 | Keith Larsen (born Keith Larsen Burt, June 17, 1924 – December 13, 2006) was an American actor who starred in three short-lived television series between 1955 and 1961.... | https://en.wikipedia.org/wiki/Keith_Larsen | https://en.wikipedia.org/w/api.php?action=query&format=json&formatversion=2&prop=extracts%7Cinfo&inprop=url&exintro=1&explaintext=1&exlimit=20&redirects=1&maxlag=5&titles=Amanda+Langlet%7CJi%C5%99%C3%AD+Langmajer%7CMargaret+Langrick%7CBrooke+Langton%7CG%C3%A9rard+Lanvin%7CClaude+Lanzmann%7CDaniel+Lapaine%7CJane+Lapotaire%7CAlexandra+Maria+Lara%7CVsevolod+Larionov%7CJohn+Larroquette%7CKeith+Larsen%7CThomas+Bo+Larsen%7CBrie+Larson%7CJill+Larson%7CBob+Larson%7CLars+Andreas+Larssen%7CFrano+Lasi%C4%87%7CAndy+Lau%7CLiubomiras+Laucevi%C4%8Dius | http://www.wikidata.org/entity/Q556265 | 2026-11-15T21:13:43+00:00 | CC BY-SA 4.0 (Wikipedia contributors) |  |
| tt0076917 | 2 | nm0488814 | Keith Larsen | director | 1924 | 2006 | Keith Larsen (born Keith Larsen Burt, June 17, 1924 – December 13, 2006) was an American actor who starred in three short-lived television series between 1955 and 1961.... | https://en.wikipedia.org/wiki/Keith_Larsen | https://en.wikipedia.org/w/api.php?action=query&format=json&formatversion=2&prop=extracts%7Cinfo&inprop=url&exintro=1&explaintext=1&exlimit=20&redirects=1&maxlag=5&titles=Amanda+Langlet%7CJi%C5%99%C3%AD+Langmajer%7CMargaret+Langrick%7CBrooke+Langton%7CG%C3%A9rard+Lanvin%7CClaude+Lanzmann%7CDaniel+Lapaine%7CJane+Lapotaire%7CAlexandra+Maria+Lara%7CVsevolod+Larionov%7CJohn+Larroquette%7CKeith+Larsen%7CThomas+Bo+Larsen%7CBrie+Larson%7CJill+Larson%7CBob+Larson%7CLars+Andreas+Larssen%7CFrano+Lasi%C4%87%7CAndy+Lau%7CLiubomiras+Laucevi%C4%8Dius | http://www.wikidata.org/entity/Q556265 | 2026-11-15T21:13:43+00:00 | CC BY-SA 4.0 (Wikipedia contributors) |  |
| tt0076917 | 3 | nm0488814 | Keith Larsen | writer | 1924 | 2006 | Keith Larsen (born Keith Larsen Burt, June 17, 1924 – December 13, 2006) was an American actor who starred in three short-lived television series between 1955 and 1961.... | https://en.wikipedia.org/wiki/Keith_Larsen | https://en.wikipedia.org/w/api.php?action=query&format=json&formatversion=2&prop=extracts%7Cinfo&inprop=url&exintro=1&explaintext=1&exlimit=20&redirects=1&maxlag=5&titles=Amanda+Langlet%7CJi%C5%99%C3%AD+Langmajer%7CMargaret+Langrick%7CBrooke+Langton%7CG%C3%A9rard+Lanvin%7CClaude+Lanzmann%7CDaniel+Lapaine%7CJane+Lapotaire%7CAlexandra+Maria+Lara%7CVsevolod+Larionov%7CJohn+Larroquette%7CKeith+Larsen%7CThomas+Bo+Larsen%7CBrie+Larson%7CJill+Larson%7CBob+Larson%7CLars+Andreas+Larssen%7CFrano+Lasi%C4%87%7CAndy+Lau%7CLiubomiras+Laucevi%C4%8Dius | http://www.wikidata.org/entity/Q556265 | 2026-11-15T21:13:43+00:00 | CC BY-SA 4.0 (Wikipedia contributors) |  |
| tt0078349 | 1 | nm0847580 | Keiko Takeshita | actress | 1953 |  | Keiko Takeshita (竹下景子 Takeshita Keiko; born on September 15, 1953, in Higashi-ku, Nagoya, Japan) is a Japanese actress. She starred in the Japanese version of From Up on Poppy Hill as Hana Matsuzaki.... | https://en.wikipedia.org/wiki/Keiko_Takeshita | https://en.wikipedia.org/w/api.php?action=query&format=json&formatversion=2&prop=extracts%7Cinfo&inprop=url&exintro=1&explaintext=1&exlimit=20&redirects=1&maxlag=5&titles=Aitana+S%C3%A1nchez-Gij%C3%B3n%7COleg+Tabakov%7CJasmin+Tabatabai%7CTakashi+Tachibana+%28journalist%29%7CLjuba+Tadi%C4%87%7CCary-Hiroyuki+Tagawa%7CSa%C3%AFd+Taghmaoui%7CTomorowo+Taguchi%7CDalip+Tahil%7CHitoshi+Takagi%7CKen+Takakura%7CMinami+Takayama%7CShinji+Takeda%7CNaoto+Takenaka%7CKeiko+Takeshita%7CYuko+Takeuchi%7CAna+Claudia+Talanc%C3%B3n%7CAndr%C3%A9+Leon+Talley%7CPatricia+Tallman%7CAmber+Tamblyn | http://www.wikidata.org/entity/Q3543913 | 2026-11-15T21:15:08+00:00 | CC BY-SA 4.0 (Wikipedia contributors) |  |

_15 of 500 rows shown._

<!-- /RESULT -->

### Methodological explanation - enrichment

To keep the enrichment volume manageable while staying representative, we
scoped the selection to the top 10 most-voted feature films for each
`(year, genre)` pair between 1980 and 2025, along with the first three
billed principals for every film. This gives 9,832 `(film, genre, year)`
slots, representing 6,821 distinct films and 10,147 distinct individuals.
These bounds are purely controlled via configuration parameters (`TOP_N`,
`N_PRINCIPALS`, and the year range); the code itself has no hard-coded limit
and could run unchanged on the entire dataset.

We used English Wikipedia (CC BY-SA licence) as our single open,
human-written enrichment source. Rather than searching Wikipedia by title,
which often fails on homonyms such as *"Home"* or *"Up"*, we linked IMDb IDs
to Wikipedia pages through Wikidata using property `P345`, querying Wikidata
in batches of 200 identifiers with SPARQL. For films, we extract the "Plot"
section (falling back to Synopsis, Premise, or Plot summary, or to the
lead section with an explicit note when no plot section is present). For
people, we retrieve the lead section of their biography in batches of 20
pages per API request.

To comply with Wikimedia's API guidelines, requests run strictly
sequentially with a minimum delay of 0.3 seconds between calls, a custom
`User-Agent` header containing a contact address, the `maxlag` parameter,
and exponential backoff retries on HTTP 429 and 503 responses. Raw responses
are appended to JSONL files and already fetched IDs are skipped on startup,
allowing an interrupted run to resume cleanly without re-querying.

While the data collection script is written in standard Python as allowed,
all downstream processing is handled in Spark (`build_enriched.py`). Spark
reads the JSONL dumps against an explicit schema (counting any corrupt
records), normalises the text, deduplicates entries by keeping the latest
record, and joins the results back to our selected films and principals.
When data is missing (no Wikidata item, no English Wikipedia article, or no
plot section), a `note` column records the exact reason. Every enriched row
retains its full lineage: article URL, exact API request URL, Wikidata URI,
page revision ID, retrieval timestamp, and licence.

Before choosing Wikipedia, we looked at alternatives such as TMDb and OMDb.
TMDb provides structured synopses, but requires an API key and only gives
short overviews; OMDb enforces tight daily rate limits; and scraping HTML
pages directly is fragile and less respectful of Wikimedia's servers than
the official API. The main bottleneck in our pipeline is having to fetch full
articles to extract the Plot section, which takes one request per film. For
lack of time, we fetched 3,500 of the 6,821 selected films, leaving the rest
tagged with `"Not fetched yet"`. Re-running the fetcher script will complete
the remaining entries without changing any code.

In the end, out of the 3,500 films fetched, 3,332 have a synopsis (3,115
from a plot section and 217 from the lead section), which corresponds to
48.8% of the 6,821 target films; 117 films had no English Wikipedia article
and 51 had no linked Wikidata item. On the people side, 7,626 of the 10,147
principals (75.2%) received a biography, while 1,698 had no Wikidata item
and 822 had no English article. None of the raw JSONL records were corrupt.

## 4. Streaming

<!-- RESULT:streaming/run_metrics -->

### Streaming run metrics

| matched_edits | distinct_entities | title_edits | person_edits | first_event | last_event | avg_ingest_delay_s | p95_ingest_delay_s | events_in_closed_windows | matched_events_older_than_10min |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| 103 | 85 | 103 | 0 | 2026-11-15 21:16:51 | 2026-11-15 21:46:21 | 248.500 | 828.400 | 2707 | 21 |

_1 of 1 rows shown._

<!-- /RESULT -->

<!-- RESULT:streaming/query_progress -->

### Micro-batch statistics per streaming query

| name | n_batches | input_rows | avg_processed_rows_per_s | avg_batch_duration_ms | max_state_rows | max_rows_dropped_by_watermark |
| --- | --- | --- | --- | --- | --- | --- |
| all_window_counts | 4 | 8926 | 89.100 | 14754.000 | 4463 | 0 |
| matched_events | 2 | 4463 | 88.600 | 15303.000 | 4463 | 0 |
| matched_window_counts | 4 | 8926 | 83.500 | 16437.000 | 4463 | 0 |
| sliding_top_pages | 4 | 8926 | 86.500 | 16193.000 | 4463 | 0 |

_4 of 4 rows shown._

<!-- /RESULT -->

<!-- RESULT:streaming/match_rate_by_window -->

### Share of Wikipedia edits matching the IMDb dataset

| window_start | all_edits | matched_edits | match_rate_pct |
| --- | --- | --- | --- |
| 2026-11-15 21:15:00 | 522 | 12 | 2.300 |
| 2026-11-15 21:20:00 | 746 | 15 | 2.010 |
| 2026-11-15 21:25:00 | 724 | 14 | 1.930 |
| 2026-11-15 21:30:00 | 715 | 15 | 2.100 |

_4 of 4 rows shown._

<!-- /RESULT -->

<!-- RESULT:streaming/window_counts -->

### Matched edits per 5-minute tumbling window

| window_start | window_end | entity_type | confidence | n_edits | n_entities | n_bot_edits | bytes_changed | avg_ingest_delay_s |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| 2026-11-15 21:15:00 | 2026-11-15 21:20:00 | title | high | 6 | 6 | 0 | 720 | 812.565 |
| 2026-11-15 21:15:00 | 2026-11-15 21:20:00 | title | low | 1 | 1 | 0 | 12 | 835.267 |
| 2026-11-15 21:15:00 | 2026-11-15 21:20:00 | title | medium | 5 | 5 | 1 | 325 | 863.044 |
| 2026-11-15 21:20:00 | 2026-11-15 21:25:00 | title | high | 2 | 2 | 0 | -22 | 641.915 |
| 2026-11-15 21:20:00 | 2026-11-15 21:25:00 | title | medium | 13 | 11 | 0 | -189 | 632.004 |
| 2026-11-15 21:25:00 | 2026-11-15 21:30:00 | title | high | 2 | 2 | 0 | 57 | 340.505 |
| 2026-11-15 21:25:00 | 2026-11-15 21:30:00 | title | medium | 12 | 10 | 1 | 1436 | 327.807 |
| 2026-11-15 21:30:00 | 2026-11-15 21:35:00 | title | high | 3 | 2 | 1 | -22 | 114.605 |
| 2026-11-15 21:30:00 | 2026-11-15 21:35:00 | title | low | 2 | 2 | 0 | 138 | 85.138 |
| 2026-11-15 21:30:00 | 2026-11-15 21:35:00 | title | medium | 10 | 8 | 0 | -288 | 74.229 |

_10 of 10 rows shown._

<!-- /RESULT -->

<!-- RESULT:streaming/top_entities -->

### Most edited IMDb entities during the run

| entity_type | imdb_id | display_name | n_edits | n_editors | confidence |
| --- | --- | --- | --- | --- | --- |
| title | tt7818580 | While at War | 7 | 1 | medium |
| title | tt0387714 | All Grown Up! | 4 | 2 | medium |
| title | tt5433114 | Diego Maradona | 4 | 2 | medium |
| title | tt0142251 | Dragon Ball: Curse of the Blood Rubies | 2 | 2 | medium |
| title | tt0248428 | Shiva | 2 | 2 | medium |
| title | tt15799866 | Silent Night | 2 | 2 | low |
| title | tt1623205 | Oz the Great and Powerful | 2 | 1 | medium |
| title | tt21637282 | Charcoal | 2 | 1 | medium |
| title | tt38058097 | Club Kid | 2 | 1 | high |
| title | tt0032851 | Northwest Passage | 1 | 1 | high |
| title | tt0038048 | San Antonio | 1 | 1 | medium |
| title | tt0048937 | Alexander the Great | 1 | 1 | low |
| title | tt0053270 | Shadows | 1 | 1 | medium |
| title | tt0063291 | Memories of Underdevelopment | 1 | 1 | medium |
| title | tt0077360 | Comes a Horseman | 1 | 1 | medium |
| title | tt0081291 | Out of the Blue | 1 | 1 | medium |
| title | tt0083203 | Time for Revenge | 1 | 1 | medium |
| title | tt0088595 | Robotech | 1 | 1 | medium |
| title | tt0089670 | National Lampoon's European Vacation | 1 | 1 | medium |
| title | tt0097958 | National Lampoon's Christmas Vacation | 1 | 1 | medium |
| title | tt0101761 | The Doors | 1 | 1 | medium |
| title | tt0102690 | Popcorn | 1 | 1 | low |
| title | tt0104774 | A Place in the World | 1 | 1 | high |
| title | tt0104797 | Malcolm X | 1 | 1 | high |
| title | tt0107563 | The Meteor Man | 1 | 1 | high |

_25 of 85 rows shown._

<!-- /RESULT -->

<!-- RESULT:streaming/sample_matches -->

### Sample of matched events

| event_time | wiki | title | entity_type | imdb_id | display_name | confidence | delay_s |
| --- | --- | --- | --- | --- | --- | --- | --- |
| 2026-11-15 21:17:16 | enwiki | Northwest Passage (TV series) | title | tt0032851 | Northwest Passage | high | 865.900 |
| 2026-11-15 21:17:56 | enwiki | Fatherland (2026 film) | title | tt37304295 | Fatherland | high | 828.400 |
| 2026-11-15 21:18:16 | enwiki | A Place in the World (film) | title | tt0104774 | A Place in the World | high | 812.000 |
| 2026-11-15 21:18:36 | enwiki | The Meteor Man (film) | title | tt0107563 | The Meteor Man | high | 793.700 |
| 2026-11-15 21:18:56 | frwiki | Parasite (film, 2019) | title | tt6751668 | Parasite | high | 799.600 |
| 2026-11-15 21:19:42 | enwiki | Common Ground (2002 film) | title | tt0329330 | Common Ground | high | 775.800 |
| 2026-11-15 21:20:18 | enwiki | Will (TV series) | title | tt1636844 | Will | high | 741.800 |
| 2026-11-15 21:23:48 | enwiki | The Little Vampire (TV series) | title | tt0192255 | The Little Vampire | high | 542.100 |
| 2026-11-15 21:26:56 | enwiki | Paper Tiger (2026 film) | title | tt34385689 | Paper Tiger | high | 359.500 |
| 2026-11-15 21:27:35 | enwiki | Richie Rich (film) | title | tt0110989 | Richie Rich | high | 321.500 |
| 2026-11-15 21:30:53 | enwiki | Kayamkulam Kochunni (2018 film) | title | tt7231546 | Kayamkulam Kochunni | high | 129.600 |
| 2026-11-15 21:31:08 | enwiki | Club Kid (film) | title | tt38058097 | Club Kid | high | 114.900 |
| 2026-11-15 21:31:25 | enwiki | Club Kid (film) | title | tt38058097 | Club Kid | high | 99.300 |
| 2026-11-15 21:35:01 | enwiki | Cheaper by the Dozen (2003 film) | title | tt0349205 | Cheaper by the Dozen | high | 2.700 |
| 2026-11-15 21:37:48 | enwiki | Son of the South (film) | title | tt2235372 | Son of the South | high | 3.600 |
| 2026-11-15 21:37:49 | enwiki | Vibe (film) | title | tt40856286 | Vibe | high | 3.300 |
| 2026-11-15 21:40:20 | enwiki | Malcolm X (1992 film) | title | tt0104797 | Malcolm X | high | 6.900 |
| 2026-11-15 21:40:24 | enwiki | Bad Education (2004 film) | title | tt0275491 | Bad Education | high | 3.900 |
| 2026-11-15 21:41:01 | enwiki | The Body (TV series) | title | tt1937149 | The Body | high | 5.200 |
| 2026-11-15 21:42:10 | enwiki | Digimon Adventure (film) | title | tt0210418 | Digimon Adventure | high | 3.400 |
| 2026-11-15 21:42:52 | enwiki | The Visit (TV series) | title | tt3567288 | The Visit | high | 2.800 |
| 2026-11-15 21:44:45 | enwiki | The Village (2004 film) | title | tt0368447 | The Village | high | 3.100 |
| 2026-11-15 21:46:07 | enwiki | Dust (2001 film) | title | tt0243232 | Dust | high | 2.400 |
| 2026-11-15 21:16:51 | enwiki | All Grown Up! | title | tt0387714 | All Grown Up! | medium | 889.100 |
| 2026-11-15 21:16:51 | enwiki | Robotech | title | tt0088595 | Robotech | medium | 889.200 |

_25 of 103 rows shown._

<!-- /RESULT -->

### Handling of late-arriving data

Our aggregations use **event time** (the timestamp of the Wikipedia edit
itself) rather than the time the event reaches Spark. We configured a
10-minute watermark on this timestamp: Spark keeps a window's state open
until the watermark (maximum event time seen minus 10 minutes) passes the
end of the window, then flushes the result once in append mode and clears
its state. Events arriving after the watermark are ignored by the windowed
aggregations and reported under `numRowsDroppedByWatermark` in the query
progress table. Duplicate deliveries are removed using
`dropDuplicatesWithinWatermark` on the event ID, relying on the same
watermark to keep state memory bounded.

To test this behaviour, the producer first replayed the last 15 minutes of
the Wikimedia stream (`--since-minutes 15`) before switching to live events,
so many messages arrived several minutes after the edit actually occurred.
Every matched event also stores its ingestion delay, which is summarised in
the run metrics table.

Setting the watermark duration is a trade-off between completeness and
latency/memory: a longer watermark accepts later events but holds more state
in memory and delays window publication. Choosing 10 minutes (twice the size
of a 5-minute window) absorbs normal delays in the Wikimedia stream while
still producing closed windows during our test session. Note that the
stateless sink of matched events keeps every match, late or not; only the
windowed counts are affected.

### Methodological explanation - streaming

The pipeline begins with a Python producer that consumes Wikimedia's
`recentchange` Server-Sent Events stream and publishes every article edit or
creation (namespace 0) from English and French Wikipedia to the Kafka topic
`wiki-changes`. Filtering on the producer side is kept intentionally broad
so that all IMDb-specific matching rules remain in Spark and can be updated
without restarting ingestion.

A batch job prepares an in-memory lookup table from IMDb containing films,
TV series, and mini-series with at least 1,000 votes, plus their credited
actors, actresses, directors, and writers. Names and titles are normalised
(lowercased, stripped of accents and punctuation, and cleaned of
disambiguators such as `"(2010 film)"`). When several entities share the
same normalised key, we keep the most-voted one and store the total number
of candidates.

The Structured Streaming job parses the Kafka payloads against an explicit
schema, applies the watermark and deduplication, normalises the page title,
and joins the stream with the static lookup table (broadcast stream-static
join). Using the Wikipedia disambiguator, we compute a confidence level: a
title like `"Inception (2010 film)"` matching a 2010 film is marked *high*,
a page without a disambiguator matching a unique key is *medium*, an
ambiguous key with multiple candidates is *low*, and a disambiguator that
contradicts the entity type (such as `"Mercury (planet)"`) is rejected.
Results are written to Parquet (matched events, 5-minute tumbling window
counts, and total event counts for the match rate) and to the console
(10-minute sliding windows), while a `StreamingQueryListener` logs every
micro-batch's progress metrics.

Matching on page titles is broad enough to capture plenty of events over a
15 to 30 minute run (English Wikipedia receives several edits per second,
and films and actors are among the most edited pages), while the
stream-static join stays fast and cheap because the lookup table fits in
memory.

We considered filtering on Wikipedia categories or Wikidata items for
higher precision, but those fields are not present in the `recentchange`
event payload, so resolving them would have required an API call per event.
Matching on titles does produce occasional false positives for common words
such as *"Love"* or *"Home"*, which is why the confidence column lets users
filter for high-confidence matches. Likewise, keeping only the most-voted
entity per key sacrifices rare homonyms in favour of matching the main
entity.

Because of our memory constraints, the lookup table used during the actual
test run included only titles (71,752 films, TV series, and mini-series
with at least 1,000 votes) and excluded people (`LOOKUP_PEOPLE=0`). Over
roughly 30 minutes (15 minutes replayed plus 15 minutes live), the producer
read 76,998 Wikimedia events and published 4,463 article edits from English
and French Wikipedia to Kafka. Spark matched 103 edits across 85 distinct
IMDb titles between 21:16 and 21:46 UTC. Because of the replayed backlog,
the mean ingestion delay was 248 seconds and the 95th percentile reached
828 seconds, with 21 matched events arriving more than 10 minutes late;
2,707 events fell into windows that had already closed by the time the job
was stopped.

One practical detail during setup was that Spark could not download the
Kafka connector automatically because Maven was unreachable from inside our
container. We worked around this by pre-downloading the jars in Python
(`get_kafka_jars.py`) and passing them explicitly via `spark.jars`.

## 5. Open question

### 5.1 Question, motivation and data

In this final part, we look at whether recurring director-actor
partnerships (defined as the same actor cast by the same director in at
least three feature films) lead to higher-rated films than the director's
other movies, and whether that effect fades as the partnership repeats.

Famous duos like Scorsese and De Niro or Burton and Depp suggest that
working repeatedly with a trusted actor helps. At the same time, directors
often re-cast an actor *because* their first film together was successful,
and repeating the collaboration might eventually yield diminishing returns.
Using the full IMDb dataset allows us to test this across thousands of
directors rather than relying on a few well-known anecdotes.

For this analysis, we used IMDb data only (`title.crew` for directors,
`title.principals` for cast, `title.basics` for feature films and release
years, `title.ratings`, and `name.basics`), without any external datasets.

### 5.2 Results

*Execution status: both scripts (`build_collaborations.py` and
`analyze_collaborations.py`) are implemented and were validated on a
synthetic sample, but could not be executed on the full dataset before the
deadline. Generating all director-actor pairs requires joining
`title.principals` (~95 million rows), which exceeded the 3.7 GB of memory
available to Docker on our laptop.*

<!-- RESULT:open_question/director_paired_comparison -->

_Result not generated yet._

<!-- /RESULT -->

<!-- RESULT:open_question/lift_by_collaboration_index -->

_Result not generated yet._

<!-- /RESULT -->

<!-- RESULT:open_question/lift_by_start_decade -->

_Result not generated yet._

<!-- /RESULT -->

<!-- RESULT:open_question/top_partnerships -->

_Result not generated yet._

<!-- /RESULT -->

### 5.3 Key takeaways

Since the full-dataset execution did not complete before the deadline, no
empirical takeaways can be reported from the empty tables above. Had the job
finished, these tables would have shown the direction and magnitude of the
mean rating difference between a director's partnership films and their
other films (along with its t-statistic), the percentage of directors who
score higher with recurring partners, and whether the rating lift drops off
between the 1st and 5th+ collaboration.

### 5.4 Methodology

All `(director, actor)` pairs are built by joining the directors of each
feature film (`>= 100` votes) with its cast, excluding cases where a
director casts themselves. A window function then counts the films of each
pair and numbers them in chronological order (the collaboration index). Any
pair with at least three films together is defined as a partnership.

The main comparison is **paired by director**: for every director who has at
least one partnership film and at least two other films, we compare the
mean rating of their partnership films against the mean rating of their
other films, then average those differences across all qualifying
directors. This controls for the baseline talent of the director, which a
global comparison would not (prolific, acclaimed directors make more films
and therefore form more partnerships). A t-statistic (mean difference
divided by standard error) measures statistical significance.

To study the effect of repetition, each partnership film is compared against
the director's own mean rating ("lift") and grouped both by collaboration
index (1st, 2nd, 3rd, 4th, and 5th+ film together) and by the decade in
which the partnership started.

A regression model with both director and actor fixed effects would control
for additional confounders, but it would be heavier to run in Spark and
harder to interpret. Setting the partnership threshold at three films is
also somewhat arbitrary: lowering it to two films would capture many more
duos at the cost of introducing more noise. Finally, because this study is
purely observational, it can only highlight statistical associations rather
than a causal effect.