# Final report outline

Owner of each section in brackets. Target ~12–15 pages + appendix.

1. **Introduction** [C] — problem statement, the four objectives from the brief.
2. **Related background** [C] — GHCN-Daily, climate normals/anomalies, ETCCDI indices, why Spark.
3. **System architecture** [A] — cluster diagram (1 master, 2 workers), HDFS roles (NameNode/DataNodes,
   replication 2), Spark standalone (master, workers, driver, executors), hardware table,
   screenshots of the HDFS / Spark / History UIs.
4. **Data & ingestion** [B] — sources, file formats, units, station metadata, terms of use;
   size of the data (files, GB, rows); why we convert csv.gz → Parquet partitioned by year.
5. **Cleaning & harmonisation** [B] — every decision from `clean.py` docstring with numbers from
   `quality_record_status`, `quality_qflag_breakdown`, `quality_coverage_by_year`;
   completeness rules from `aggregate.py`.
6. **Distributed processing & performance** [A] — pipeline DAG (stages, shuffles), runtime per stage
   (`logs/run_*.json`), 1 vs 2 workers speed-up, partition tuning, fault-tolerance test.
7. **Results** [C]
   - 7.1 Climate summaries — seasonal cycle, annual means, anomalies
   - 7.2 Extremes — hottest/coldest/wettest, hot days, CDD, country records
   - 7.3 Seasonal trends — per season and per region
   - 7.4 Spatial aggregates — grid map, hemispheres, latitude bands, focus countries (Libya & neighbours)
8. **Descriptive analysis vs causal attribution** [C] — required by objective 4:
   - What our numbers *are*: observed averages, differences from a baseline, linear trends.
   - What they are *not*: evidence of cause. Confounders — urbanisation (urban heat island), station
     moves, instrument changes, changing station network, natural variability (ENSO, NAO).
     GHCN-Daily is quality-controlled but not homogenised.
   - Short period (~10 years) → trends are uncertain; give r² / number of years.
   - What attribution would need: climate-model counterfactuals, detection & attribution methods.
   - Wording rule used in the report: "warmer than the baseline", "a positive trend of X °C/decade
     was observed" — never "caused by".
9. **Limitations & future work** [all] — uneven station density, sparse African data, no homogenisation,
   longer period (1991–2020 normals), YARN/cloud deployment, streaming updates.
10. **Conclusion** [C]

**Appendix** — how to reproduce (link SETUP_GUIDE), config files, team contribution table.
