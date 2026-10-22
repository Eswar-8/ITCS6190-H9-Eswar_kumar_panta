# Hands-on L9: Report

**Name:**
**Student ID:**
**Email:**

---

## Seed and commands

Seed used for `datagen.py`: python3 datagen.py 801505751

The commands you ran, in order. If you deviated from the steps in the README, say where and
why.

```bash
python3 datagen.py 801505751
docker compose -f docker-compose.codespaces.yml up -d
docker cp main.py spark-master:/opt/spark/work-dir/
docker exec -it spark-master /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  /opt/spark/work-dir/main.py \
  /opt/spark/work-dir/shared/input \
  /opt/spark/work-dir/shared/output

```

---

## Results

For each task, your output and one or two sentences on what it says about your data.

### Task 1: hashtag trends (DataFrame API)

```
hashtag,count
"#ai",1284
"#love",1252
"#bug",1248
"#ux",1235
"#fail",1222
"#cleanui",1193
"#darkmode",798
"#social",515
"#mood",502
"#design",329
```

How many distinct spellings did your most frequent hashtag have before `lower` and `trim`?

``Before normalization, the most frequent hashtag appeared in 3 distinct case variations (e.g., #AI, #Ai, and #ai), which were successfully merged into a single count using lower() and trim()``

### Task 2: engagement by age group (Spark SQL)

```
age_group,posts,avg_likes,avg_retweets
Teen,1329,56.85,14.36
Senior,2139,53.78,13.43
Adult,1532,51.14,13.35
```
The engagement metrics show that Teen users generated the highest average engagement per post (56.85 likes), closely followed by Seniors and Adults, indicating that younger demographic groups drive higher interaction rates relative to their post volumes.

### Task 3: sentiment versus engagement (Spark SQL)

```
sentiment,posts,avg_likes,avg_retweets
Negative,1040,25.99,6.66
Neutral,2285,49.31,12.68
Positive,1675,77.17,19.32
```

Does sentiment pay on your data? Say it with the numbers.

``Yes, positive sentiment strongly drives higher engagement. Positive posts averaged 77.17 likes and 19.32 retweets, which is significantly higher than neutral posts (49.31 likes, 12.68 retweets) and negative posts (25.99 likes, 6.66 retweets).``

### Task 4: top verified users (DataFrame API)

```
username,total_likes,total_retweets,reach
@calm_mind34,18746,4668,23414
@pixel_pusher43,17967,4688,22655
@pixel_pusher29,17330,4656,21986
@coffee_first5,17489,4424,21913
@social_queen31,10296,2238,12534
```
The top verified users achieved massive reach scores, dominated by accounts like @calm_mind34 and @pixel_pusher43, where likes make up the overwhelming majority of their overall engagement footprint compared to retweets.
---

## Two languages, one plan

Paste the two `explain()` outputs of task 2, the SQL version and the DataFrame version:

```
=== task2: physical plan of the SQL version ===
== Physical Plan ==
AdaptiveSparkPlan isFinalPlan=false
+- Sort [avg_likes#35 DESC NULLS LAST], true, 0
   +- Exchange rangepartitioning(avg_likes#35 DESC NULLS LAST, 200), ENSURE_REQUIREMENTS, [plan_id=533]
      +- HashAggregate(keys=[age_group#10], functions=[count(1), avg(likes#4), avg(retweets#5)])
         +- Exchange hashpartitioning(age_group#10, 200), ENSURE_REQUIREMENTS, [plan_id=530]
            +- HashAggregate(keys=[age_group#10], functions=[partial_count(1), partial_avg(likes#4), partial_avg(retweets#5)])
               +- Project [likes#4, retweets#5, age_group#10]
                  +- BroadcastHashJoin [user_id#1], [user_id#8], Inner, BuildRight, false, false
                     :- Filter isnotnull(user_id#1)
                     :  +- FileScan csv [user_id#1,likes#4,retweets#5] Batched: false, DataFilters: [isnotnull(user_id#1)], Format: CSV, Location: InMemoryFileIndex(1 paths)[file:/opt/spark/work-dir/shared/input/posts.csv], PartitionFilters: [], PushedFilters: [IsNotNull(user_id)], ReadSchema: struct<user_id:string,likes:int,retweets:int>
                     +- BroadcastExchange HashedRelationBroadcastMode(List(input[0, string, false]),false), [plan_id=525]
                        +- Filter isnotnull(user_id#8)
                           +- FileScan csv [user_id#8,age_group#10] Batched: false, DataFilters: [isnotnull(user_id#8)], Format: CSV, Location: InMemoryFileIndex(1 paths)[file:/opt/spark/work-dir/shared/input/users.csv], PartitionFilters: [], PushedFilters: [IsNotNull(user_id)], ReadSchema: struct<user_id:string,age_group:string>

=== task2: physical plan of the DataFrame version ===
== Physical Plan ==
AdaptiveSparkPlan isFinalPlan=false
+- Sort [avg_likes#116 DESC NULLS LAST], true, 0
   +- Exchange rangepartitioning(avg_likes#116 DESC NULLS LAST, 200), ENSURE_REQUIREMENTS, [plan_id=583]
      +- HashAggregate(keys=[age_group#10], functions=[count(1), avg(likes#4), avg(retweets#5)])
         +- Exchange hashpartitioning(age_group#10, 200), ENSURE_REQUIREMENTS, [plan_id=580]
            +- HashAggregate(keys=[age_group#10], functions=[partial_count(1), partial_avg(likes#4), partial_avg(retweets#5)])
               +- Project [likes#4, retweets#5, age_group#10]
                  +- BroadcastHashJoin [user_id#1], [user_id#8], Inner, BuildRight, false, false
                     :- Filter isnotnull(user_id#1)
                     :  +- FileScan csv [user_id#1,likes#4,retweets#5] Batched: false, DataFilters: [isnotnull(user_id#1)], Format: CSV, Location: InMemoryFileIndex(1 paths)[file:/opt/spark/work-dir/shared/input/posts.csv], PartitionFilters: [], PushedFilters: [IsNotNull(user_id)], ReadSchema: struct<user_id:string,likes:int,retweets:int>
                     +- BroadcastExchange HashedRelationBroadcastMode(List(input[0, string, false]),false), [plan_id=575]
                        +- Filter isnotnull(user_id#8)
                           +- FileScan csv [user_id#8,age_group#10] Batched: false, DataFilters: [isnotnull(user_id#8)], Format: CSV, Location: InMemoryFileIndex(1 paths)[file:/opt/spark/work-dir/shared/input/users.csv], PartitionFilters: [], PushedFilters: [IsNotNull(user_id)], ReadSchema: struct<user_id:string,age_group:string>
```

Your comparison: are the two plans the same? Which operator is the join and which kind of
join did Spark choose, where are the `Exchange` operators and why are they needed, and what
does this tell you about choosing between SQL and the DataFrame API?



---

## Views

What did `createOrReplaceTempView("posts")` do, and what did it not do? Was any data copied?
What happens to the views when the program ends, and how would you keep a table for the
next session?

```
- What createOrReplaceTempView("posts") did: It registered the DataFrame's logical plan in Spark's internal catalog under the string name "posts", making it accessible for SQL queries.
- What it did not do / Data copying: No data was copied, moved, or cached into memory or disk. A temporary view is simply a pointer pointing to the lazy evaluation plan of the DataFrame.
- Lifecycle and Persistence: Temporary views are automatically dropped when the Spark session terminates (spark.stop()). To keep a table for future sessions, you would persist it to a permanent metastore using df.write.saveAsTable("table_name") or write it out to disk in formats like Parquet or Delta.
```
---

## Problems and fixes

Anything that went wrong and what resolved it. Paste the actual error message. If nothing
went wrong, say so.

No errors encountered. The code executed successfully on the Spark cluster.

