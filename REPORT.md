# Hands-on L9: Report

**Name:**
**Student ID:**
**Email:**

---

## Seed and commands

Seed used for `datagen.py`: 801240291

The commands you ran, in order. If you deviated from the steps in the README, say where and
why.

```bash

```

---

## Results

For each task, your output and one or two sentences on what it says about your data.

### Task 1: hashtag trends (DataFrame API)

```
The output shows the most frequent hashtags. Lower and Trim combined hashtags with different capitilizations and extra spaces.

```

How many distinct spellings did your most frequent hashtag have before `lower` and `trim`?

### Task 2: engagement by age group (Spark SQL)

```
The output shows the average engagment by different age groups.


```

### Task 3: sentiment versus engagement (Spark SQL)

```
Positive sentiment tends to have higher engagement, followed by neutral, and lastly negative. Does it pay? Given that positive and neutral have higher engagement than negative, I say yes. Positive and neutral provide the most engagement, with positive averaging 75.88 likes and 19.11 retweets, and neutral averaging 48.47 likes and 11.96 retweets.
```

Does sentiment pay on your data? Say it with the numbers.

### Task 4: top verified users (DataFrame API)

```
The output shows the top five verified users ranked by their total engagement. @pixel_pusher1 had the highest reach with 24,193 interactions, followed by @designer39 with 22,811 interactions.```

---

## Two languages, one plan

Paste the two `explain()` outputs of task 2, the SQL version and the DataFrame version:

```
=== task2: physical plan of the SQL version ===
26/10/23 21:19:57 INFO FileSourceStrategy: Pushed Filters: IsNotNull(user_id)
26/10/23 21:19:57 INFO FileSourceStrategy: Post-Scan Filters: Set(isnotnull(user_id#1))
26/10/23 21:19:57 INFO FileSourceStrategy: Pushed Filters: IsNotNull(user_id)
26/10/23 21:19:57 INFO FileSourceStrategy: Post-Scan Filters: Set(isnotnull(user_id#8))
== Physical Plan ==
AdaptiveSparkPlan isFinalPlan=false
+- Sort [avg_likes#61 DESC NULLS LAST], true, 0
   +- Exchange rangepartitioning(avg_likes#61 DESC NULLS LAST, 200), ENSURE_REQUIREMENTS, [plan_id=697]
      +- HashAggregate(keys=[age_group#10], functions=[count(1), avg(likes#4), avg(retweets#5)])
         +- Exchange hashpartitioning(age_group#10, 200), ENSURE_REQUIREMENTS, [plan_id=694]
            +- HashAggregate(keys=[age_group#10], functions=[partial_count(1), partial_avg(likes#4), partial_avg(retweets#5)])
               +- Project [likes#4, retweets#5, age_group#10]
                  +- BroadcastHashJoin [user_id#1], [user_id#8], Inner, BuildRight, false, false
                     :- Filter isnotnull(user_id#1)
                     :  +- FileScan csv [user_id#1,likes#4,retweets#5] Batched: false, DataFilters: [isnotnull(user_id#1)], Format: CSV, Location: InMemoryFileIndex(1 paths)[file:/opt/spark/work-dir/shared/input/posts.csv], PartitionFilters: [], PushedFilters: [IsNotNull(user_id)], ReadSchema: struct<user_id:string,likes:int,retweets:int>
                     +- BroadcastExchange HashedRelationBroadcastMode(List(input[0, string, false]),false), [plan_id=689]
                        +- Filter isnotnull(user_id#8)
                           +- FileScan csv [user_id#8,age_group#10] Batched: false, DataFilters: [isnotnull(user_id#8)], Format: CSV, Location: InMemoryFileIndex(1 paths)[file:/opt/spark/work-dir/shared/input/users.csv], PartitionFilters: [], PushedFilters: [IsNotNull(user_id)], ReadSchema: struct<user_id:string,age_group:string>






=== task2: physical plan of the DataFrame version ===
26/10/23 21:19:57 INFO FileSourceStrategy: Pushed Filters: IsNotNull(user_id)
26/10/23 21:19:57 INFO FileSourceStrategy: Post-Scan Filters: Set(isnotnull(user_id#1))
26/10/23 21:19:57 INFO FileSourceStrategy: Pushed Filters: IsNotNull(user_id)
26/10/23 21:19:57 INFO FileSourceStrategy: Post-Scan Filters: Set(isnotnull(user_id#8))
== Physical Plan ==
AdaptiveSparkPlan isFinalPlan=false
+- Sort [avg_likes#141 DESC NULLS LAST], true, 0
   +- Exchange rangepartitioning(avg_likes#141 DESC NULLS LAST, 200), ENSURE_REQUIREMENTS, [plan_id=747]
      +- HashAggregate(keys=[age_group#10], functions=[count(1), avg(likes#4), avg(retweets#5)])
         +- Exchange hashpartitioning(age_group#10, 200), ENSURE_REQUIREMENTS, [plan_id=744]
            +- HashAggregate(keys=[age_group#10], functions=[partial_count(1), partial_avg(likes#4), partial_avg(retweets#5)])
               +- Project [likes#4, retweets#5, age_group#10]
                  +- BroadcastHashJoin [user_id#1], [user_id#8], Inner, BuildRight, false, false
                     :- Filter isnotnull(user_id#1)
                     :  +- FileScan csv [user_id#1,likes#4,retweets#5] Batched: false, DataFilters: [isnotnull(user_id#1)], Format: CSV, Location: InMemoryFileIndex(1 paths)[file:/opt/spark/work-dir/shared/input/posts.csv], PartitionFilters: [], PushedFilters: [IsNotNull(user_id)], ReadSchema: struct<user_id:string,likes:int,retweets:int>
                     +- BroadcastExchange HashedRelationBroadcastMode(List(input[0, string, false]),false), [plan_id=739]
                        +- Filter isnotnull(user_id#8)
                           +- FileScan csv [user_id#8,age_group#10] Batched: false, DataFilters: [isnotnull(user_id#8)], Format: CSV, Location: InMemoryFileIndex(1 paths)[file:/opt/spark/work-dir/shared/input/users.csv], PartitionFilters: [], PushedFilters: [IsNotNull(user_id)], ReadSchema: struct<user_id:string,age_group:string>


```

Your comparison: are the two plans the same? Which operator is the join and which kind of
join did Spark choose, where are the `Exchange` operators and why are they needed, and what
does this tell you about choosing between SQL and the DataFrame API?

Both plans are basically the same. They both use BroadcastHashJoin and Exchange.

---

## Views

What did `createOrReplaceTempView("posts")` do, and what did it not do? Was any data copied?
What happens to the views when the program ends, and how would you keep a table for the
next session?

createOrReplaceTempView("posts") created temporary SQL view of the dataframe without copying the data. Bfore the session ends, saveAsTable() is used to keep the table. 

---

## Problems and fixes

Anything that went wrong and what resolved it. Paste the actual error message. If nothing
went wrong, say so.

everything went smooth.
