# Movie Rating Analysis

This MapReduce (via Apache Pig) analysis calculates the **average rating** for each movie in a dataset of user ratings. By understanding how users perceive individual movies, content creators and streaming services can make more informed decisions about **content quality**, **user preferences**, and **acquisition trends**.

---

## Objective

- **Goal:** Compute the **average rating** for each movie from a user ratings dataset.
- **Benefit:** Provides foundational insights into user sentiment and helps stakeholders prioritize high-quality content.

---

## Use Case: Content Acquisition and Licensing

- **Problem:** A streaming service must decide which movies to acquire or license.
- **Solution:** Leverage **average ratings** to prioritize high-quality movies.
  - Avoid investing in titles with **consistently low ratings**.

---

## Pig Script

```pig
-- Load the ratings data from the CSV file
ratings = LOAD '/final_project/ratings.csv' 
           USING PigStorage(',')  
           AS (userId:int, movieId:int, rating:float, timestamp:chararray);

-- Group the ratings by movieId
grouped_ratings = GROUP ratings BY movieId;

-- Calculate the average rating for each movie
avg_ratings = FOREACH grouped_ratings GENERATE 
              group AS movieId, 
              AVG(ratings.rating) AS avgRating;

-- Sort the results by average rating in descending order (optional)
sorted_ratings = ORDER avg_ratings BY avgRating DESC;

-- Store the results in an output directory
STORE sorted_ratings INTO '/user/hdoop/avg_movie_ratings' 
USING PigStorage(',');
```