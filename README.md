# #tagReddit

## `Motivation:`

Reddit is a community-driven social platform with lots of interesting posts shared every day across a number of 
subreddits on a variety of topics. This is great for Reddit community but this also leads to some problems for the users. The users have to spend a significant amount of time going over all the posts in order to discover interesting posts of their choice/preference in a subreddit they follow. While exploring a new Subreddit, users would want to spend less time to get an overview of the subreddit community they are exploring. In this project I come up with - **`Reddit-Tags`**, that would help to enhance `user experience` and improve `user engagement` by showing popular tags. The inspiration for **`Reddit-Tags`** come from hashtags used in other social media platforms like Instagram and Twitter. Hashtags allow users to explore new contents corresponding to each tags thereby increasing user engagement and improving user experience on the platform.

In addition to improving the user experience, the tags can also be helpful for `Ads team` to show ads based on the keywords clicked by users. 

### `Objective:` 
1. Generate popular tags(keywords) for each SubReddit for each month and year.
2. Index all the posts corresponding to each generated tag.
3. Allow user to see popular tags for a subreddit over the time. 


### ` Future Ideas:`
4. Identify the most active SubReddits (Real Time Processing)
5. Index posts in real time
6. Top keywords being used in last one hour


### ` Business Case:` 
1. Improve Customer Experience by generating tags for each subreddit and displaying corresponding list of posts
2. Displaying ads on relevant reddit channel
3. Seasonality insight can be used by Analyst and Marketing team in managing their social media/Ad campaigns

### `Dataset:` 
1. Data taken from Pushshift.io.

2. Size: 500 GB (taken from 2008 - 2015 for comments)
Schema - <img>
3. Data Stored in AWS S3 data lake in three separate buckets - 
> `Raw Comments`: Data taken from source, `Cleaned Comments`: Data generated after initial preprocessing, `Frequent Words`: Tags identified from the posts

### `Data Pipeline`
<img src="./images/pipeline.png" width="800">
### `Cluster Setup`

### ``Engineering Challenge:`` 

1. Indexing posts for generated tags to reduce search complexity of  O(M*N) to O(1) 
2. Text data preprocessing and cleaning which include - removing stopword, removing punctuations, removing urls etc. 
    Complexity:  O(M*(N^2))
    O(N^2) to compare two text bodies (comments vs stopwords)
    O(M) for all posts from reddit
    M= documents
    N= number of records 
3.  Implement incremental aggregations and avoid re-computation.

### `` Tech Stack:`` 

1. S3
2. Spark
3. Redshift
4. Elasticsearch *
5. Kafka
6. Spark streaming/Flink


### `` Presentation Slides:`` 
https://docs.google.com/presentation/d/1GJnKdTFyCLTXobDSQXum8JLYhgn6IeC2ZAp_jjl0nEU/edit#slide=id.g5b1cafed5b_0_1416



