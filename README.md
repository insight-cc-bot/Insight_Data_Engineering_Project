# Insight_Data_Engineering_Project

## Reddit Insights

Reddit is a community-driven social platform, there are a lot of interesting posts shared every day across a number of 
subreddits. This is great for users but the problem is that users have to spend a significant amount of time scrolling 
through all the posts in order to discover new and interesting posts in a subreddit that can be valuable to him/her. 
It is very time-consuming to go through all of the posts but at the same time, you would not want to miss knowing about 
those interesting topics. As new events occur every day, there are always new and interesting topics to explore. It is 
important for a user to spend less time discovering the content but still remain informed about the latest trend in a 
subreddit. This is something we often see on Twitter or Instagram where hashtags allow a user to explore content on 
related topics. Even YouTube has started generating tags for each video that would allow users to explore similar 
content. Therefore in order to enhance customer experience and user engagement in Reddit, I want to come up with this 
new feature to help users stay more informed and active with Reddit.


### `` Project Idea:`` 

1. Generate tags(keywords) for each SubReddit that help user to see top topics
2. Index all the posts corresponding to the generated tags   
3. Identify subreddit seasonality using comments aggregation.
4. Identify popular vs active SubReddits based on user engagement to decide best subreddit to put ads. 


### `` Extended Project Idea:``

5. Identify the most active SubReddits (Real Time Processing)
6. Top keywords being used in last one hour



### `` Business Case:`` 

1. Improve Customer Experience by generating tags for each subreddit and displaying corresponding list of posts
2. Displaying ads on relevant reddit channel
3. Seasonality insight can be used by Analyst and Marketing team in managing their social media/Ad campaigns


### ``Dataset:`` 

1. Link: https://files.pushshift.io/reddit/
2. Size: 900 GB
3. Features :  Subreddit_id , subreddit name , Author,  created timestamp,    submission, comments, replies, upvotes, downvotes


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



