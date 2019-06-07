# Insight_Data_Engineering_Project

## ``Reddit Insights``

Reddit is a community-driven social platform with lots of interesting posts shared every day across a number of 
subreddits on a variety of topics. This is great for Reddit community and users but this also leads to some problems for the users. The users have to spend a significant amount of time scrolling through all the posts in order to discover some interesting posts of their choice/preference in a subreddit they follow. Although it is time-consuming to surf through the posts but at the same time, users would not want to miss knowing the topics of their choice. So it is important that user spend less time discovering the content but still remain informed about the latest trend in their favorite subreddit. Therefore I have come up with a new feature - **`Reddit-Tags`**, that would help to enhance `customer experience` and improve `user engagement` by showing popular tags on a subreddit. The inspiration for **`Reddit-Tags`** come from hashtags used in other social media platforms like Instagram and Twitter. Hashtags allow users to explore new contents corresponding to each tags thereby increasing user engagement and improving user experience on the platform.


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



