# Insight_Data_Engineering_Project

# Reddit Insights

As a Reddit user, I have often spent most of my time scrolling, reading and searching a lot of posts in a subreddit and 
then finding out the post which is interesting and valuable to me. There is a lot of interesting posts shared by 
community members but it is time-consuming to go through all of them but at the same time, you would not want to miss 
knowing about those interesting topics. As with changing time and evolving trends in the social media industry. It is 
important for a user to spend less time and still remain informed about the latest trend in a particular field. We need
to enhance user experience. Hence in order to enhance customer experience and increase user engagement in Reddit. I have
come up with a new feature for Reddit users to stay more informed and active with Reddit.


`` Project Idea:`` 

1. Generate tags(keywords) for each SubReddit that help user to see top topics
2. Index all the posts corresponding to the generated tags   
3. Identify subreddit seasonality using comments aggregation.
4. Identify popular vs active SubReddits based on user engagement to decide best subreddit to put ads 


`` Extended Project Idea:``

5. Identify the most active SubReddits (Real Time Processing)
6. Top keywords being used in last one hour



`` Business Case:`` 

1. Improve Customer Experience by generating tags for each subreddit and displaying corresponding list of posts
2. Displaying ads on relevant reddit channel
3. Seasonality insight can be used by Analyst and Marketing team in managing their social media/Ad campaigns


``Dataset:`` 

1. Link: https://files.pushshift.io/reddit/
2. Size: 900 GB
3. Features :  Subreddit_id , subreddit name , Author,  created timestamp,    submission, comments, replies, upvotes, downvotes


``Engineering Challenge:`` 

1. Indexing posts for generated tags to reduce search complexity of  O(M*N)
2. Text data preprocessing and cleaning which include - removing stopword, removing punctuations, removing urls etc. 
   Complexity:  O(M*(N^2))
   O(N^2) to compare two text bodies (comments vs stopwords)
   O(M) for all posts from reddit
   M= documents
   N= number of records


`` Tech Stack:`` 

1. S3
2. Spark
3. Redshift
4. Elasticsearch *
5. Kafka
6. Spark streaming/Flink


`` Presentation Slides:`` 
https://docs.google.com/presentation/d/1GJnKdTFyCLTXobDSQXum8JLYhgn6IeC2ZAp_jjl0nEU/edit#slide=id.g5b1cafed5b_0_1416



