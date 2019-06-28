import pandas as pd

columns=["word", "subreddit_id", "subreddit","year", "month", "count", "rank"]
df= pd.read_csv("/Users/smiley/Documents/Insight/part-00007-3212c4eb-7539-42f5-8a5b-0dfacbd5d28e-c000.csv" ,names=columns)
#print(df[(df['subreddit']=="l33t") & (df['count']==2)])

#temp_df=df[df.subreddit=="l33t"]
#temp_df1=temp_df[temp_df["year"]==2008]
#temp_df2=temp_df1[temp_df1['count']==2]
#print(temp_df2)

print(df[(df["subreddit"]=="l33t") & (df["year"]==2008) & (df["month"]==1)])


