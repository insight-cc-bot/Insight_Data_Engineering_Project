

import praw
reddit_obj = praw.Reddit(client_id="MwBq5YRdgQZbbQ", client_secret="zoBtH3BMCINonQbseGvslik6j7A",
                     user_agent="reddit_tags")
reddit_data = []
text_field="Pets"
for submission in reddit_obj.subreddit(text_field).top(limit=10):
    reddit_data.append(submission.title)


print(reddit_data)