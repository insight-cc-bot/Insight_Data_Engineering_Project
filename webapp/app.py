from flask import render_template
from flask import Flask, jsonify, request, make_response, url_for
import psycopg2
import praw
import logging
from elasticsearch import Elasticsearch
from datetime import datetime
import timeit
import time
import os
from config import (user,
                    password,
                    host,
                    port,
                    database,
                    client_id,
                    client_secret,
                    user_agent)

now = datetime.now()
# ===== Logging ========
TS = time.strftime("%Y-%m-%d:%H-%M-%S")
log_dir = "./logs/{0}_{1}_{2}".format(now.year, now.month, now.day)

if not os.path.exists(log_dir):
    os.mkdir(log_dir)

logger = logging.basicConfig(level=logging.DEBUG,
                             format='%(asctime)s %(name)-12s %(levelname)-8s %(message)s',
                             datefmt='%m-%d %H:%M',
                             filename=log_dir + "reddit_insights" + str(TS) + ".log",
                             filemode='w')
logging.basicConfig(level=logging.DEBUG)

# Flask App
app = Flask(__name__, static_url_path='/Users/dm/Desktop/insight_html/static')

# ELASTIC SEARCH Client
# es = Elasticsearch([{'host': '54.212.124.15', 'port': 9200}])
es1 = Elasticsearch([{'host': '10.0.0.6', 'port': 9200}])
es2 = Elasticsearch([{'host': '10.0.0.8', 'port': 9200}])

# REDDIT CLIENT
reddit = praw.Reddit(client_id=client_id, client_secret=client_secret,
                     user_agent=user_agent)

# default list of 15 recent posts from Reddit -
reddit_posts = [submission.title for submission in reddit.subreddit('all').hot(limit=30)]

subreddit_state = {
    "subreddit_name": "Reddit",
    "year": 2019,
    "month": None,
    "tags": None,
    "description": None
}


def handle_postgres(subreddit_name, year, month):
    """
    Fetch data from Postgres
    :param text_field:
    :param year_field:
    :param month_field:
    :return:
    """
    try:
        # set connection
        connection = psycopg2.connect(user=user,
                                      password=password,
                                      host=host,
                                      port=port,
                                      database=database)
        # set cursor
        cursor = connection.cursor()
        # query
        postgreSQL_select_Query = "select word from word_count where subreddit=%s and year=%s and month=%s order by count desc,rank asc;"

        input = (subreddit_name, year, month)

        start_time = timeit.default_timer()
        cursor.execute(postgreSQL_select_Query, input)

        # execute query
        word_list = cursor.fetchmany(10)
        end_time = timeit.default_timer()
        query_latency = end_time - start_time
        print(query_latency)
        logging.info("PostgreSQL query time - {0}".format(query_latency))

        result_list = [word[0] for word in word_list]

        # close connection
        connection.commit()

    finally:
        # closing database connection.
        if (connection):
            cursor.close()
            connection.close()
            logging.info("PostgreSQL connection is closed")

    return result_list


def handle_elasticsearch(subreddit_name, input_year, input_month, topic):
    year_list = [2008, 2009, 2010]
    if input_year in year_list:
        es = es1
    else:
        es = es2
    logging.info("ElasticSearch QUERY- {0}, {1}, {2}, {3}".format(subreddit_name, input_year, input_month, topic))
    start_time = timeit.default_timer()
    res = es.search(index="comments_{0}".format(input_year),
                    body={"from": 0,
                          "size": 10,
                          "query":
                              {
                                  "bool":
                                      {
                                          "should": [{"match": {"subreddit": subreddit_name}},
                                                     {"match": {"year": input_year}},
                                                     {"match": {"month": input_month}},
                                                     {"match": {"body": topic}}]
                                      }
                              }
                          }
                    )
    end_time = timeit.default_timer()
    query_latency = end_time - start_time
    print(query_latency)
    logging.info("ElasticSearch query time - {0}".format(query_latency))
    No_of_hits = res['hits']['total']['value']
    results_links = list()
    for hit in res['hits']['hits']:
        results_links.append(hit['_source']["body"])

    return results_links


# -----------------
# Error Handling
# -----------------
@app.errorhandler(400)
def not_found(error):
    return make_response(jsonify({'error': 'Bad Request'}), 400)


@app.errorhandler(404)
def not_found(error):
    return make_response(jsonify({'error': 'No Such Data Found}'}), 404)


@app.errorhandler(408)
def not_found(error):
    return make_response(jsonify({'error': 'Request Timeout'}), 408)


@app.errorhandler(412)
def not_found(error):
    return make_response(jsonify({'error': 'Please select values from all the drop downs'}), 412)


# -----------------------
# Stage 2: Get Posts
# -----------------------
@app.route('/redditinsight/get_posts', methods=['POST'])
def get_filtered_posts():
    """
    Generate list of Posts from Elastic Search
    Input: Year, Month, Subreddit Name, Topic Name
    :return:
    """
    if request.method == 'POST':
        topic = request.form['topic']
        # get results from Elastic

        if not subreddit_state["subreddit_name"] or not subreddit_state["year"] or not subreddit_state["month"]:
            return render_template("index.html", subreddit_name="Oops!! Something is missing.")
        try:
            filtered_posts = handle_elasticsearch(subreddit_state["subreddit_name"][0], subreddit_state["year"][0],
                                                  subreddit_state["month"], topic)

            return render_template("index.html",
                                   subreddit_name=subreddit_state["subreddit_name"][0],
                                   subreddit_year=subreddit_state["year"][0],
                                   subreddit_month=subreddit_state["month"],
                                   results=subreddit_state["tags"],
                                   results_links=filtered_posts)
        except:
            return render_template("index.html",
                                   subreddit_name=subreddit_state["subreddit_name"][0],
                                   subreddit_year=subreddit_state["year"][0],
                                   subreddit_month=subreddit_state["month"],
                                   results=subreddit_state["tags"],
                                   results_links=["SORRY!! Posts missing. Calling SNOOOO for resque"])


# -----------------------
# Stage 1: Get Tags
# -----------------------
@app.route('/redditinsight/get_tags', methods=['POST'])
def process():
    if request.method == 'POST':
        print(request.form['taskoption1'])
        # get user inputs
        subreddit_name = request.form['taskoption1'].strip().lower()
        inp_year = request.form['taskoption2'].strip()
        inp_month = request.form['taskoption3'].strip()
        if not subreddit_name or not inp_year or not inp_month:
            return render_template("index.html",
                                   subreddit_name="Oops!! Please make sure you have selected the items from drop-down.")

        try:
            # initialize the submission
            subreddit_state["subreddit_name"] = subreddit_name,
            subreddit_state["year"] = int(inp_year),
            subreddit_state["month"] = int(inp_month)
            subreddit_state["description"] = reddit.subreddit(subreddit_name).public_description
        except:
            pass
        # defaults
        reddit_posts = []
        word_list = []
        try:
            # fetch data from database
            word_list = handle_postgres(subreddit_name, inp_year, inp_month)
            if len(word_list) == 0:
                word_list.append("There_are_no_tags_in_this_Subreddit")

            subreddit_state["tags"] = word_list
            for submission in reddit.subreddit(subreddit_name).top(limit=10):
                reddit_posts.append(submission.title)
            return render_template("index.html", subreddit_name=subreddit_state["subreddit_name"][0],
                                   subreddit_year=subreddit_state["year"][0],
                                   subreddit_month=subreddit_state["month"],
                                   results=word_list, results_links=reddit_posts,
                                   description=subreddit_state["description"])
        except:
            return render_template("index.html", subreddit_name=subreddit_state["subreddit_name"][0], results=word_list,
                                   description=subreddit_state["description"],
                                   results_links=[
                                       "SORRY!! Tagss missing, calling SNOOOO for rescue. Please make sure you have selected the items from drop-down."])


# ---- Home -----
# ----------------
@app.route('/')
def index():
    print("loaded template")
    return render_template("index.html", subreddit_name="Top Posts", subreddit_year=datetime.now().year,
                           subreddit_month=datetime.now().month, results_links=reddit_posts)


if __name__ == "__main__":
    app.run(port="80", host="0.0.0.0", debug=True)

