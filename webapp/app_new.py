from flask import render_template
from flask import Flask, jsonify, request, make_response
import psycopg2
import praw
#import config
#from config import (client_id,client_secret,user_agent)

from elasticsearch import Elasticsearch
es = Elasticsearch([{'host': '54.212.124.15', 'port': 9200}])
app = Flask(__name__)


# TODO:  1. Clicking on clear should remove the result
# TODO:  2. Watermark should disapper as soon clicked on text box
# TODO:  3. Subreddit name should be converted to lowercase

def handle_postges(text_field,year_field,month_field):
    try:
        # set connection
        connection = psycopg2.connect(user="webuiuser",
                                      password="webuiuser",
                                      host="ec2-54-214-117-182.us-west-2.compute.amazonaws.com",
                                      port="5432",
                                      database="reddit")

        # set cursor
        cursor = connection.cursor()

        # query
        postgreSQL_select_Query = "select word from word_count where subreddit=%s and year=%s and month=%s"
        input=(text_field, year_field,month_field)

        #print("input", input)

        cursor.execute(postgreSQL_select_Query,input)

        # execute query
        word_list = cursor.fetchmany(10)
        print("word_list", word_list)

        result_list=[word[0] for word in word_list]
        print("result_list",result_list)

        #print(result_list)
        # close connection
        connection.commit()

    finally:
        # closing database connection.
        if (connection):
            cursor.close()
            connection.close()
            print("PostgreSQL connection is closed")

    return result_list


def handle_elasticsearch(text_field, year_field, month_field,word):
    res = es.search(index="comments_{0}_{1:02d}".format(year_field, month_field),
                    body={"from": 0,
                          "size": 10,
                          "query": {
                              "bool":
                                  {"should": [{"match": {"subreddit": text_field}},
                                              {"match":{"year": year_field}},
                                              {"match":{"month": month_field}},
                                              {"match": {"body": word}}]
                                   }
                          }
                          }
                    )

    No_of_hits = res['hits']['total']['value']

    results_links=list()
    for hit in res['hits']['hits']:
        results_links.append(hit['_source'])

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


# ---- POST Requests: Web Application -----
# ------------------------------------------------

@app.route('/redditinsight/get_tags', methods=['POST'])
def process():
    if request.method == 'POST':
        # input parameters
        # get user inputs
        text_field = request.form['taskoption1'].strip().lower()
        # print("text_field", text_field)
        year_field = request.form['taskoption2'].strip()
        # print("year", year_field)
        month_field = request.form['taskoption3'].strip()
        # print("month",month_field)
        word_list=[]
        reddit_data=[]
        try:
        # fetch data from database
            word_list = handle_postges(text_field,year_field,month_field)

            if len(word_list)==0:
                word_list="There_are_no_tags_in_this_Subreddit".split()

            reddit = praw.Reddit(client_id="MwBq5YRdgQZbbQ", client_secret="zoBtH3BMCINonQbseGvslik6j7A",
                        user_agent="reddit_tags")

            reddit_data = []
            for submission in reddit.subreddit(text_field).top(limit=10):
                reddit_data.append(submission.title)

            #return render_template("index.html", results=word_list, results_links=reddit_data)
        except:
            print("Please select values from all the drop downs")

        return render_template("index.html", results=word_list, results_links=reddit_data)



@app.route('/redditinsight/get_tags', methods=['POST'])
def process_topics():
    if request.method == 'POST':
        # get results from elasticsearch
        filtered_posts = handle_elasticsearch("l33t", "2008", "2", "word")
        print("hello_world")

    return render_template("index.html", results_links=filtered_posts)


"""

@app.route('/background_process_test')
def background_process_test():
    print ("Hello")
    return "nothing"


@app.route("/forward/", methods=['POST'])
def move_forward():
    #Moving forward code
    forward_message = "Moving Forward..."
    return render_template('index.html', message=forward_message)

@app.route('/redditinsight/get_links', methods=['POST'])
def get_filtered_posts():
    if request.method == 'POST':
        # get results from elasticsearch
        filtered_posts=handle_elasticsearch(text_field, year_field, month_field, word)
        s="hello world"
        print(s)

    return render_template("index.html", filtered_links=s)
    #filtered_links=filtered_posts

"""

# ---- Home -----
# ----------------
@app.route('/')
def index():
    print("loaded template")
    return render_template("index.html")


if __name__ == "__main__":
    app.run(debug=True)