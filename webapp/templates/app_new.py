from flask import Flask, render_template, url_for, request
from flask import Flask, jsonify, abort, request, make_response
import psycopg2
#from psycopg2 import Error

from elasticsearch import Elasticsearch

es = Elasticsearch([{'host': '54.212.124.15', 'port': 9200}])

app = Flask(__name__)


# TODO:  1. Clicking on clear should remove the result
# TODO:  2. Watermark should disapper as soon clicked on text box
# TODO:  3. Subreddit name should be converted to lowercase

def handle_postges(text_field):


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
        query = "select word from word_count where subreddit==text_field"

        # execute query
        word_list = cursor.execute(query)

        # close connection
        connection.commit()


    #except (Exception, psycopg2.DatabaseError) as error:

    finally:
        # closing database connection.
        if (connection):
            cursor.close()
            connection.close()
            print("PostgreSQL connection is closed")

    return word_list


def handle_elasticsearch(text_field, year_field, month_field, word):
    res = es.search(index="comments_{0}_{1:02d}".format(year_field, month_field),
                    body={"from": 0, "size": 10, "query": {
                        "bool": {"should": [{"match": {"subreddit": text_field}}, {"match": {"body": word}}]}}})

    No_of_hits = res['hits']['total']['value']

    for hit in res['hits']['hits']:
        results_links = hit['_source']

    return results_links


# -----------------
# Error Handling
# -----------------
@app.errorhandler(400)
def not_found(error):
    return make_response(jsonify({'error': 'No such Reddit exists'}), 400)


@app.errorhandler(404)
def not_found(error):
    return make_response(jsonify({'error': 'Reddit does not exists'}), 404)


@app.errorhandler(408)
def not_found(error):
    return make_response(jsonify({'error': 'No such Reddit exists'}), 408)


# ---- POST Requests: Web Application -----
# ------------------------------------------------

@app.route('/redditinsight/get_tags', methods=['POST'])
def process():
    if request.method == 'POST':
        # input parameters

        # get user inputs
        text_field = request.form['subreddit'].strip().lower()
        # print("text_field", text_field)
        year_field = int(request.form['year'].strip())
        # print("year", year_field)
        month_field = int(request.form['month'].strip())
        # print("month",month_field)

        # fetch data from database
        word_list = handle_postges(text_field)

        # get results from elasticsearch
        for word in word_list:
            results_links = handle_elasticsearch(text_field, year_field, month_field, word)

        return render_template("index.html", results=word_list, results_links=results_links)


# ---- Home -----
# ----------------
@app.route('/')
def index():
    print("loaded template")
    return render_template("index.html")


if __name__ == "__main__":
    app.run(debug=True)