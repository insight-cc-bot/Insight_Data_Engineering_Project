from flask import Flask, render_template, url_for, request
from flask import Flask, jsonify, abort, request, make_response
from elasticsearch import Elasticsearch
es = Elasticsearch([{'host': '54.212.124.15', 'port': 9200}])
# NOTES: Use following command to terminate running port -
# netstat -vanp tcp | grep 5000 to see port
# sudo lsof -i tcp:5000
# kill process
app = Flask(__name__)

import pandas as pd

columns=["word", "subreddit_id", "subreddit","year", "month", "count", "rank"]
df= pd.read_csv("/Users/smiley/Documents/Insight/part-00007-3212c4eb-7539-42f5-8a5b-0dfacbd5d28e-c000.csv"
                ,names=columns)

#TODO: 1. Clicking on clear should remove the result
#TODO:  2. Watermark should disapper as soon clicked on text box
#TODO:  3. Subreddit name should be converted to lowercase

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


        #extract user inputs

        text_field = request.form['subreddit'].strip().lower()
        #print("text_field", text_field)
        year_field=int(request.form['year'].strip())
        #print("year", year_field)
        month_field=int(request.form['month'].strip())

        #print("month",month_field)

        #fetch data from dataframe

        subreddit_df=df[df["subreddit"]==text_field]
        year_df=subreddit_df[subreddit_df["year"]==year_field]
        month_df=year_df[year_df["month"]==month_field]
        results = month_df["word"][:10]

        print("results",results)


        #subreddit:reddit.com
        #year:2006
        #month: 07

        res = es.search(index="comments_{0}_{1:02d}".format(year_field,month_field), body={"from": 0, "size": 10, "query": {
            "bool": {"should": [{"match": {"subreddit": text_field}}, {"match": {"body": "java"}}]}}})
        #            "match": {'body':'java','subreddit':'programming'}}})
        print("Got %d Hits:" % res['hits']['total']['value'])
        for hit in res['hits']['hits']:
            results_links = hit['_source']
            print(results_links)


        return render_template("index.html", results=results ,results_links=results_links)





# ---- Home -----
# ----------------
@app.route('/')
def index():
    print("loaded template")
    return render_template("index.html")


if __name__ == "__main__":
    app.run(debug=True)