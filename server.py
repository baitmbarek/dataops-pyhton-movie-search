from flask import Flask, request, send_from_directory
from elasticsearch import Elasticsearch
import json

es = Elasticsearch(["http://elastic:9200"])

app = Flask(__name__)

@app.route('/')
def home():
  return send_from_directory(".","index.html")

@app.route('/js/<path:path>')
def send_js(path):
  return send_from_directory('js', path)

@app.route('/css/<path:path>')
def send_css(path):
  return send_from_directory('css', path)

@app.route('/aggs')
def getAggs():
    res = es.search(index="myindex", body={"size": 0, "aggs": {"genres": {"terms": { "field": "genres.name.keyword", "size":20 }}, "keywords": {"terms": { "field": "keywords.name.keyword", "size":25 }}, "status": {"terms": { "field": "status.keyword", "size":10 }}, "vote_average": {"range":{"field":"vote_average", "ranges": [{ "to": "5.0" }, { "from": "5.0", "to": "6.0" }, {"from": "6.0", "to": "7.0"}, {"from": "7.0", "to": "8.0"}, {"from": "8.0"}]}}, "runtime": {"range":{"field":"runtime", "ranges":[{"key": "< 30 min", "to": 30}, {"key": "30 - 60 min", "from": 30, "to": "60"}, {"key": "60 - 90 min", "from": 60, "to": 90}, {"key": "1h30 - 2h30", "from": 90, "to": 150}, {"key": "> 2h30", "from": 150}]}}, "decade": {"terms": {"field": "decade.keyword", "size": 20} } }} )
    aggs = res['aggregations']

    return {"genres": aggs['genres']['buckets'], "keywords": aggs['keywords']['buckets'], "status": aggs['status']['buckets'], "vote_average": aggs['vote_average']['buckets'], "runtime": aggs['runtime']['buckets'], "decade": aggs["decade"]["buckets"]}

def extractFilters(request):
  data = request.get_json()

  termsFilters = []
  keywords = filter(lambda f: f['key'] not in ["vote_average", "runtime"], data.get("filters"))
  for entry in map(lambda f: {"term": {f['key']+'.keyword': {"value": f['value']}}}, keywords):
    termsFilters.append(entry)

  rangeFilters = []
  for entry in filter(lambda x: "to" in x or "from" in x , data.get("filters")):
    if "from" in entry and "to" in entry:
      rangeFilters.append({"range": {entry['key']: { "gte": entry['from'], "lt": entry['to'] }}})
    elif "from" in entry:
      rangeFilters.append({"range": {entry['key']: { "gte": entry['from'] }}})
    else:
      rangeFilters.append({"range": {entry['key']: { "lt": entry['to'] }}})


  filters = termsFilters + rangeFilters

  return filters

@app.route('/search',methods = ['POST'])
def search():
  data = request.get_json()

  multimatchFilters = []
  for entry in map(lambda word: {"multi_match": {"query": word, "fields": [ "title", "cast.character", "cast.name", "crew.name", "genres.name", "keywords.name" ], "type": "phrase_prefix" }}, data.get("search").split()):
    multimatchFilters.append(entry)

  filters = extractFilters(request) + multimatchFilters

  query = {"size": 10, "query": {"bool":{"must": filters}}, "highlight": {"fields":{
    "title":{},
    "cast.character":{},
    "cast.name":{},
    "crew.name":{}
  }}}

  res = es.search(index="myindex", body=query)

  return res["hits"]

@app.route("/suggest",methods = ['POST'])
def suggest():
  data = request.get_json()

  filters = extractFilters(request) + [{"multi_match": {"query": "'"+data.get("search")+"'", "fields": [ "title", "cast.name", "crew.name" ], "type": "phrase_prefix" }}]

  query = {"size": 8, "query": {"bool": {"must": filters}}, "_source": "", "highlight": {"fields":{
    "title":{},
    "cast.character":{},
    "cast.name":{},
    "crew.name":{}
  }}}
  res = es.search(index="myindex", body=query)
  highlights = []
  for entry in map(lambda e: e['highlight'], res["hits"]["hits"]):
    highlights.append(entry)
  return json.dumps(highlights)
  
if __name__ == '__main__':
  app.run('0.0.0.0', 8000)

