from flask import Flask
from flask import jsonify
from flask import request
from flask_pymongo import PyMongo
from flask_cors import CORS
from bson.objectid import ObjectId

from os import environ

from make_rankings import make_indicator

app = Flask(__name__)

port = int(environ.get('PORT', 33507))

if (environ.get('MONGODB_URI')):
	mongo_uri = environ.get('MONGODB_URI')
	app.config['MONGO_DBNAME'] = mongo_uri.split("/")[1]
	app.config['MONGO_URI'] = mongo_uri
else:
	app.config['MONGO_DBNAME'] = 'restdb'
	app.config['MONGO_URI'] = 'mongodb://localhost:27017/restdb'

cors = CORS(app)

mongo = PyMongo(app)

@app.route('/rankings', methods=['GET'])
def get_all_rankings():
  rankings = mongo.db.rankings
  output = []
  for s in rankings.find({"years_number":{"$exists":True}, "name":{"$exists":True}}).sort([("_id", -1)]).limit(30):
    output.append(
    	{'id' : str(s['_id'])
    	, 'name' : s['name']
    	, 'countries_num' : s['countries_num']
    	, 'years_number' : s['years_number']
    	, 'last_year' : s['last_year']
    	, 'lead_name': s['lead_name']})
  return jsonify({'result' : output})

@app.route('/ranking/<ranking_id>', methods=['GET'])
def get_one_ranking(ranking_id):
  rankings = mongo.db.rankings
  s = rankings.find_one({'_id' : ObjectId(ranking_id)})
  if s:
    output = {'name' : s['name'], 'data' : s['data']}
  else:
    output = "No such ranking"
  return jsonify({'result' : output})

@app.route('/new_ranking', methods=['POST'])
def add_ranking():
  rankings = mongo.db.rankings
  indicators = request.json['indicators']
  data, lead_name, last_year, years_number, countries_num  = make_indicator(indicators)
  ranking_id = rankings.insert(
  	{'data': data
  	, 'last_year': last_year
  	, 'years_number': years_number
  	, 'countries_num': countries_num
  	, 'lead_name': lead_name})
  return jsonify({'ranking_id' : str(ranking_id)})

@app.route('/name_ranking', methods=['POST'])
def add_name():
  rankings = mongo.db.rankings
  name = request.json['name']
  ranking_id = request.json['ranking_id']
  rankings.update_one({
	  '_id':ObjectId(ranking_id)
	},{
	  '$set': {
	    'name': name
	  }
	}, upsert=False)
  return jsonify({'result' : "success"})

if __name__ == '__main__':
    app.run(debug=True, port=port)