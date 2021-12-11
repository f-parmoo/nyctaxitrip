# NYC Taxi Trip
 
REST API written in python using flask

## Getting started

Make sure you're using python 3.8

Setup virtual env

```
virtualenv --python=python3 venv
source venv/bin/activate
```

Install requirements

```
pip install -r requirements.txt
```
Get Taxi Trip Data,
Download data from this link: https://s3.amazonaws.com/nyc-tlc/trip+data/yellow_tripdata_2020-01.csv

Copy data in the root of the project, run this code to load data in database
```
python gettaxitripdata.py
```


Start server

```
python server.py
```

Hit the API

```
GET http://127.0.0.1:5000/api/taxitripcount
```
