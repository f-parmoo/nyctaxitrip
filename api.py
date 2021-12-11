from flask import Blueprint, jsonify, request
from gettaxitripdata import engine
from sqlalchemy import create_engine


api = Blueprint("api", __name__, url_prefix="/api")
engine = create_engine('postgresql://postgres:postgres@localhost:5432/taxitrip')


@api.route('taxitripcount', methods=["GET"])
def taxitripcount():
    n = request.args.get('n')
    if not n:
        n = request.args.get('N')
    if n:
        result = engine.execute(f'''select  sum(1) from nyc_taxitrip where total_amount<{n} ''').fetchall()
        if result:
            result = result[0][0]
        else:
            result = None
        message = 'Success'
        return jsonify({'data': {'NumberOfTrips': result}, 'Message': message}), 200

    result = None
    message = 'Please Send N as a Parameter'
    return jsonify({'data': {'NumberOfTrips': result}, 'Message': message}), 400
