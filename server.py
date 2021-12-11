from flask import Flask
from api import api


app = Flask(__name__)
app.register_blueprint(api)
app.config['JSON_SORT_KEYS'] = False

if __name__ == '__main__':
    app.run(debug=True)
