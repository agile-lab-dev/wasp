from flask import Flask
from flask import request
from flask import jsonify

app = Flask(__name__)

@app.route('/writeExecutionPlan', methods=['POST'])
def hello_world():
    print('---> body request: {}', request.get_json())

    readTempCred = {"accessKeyID": "ReadaccessKeyID",
                    "secretKey": "ReadsecretKey",
                    "sessionToken": "ReadsessionToken"
                    }

    writeTempCred = {
                "accessKeyID": "accesskey",
                "secretKey": "secretkey",
                "sessionToken": "token"
                     }

    tempCred = {"r": readTempCred,
                "w": writeTempCred
                }

    response = jsonify(format="Delta", writeUri="s3://mytestbucket/test3/", writeType="Cold", temporaryCredentials=tempCred)
    
    return response