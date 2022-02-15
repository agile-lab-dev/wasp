from flask import Flask
from flask import request
from flask import jsonify

app = Flask(__name__)

@app.route("/writeExecutionPlan", methods=['POST'])
def send_response():
    print("---> body request: {}", request.get_json())


    import boto3
    import os

    AWS_ACCESS_KEY_ID = os.environ.get('MINIO_ROOT_USER')
    AWS_SECRET_ACCESS_KEY = os.environ.get('MINIO_ROOT_PASSWORD')
    MINIO_ENDPOINT = 'http://' + os.environ.get('MINIO_ENDPOINT')

    session = boto3.Session(aws_access_key_id=AWS_ACCESS_KEY_ID, aws_secret_access_key=AWS_SECRET_ACCESS_KEY)
    sts = session.client('sts', endpoint_url=MINIO_ENDPOINT, region_name='eu-central-1')
    credentials = sts.assume_role(RoleArn='arn:xxx:xxx:xxx:xxxx', RoleSessionName='name', DurationSeconds=3600)

    TEMPORARY_ACCESS_KEY_ID=credentials['Credentials']['AccessKeyId']
    TEMPORARY_SECRET_ACCESS_KEY=credentials['Credentials']['SecretAccessKey']
    TEMPORARY_SESSION_TOKEN=credentials['Credentials']['SessionToken']

    print(TEMPORARY_ACCESS_KEY_ID)
    print(TEMPORARY_SECRET_ACCESS_KEY)
    print(TEMPORARY_SESSION_TOKEN)

    readTempCred = {"accessKeyID": TEMPORARY_ACCESS_KEY_ID, "secretKey": TEMPORARY_SECRET_ACCESS_KEY, "sessionToken": TEMPORARY_SESSION_TOKEN}
    writeTempCred = {"accessKeyID": TEMPORARY_ACCESS_KEY_ID, "secretKey": TEMPORARY_SECRET_ACCESS_KEY, "sessionToken": TEMPORARY_SESSION_TOKEN}

    tempCred = {"r": readTempCred,
                "w": writeTempCred
                }

    format = app.config.get("format")
    writeType = app.config.get("writeType")
    response = jsonify(format=format, writeUri="s3://entity", writeType=writeType, temporaryCredentials=tempCred)

    return response

if __name__ == "__main__":
    from argparse import ArgumentParser

    parser = ArgumentParser()
    parser.add_argument("-t", "--type",   choices=["Cold", "Hot"],      required=True, help="Parallel write type")  #write type
    parser.add_argument("-f", "--format", choices=["Delta", "Parquet"], help="Cold parallel write format")  #format
    args = parser.parse_args()

    if args.type == "Cold" and (args.format is None):
        parser.error("Parallel write type Cold requires a cold write format (Delta/Parquet)")

    app.config["format"] = args.format
    app.config["writeType"] = args.type
#     app.run(host=args.host, port=args.port)
    app.run(host="0.0.0.0", port=9999)