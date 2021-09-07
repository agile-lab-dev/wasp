### Testing

#### How to run TestGeneric pipegraph
* Install [localstack](https://github.com/localstack/localstack), and follow the instructions here: https://www.youtube.com/watch?v=Bytw73GvRHA
* Create s3 bucket and name it `mytestbucket` with `aws --endpoint-url=http://localhost:4566 s3 mb s3://mytestbucket` 
* Install Flask with
    * `pip install flask`
* Go to cdh6 directory 
    * `cd whitelabel/docker/cdh6/` 
* Run Wasp on cdh6 with `./start-wasp.sh` and wait for it to start
* In `whitelabel/docker/cdh6` execute following commands (you should find flask-parallel-write-entity.py file there):
    * `export FLASK_APP=flask-parallel-write-entity.py`
    * `flask run --port 9999 --host 0.0.0.0`
* Start producer and TestGeneric pipegraph
* When you run `aws --endpoint-url=http://localhost:4566 s3 ls s3://mytestbucket` you should see `test3/` inside
* After some time you should be able to see some parquet files in your s3 bucket by running `aws --endpoint-url=http://localhost:4566 s3 ls s3://mytestbucket/test3/`
