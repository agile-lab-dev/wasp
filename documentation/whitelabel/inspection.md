## Inspection

After starting WASP using whitelabel/cdh6 or whitelabel/cdh7, you can start pipegraphs and producers using the provided REST API.
To browse all pipegraphs and their settings you can use:
``` curl -X GET http://localhost:2891/pipegraphs``` which will return a JSON document.

To start a specific pipegraph:

``` curl -X POST http://localhost:2891/pipegraphs/{pipegraph_name}/start ```

Example:

``` curl -X POST http://localhost:2891/pipegraphs/TestConsoleWriterStructuredJSONPipegraph/start ```

For that specific **Pipegraph** we can see that it uses the **test_json.topic** to get data from Kafka, and sends it to the console.

To actually have some data go through the topic, we have to start a **producer**, that will populate the topic.

Start a specific producer:

``` curl -X POST http://localhost:2891/producers/{producer_name}/start ```

For this example we will start the TestJSONProducer, which also writes into the test_json.topic.

After a few seconds you should start seeing some output in the console, in form of tables or logs about document creation.

The full REST API can be found at [REST API](../api.md)