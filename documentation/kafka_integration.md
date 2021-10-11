# Kafka

## Writer

### MultiTopicWriter

A `MultiTopicModel` can be used to describe a set of `TopicModels` whose properties can all differ from the properties of the other `TopicModels` involved, including the schema and the encoding of the message.

The Dataframe coming out of the strategy is converted to a Dataframe suitable for the Kafka writer plugin. This transformed Dataframe consists of these four columns:

- `key` (optional)
- `value` (required)
- `headers` (optional)
- `topic` (optional, depending whether you are using MultiTopicModel or not. Column name depends on what is defined as topicNameField in MultiTopicModel).

After a series of integrity checks on the models involved in the MultiTopicModel, a conversion of the `value` column (and possibly, `key` column too) of each row into the dataType specified in the relative TopicModel is performed.

For instance, let's consider a MultiTopicModel consisting of two `TopicModel`s that do not share the same `schema` nor the `topicDataType`.

```
val topicA = TopicModel(
    name = "topicA', 
    topicDataType = "avro", 
    keyFieldName = Some("key"), 
    valueFieldsNames = Some("name", "surname", "city"), 
    schema = ...,
    ...
)
```
```
val topicB = TopicModel(
    name = "topicA',
    topicDataType = "json", 
    keyFieldName = Some("errorId"), 
    valueFieldsNames = Some("error", "errorMsg")
    schema = ...,
    ...
)
```

```
val multiTopicModel = MultiTopicModel(name = "name", topicNameField = "topic", topicModelNames = Seq("topicA", "topicB"))
```


The schema of the Dataframe coming out of the Strategy must contain all the fields of each `TopicModel`'s schema involved and, in addition, the column _topic_ that will contain the destination topic's name where the specific row should be sent to. Therefore, following the previous example, the Dataframe should look like this:

| key | name | surname | city | errorId | error | errorMsg | topic |
| ------ | ------ | ------ | ------ | ------ | ------ | ------ | ------ | 
| "A1" | "Dave" | "Gahan" | "London" | null | null | null | "topicA" |
| null | null | null | null | "404" | "NotFound" | "There was a problem." | "topicB" |

This Dataframe is then transformed into a Dataframe whose `key` column and `value` column contain rows having different schema. Therefore, leveraging the kafka-writer-plugin, the following messages will be written into two different topics:

##### Message sent to topic "topicA" (format: _json_, as per TopicModel definition)
```
{
    "key":"A1",
    "value": {
        "name": "Dave",
        "surname": "Gahan",
        "city": "London"
    }
    "headers": someHeaders
}
```

##### Message sent to topic "topicB" (format: _avro_, as per TopicModel definition)
```
{
    "key":"404",
    "value": {
        "error": "NotFound",
        "errorMsg": "There was a problem."
    }
    "headers": someHeaders
}
```
Write operation with MultiTopicModel also fully supports complex data in the source Dataframe. For example, let's consider the previous example's Dataframe and let's wrap the valueFields in a struct:

| key | personalInfo | errorId | errorInfo | topic |
| ------ | ------ | ------ | ------ | ------ |
| "A1" | {"name": "Dave", "surname": "Gahan", "city": "London"} | null | null | "topicA" |
| null | null | "404" | {"error": "NotFound", "errorMsg": "There was a problem."} | "topicB" |

Correctly declaring the `valueFieldsNames` property in the respective TopicModel with the classical SQL dot notation `(valueFieldsNames = Some("personaInfo.name", "personalInfo.surname", ...))` the conversion works properly.


