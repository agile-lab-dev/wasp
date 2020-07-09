# Configurations

This document describes the configurations available for WASP, broken down by feature/component.

## Telemetry

The model is `it.agilelab.bigdata.wasp.models.configuration.TelemetryConfigModel`.

The typesafe config object is found at path `wasp.telemetry`.

The available configurations are:

| Model field | Configuration key | Optional | Default value | Description |
|---|---|---|---|---|
| writer | writer | no | default | Name of the datastore product to use, for example "solr" or "elastic" for a specific product, or a generic "default" or empty string to leave the choice to the framework | 
| sampleOneMessageEvery | latency.sample-one-message-every | no | 100 | Latency calculation sample rate, expressed as "1 in n", eg 100 => 1 in 100 => 1% sampling rate |