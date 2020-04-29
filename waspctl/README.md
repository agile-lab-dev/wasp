# Build locally

## Regenerate Api Bindings

From the root of the wasp repository run

```bash
docker run -v $PWD:/code -it registry.gitlab.com/agilefactory/agile.wasp2/waspctl-build:0.1.0 bash -c "cd /code/waspctl/ && openapi-cli-generator generate ../documentation/wasp-openapi.yaml"
```

## Build executables for all archs

From the root of the wasp repository run

```bash
docker run -v $PWD:/code -eCI_PROJECT_DIR=/code -it registry.gitlab.com/agilefactory/agile.wasp2/waspctl-build:0.1.0 bash /code/waspctl/build-for-all-archs.bash
```


# Rebuild the docker imageA
