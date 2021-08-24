##### Setting up the development environment
WASP is written in Scala, and the build is managed with SBT.

The recommended development environment is Linux; anyway developing on Windows or MacOS should be quite easy too.

Before starting:
- Install JDK 8
- Install SBT
- Install Git


The steps to getting WASP up and running for development are pretty simple:

*Github mirror*

- Clone this repository:

    `git clone https://github.com/agile-lab-dev/wasp.git`

- Build the image locally (it is a cloudera based deployment so we cannot distribute it)

```sh
cd whitelabel/docker/cdh6/cdh-docker
docker build . -t  registry.gitlab.com/agilefactory/agile.wasp2/cdh-docker:6.3.2
```

 - Use the scripts to start WASP:

    `whitelabel/docker/cdh6/start-wasp.sh`

*Gitlab* 

- Clone this repository:

    `git clone https://gitlab.com/AgileFactory/Agile.Wasp2/` 

- Perform a docker login on gitlab registry:
  
    `docker login registry.gitlab.com`

- Use the scripts to start WASP:
    
    `whitelabel/docker/cdh6/start-wasp.sh`
    
##### Just try wasp without dev dependencies

*Github mirror*

Sorry at the moment no public accessible image is available due to distribution concerns, you can build it yourself
but you still need a development environment set up

*Gitlab mirror*

- Login to the registry:

    `docker login registry.gitlab.com`

- Run wasp chosing a version (replace `$TAG`):

    `docker run -it --rm -p 8088:8088 -p 2891:2891 -p 8042:8042 -p 5005:5005 -p 5006:5006 -p 5007:5007 -p 5008:5008 -p8983:8983 -p4040:4040 -p4041:4041 registry.gitlab.com/agilefactory/agile.wasp2/try-wasp:$TAG`