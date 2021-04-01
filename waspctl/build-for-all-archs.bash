#!/usr/bin/env bash
OUTPUT_DIRECTORY="$CI_PROJECT_DIR/waspctl/output"

mkdir -p /go/src/waspctl
mkdir -p $OUTPUT_DIRECTORY
cp -r $CI_PROJECT_DIR/waspctl/* /go/src/waspctl
cd /go/src/waspctl

package="waspctl"
package_name="waspctl"

platforms=("windows/amd64" "darwin/amd64" "linux/amd64")

env CGO111MODULE=on CGO_ENABLED=0 GOOS=$GOOS GOARCH=$GOARCH go mod init
for platform in "${platforms[@]}"
do
    platform_split=(${platform//\// })
    GOOS=${platform_split[0]}
    GOARCH=${platform_split[1]}
    output_name=$package_name'-'$GOOS'-'$GOARCH
    if [ $GOOS = "windows" ]; then
        output_name+='.exe'
    fi


    env CGO111MODULE=on CGO_ENABLED=0 GOOS=$GOOS GOARCH=$GOARCH go get -v
    env CGO111MODULE=on CGO_ENABLED=0 GOOS=$GOOS GOARCH=$GOARCH go build -ldflags='-extldflags=-static' -v -o "$OUTPUT_DIRECTORY/$output_name" $package

    if [ $? -ne 0 ]; then
        echo 'An error has occurred! Aborting the script execution...'
        exit 1
    fi
done
