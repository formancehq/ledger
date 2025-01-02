VERSION --wildcard-builds 0.8
PROJECT FormanceHQ/ledger

IMPORT github.com/formancehq/earthly:tags/v0.19.0 AS core

postgres:
    FROM postgres:15-alpine

sources:
    FROM core+builder-image
    CACHE --id go-mod-cache /go/pkg/mod
    CACHE --id go-cache /root/.cache/go-build
    WORKDIR /src/pkg/client
    COPY pkg/client/go.mod pkg/client/go.sum ./
    RUN go mod download
    WORKDIR /src
    COPY go.mod go.sum ./
    RUN go mod download
    COPY --dir internal pkg cmd .
    COPY main.go .
    SAVE ARTIFACT /src

compile:
    LOCALLY
    ARG VERSION=latest
    RUN go build -o main -ldflags="-X ${GIT_PATH}/cmd.Version=${VERSION} \
        -X ${GIT_PATH}/cmd.BuildDate=$(date +%s) \
        -X ${GIT_PATH}/cmd.Commit=${EARTHLY_BUILD_SHA}"
    SAVE ARTIFACT main

build-image:
    FROM core+final-image
    ENTRYPOINT ["/bin/ledger"]
    CMD ["serve"]
    COPY --pass-args (+compile/main) /bin/ledger
    ARG REPOSITORY=ghcr.io
    ARG tag=latest
    DO --pass-args core+SAVE_IMAGE --COMPONENT=ledger --REPOSITORY=${REPOSITORY} --TAG=$tag

deploy:
    COPY (+sources/*) /src
    LET tag=$(tar cf - /src | sha1sum | awk '{print $1}')
    WAIT
        BUILD --pass-args +build-image --tag=$tag
    END
    FROM --pass-args core+vcluster-deployer-image
    RUN kubectl patch Versions.formance.com default -p "{\"spec\":{\"ledger\": \"${tag}\"}}" --type=merge

deploy-staging:
    BUILD --pass-args core+deploy-staging

pre-commit:
    BUILD +openapi
    BUILD +openapi-markdown
    BUILD +generate-client

openapi:
    FROM node:20-alpine
    RUN apk update && apk add yq
    RUN npm install -g openapi-merge-cli
    WORKDIR /src
    COPY --dir openapi openapi
    RUN openapi-merge-cli --config ./openapi/openapi-merge.json
    RUN yq -oy ./openapi.json > openapi.yaml
    SAVE ARTIFACT ./openapi.yaml AS LOCAL ./openapi.yaml

openapi-markdown:
    FROM node:20-alpine
    RUN npm install -g widdershins
    COPY openapi/v2.yaml openapi.yaml
    RUN widdershins openapi.yaml -o README.md --search false --language_tabs 'http:HTTP' --summary --omitHeader
    SAVE ARTIFACT README.md AS LOCAL docs/api/README.md

generate-client:
    FROM node:20-alpine
    RUN apk update && apk add yq jq
    WORKDIR /src
    COPY (core+sources-speakeasy/speakeasy) /bin/speakeasy
    COPY (+openapi/openapi.yaml) openapi.yaml
    RUN cat ./openapi.yaml |  yq e -o json > openapi.json
    COPY (core+sources/out --LOCATION=openapi-overlay.json) openapi-overlay.json
    RUN jq -s '.[0] * .[1]' openapi.json openapi-overlay.json > final.json
    COPY --dir pkg/client client
    RUN --secret SPEAKEASY_API_KEY speakeasy generate sdk -s ./final.json -o ./client -l go
    SAVE ARTIFACT client AS LOCAL ./pkg/client

export-database-schema:
    FROM +sources
    RUN go install github.com/roerohan/wait-for-it@latest
    WITH DOCKER --load=postgres:15-alpine=+postgres --pull schemaspy/schemaspy:6.2.4
        RUN bash -c '
            echo "Creating PG server...";
            postgresContainerID=$(docker run -d --rm -e POSTGRES_USER=root -e POSTGRES_PASSWORD=root -e POSTGRES_DB=formance --net=host postgres:15-alpine);
            wait-for-it -w 127.0.0.1:5432;

            echo "Creating bucket...";
            go run main.go buckets upgrade _default --postgres-uri "postgres://root:root@127.0.0.1:5432/formance?sslmode=disable";

            echo "Exporting schemas...";
            docker run --rm -u root \
              -v ./docs/database:/output \
              --net=host \
              schemaspy/schemaspy:6.2.4 -u root -db formance -t pgsql11 -host 127.0.0.1 -port 5432 -p root -schemas _system,_default;

            docker kill "$postgresContainerID";
        '
    END
    SAVE ARTIFACT docs/database/_system/diagrams AS LOCAL docs/database/_system/diagrams
    SAVE ARTIFACT docs/database/_default/diagrams AS LOCAL docs/database/_default/diagrams
