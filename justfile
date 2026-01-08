set dotenv-load

pre-commit: tidy generate-proto generate-sdk lint lint-client
pc: pre-commit

lint:
    golangci-lint run --fix --build-tags it,local --timeout 5m

lint-client:
    golangci-lint run --fix --build-tags it,local --timeout 5m ./cmd/client/...

tidy:
    go mod tidy

# Build the application
build:
    go build -o server ./cmd/server

# Build the client application
build-client:
    go build -o client ./cmd/client

# Run the application locally (single node)
run:
    go run ./cmd/server --node-id node-1 --bind-addr 127.0.0.1:8888 --data-dir ./data/node-1

# Run the client application
run-client:
    go run ./cmd/client

install-client:
    go build -o $GOPATH/bin/ledger-poc-client ./cmd/client
    #todo: make optional or configurable or whatever
    ledger-poc-client completion zsh > ~/.oh-my-zsh/custom/completions/_ledger-poc-client

# Run tests
test:
    go test ./... -tags it,e2e

# Clean build artifacts
clean:
    rm -rf client server

clean-benchmarks-data:
    rm -rf build

# Start Docker cluster
docker-up:
    docker-compose up -d

# Stop Docker cluster
docker-down:
    docker-compose down -v

# View logs
docker-logs:
    docker-compose logs -f

# Generate SDK from OpenAPI specification using Speakeasy
generate-sdk:
    @echo "Generating SDK from openapi.yml using Speakeasy..."
    @nix develop --command speakeasy generate sdk \
        --lang go \
        --schema openapi.yml \
        --out ./pkg/client
    @echo "SDK generated in ./pkg/client"

# Clean generated SDK
clean-sdk:
    rm -rf pkg/client

# Generate gRPC code from protobuf files
generate-proto:
    @echo "Generating gRPC code from proto files..."
    @protoc --go_out=. --go_opt=module=github.com/formancehq/ledger-v3-poc \
        --go-grpc_out=. \
        --go-grpc_opt=module=github.com/formancehq/ledger-v3-poc \
        -I proto \
        proto/raft_transport.proto \
        proto/system.proto \
        proto/ledger.proto \
        proto/commands/commands.proto \
        proto/commands/ledger_commands.proto \
        proto/commands/system_commands.proto

docker-build:
    docker buildx build \
        --platform linux/amd64 \
        --push \
        -t ${REGISTRY:-ghcr.io}/formancehq/ledger-v3-poc:latest \
        --cache-to type=registry,ref=${REGISTRY:-ghcr.io}/formancehq/ledger-v3-poc:buildcache,mode=max \
        --cache-from type=registry,ref=${REGISTRY:-ghcr.io}/formancehq/ledger-v3-poc:buildcache .
    docker push ${REGISTRY:-ghcr.io}/formancehq/ledger-v3-poc:latest

k8s-install: docker-build
    helm upgrade --install ledger-v3-poc \
        ./deployments/chart \
        --set image.repository=${REGISTRY:-ghcr.io}/formancehq/ledger-v3-poc \
        --values ./deployments/chart/dev-values.yaml

k8s-uninstall:
    helm uninstall ledger-v3-poc

k8s-watch:
    watch -n 1 kubectl get pods -l app.kubernetes.io/name=ledger-v3-poc

k8s-logs:
    kubectl logs -f statefulset/ledger-v3-poc

k8s-describe-ss:
    kubectl describe statefulset/ledger-v3-poc

k8s-describe-pod:
    kubectl describe pods/ledger-v3-poc-0

k8s-rollout-restart:
    kubectl rollout restart statefulsets/ledger-v3-poc
    kubectl rollout status statefulset/ledger-v3-poc

k8s-install-victoria-metrics:
    helm repo add victoria-metrics https://victoriametrics.github.io/helm-charts/
    helm repo update
    helm upgrade --install vm victoria-metrics/victoria-metrics-single \
        -n monitoring \
        --create-namespace \
        -f ./deployments/k8s/victoriametrics/values.yaml

k8s-install-otlp-collector:
    helm repo add open-telemetry https://open-telemetry.github.io/opentelemetry-helm-charts
    helm repo update
    helm upgrade --install otel open-telemetry/opentelemetry-collector \
        -n monitoring \
        -f ./deployments/k8s/otlp/values.yaml

k8s-install-grafana:
    helm repo add grafana https://grafana.github.io/helm-charts
    helm repo update
    kubectl create configmap grafana-dashboard \
        -n monitoring \
        --from-file=./deployments/docker-compose/grafana/provisioning/dashboards/ledger-metrics.json \
        -o yaml --dry-run=client \
        | kubectl label -f - grafana_dashboard=1 --local -o yaml \
        | kubectl apply -f -
    helm upgrade --install grafana grafana/grafana \
        -n monitoring \
        -f ./deployments/k8s/grafana/values.yaml

k8s-install-k6-operator:
    helm repo add grafana https://grafana.github.io/helm-charts
    helm repo update
    helm upgrade --install k6-operator grafana/k6-operator
    kubectl delete configmap k6-scripts || true
    kubectl create configmap k6-scripts --from-file ./k6/scripts

k8s-install-tempo:
    helm repo add grafana https://grafana.github.io/helm-charts
    helm repo update
    helm upgrade --install tempo grafana/tempo -n monitoring -f ./deployments/k8s/tempo/values.yaml

k8s-install-loki:
    helm repo add grafana https://grafana.github.io/helm-charts
    helm repo update
    helm upgrade --install loki grafana/loki -n monitoring -f ./deployments/k8s/loki/values.yaml