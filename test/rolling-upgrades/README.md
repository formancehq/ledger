# Rolling upgrade tests

This directory contains tests for rolling upgrades on K8S.

## Running the tests

To run the tests, you need to have a K8S cluster running. You can use the `k3d` tool to create a local K8S cluster.

You need also a Pulumi access token to run the tests. 
You can create one by following the instructions [here](https://www.pulumi.com/docs/pulumi-cloud/access-management/access-tokens/).

### Install k3d

```bash
curl -s https://raw.githubusercontent.com/rancher/k3d/main/install.sh | bash
```

### Create a K8S cluster

```bash
k3d cluster create
```

### Run the tests

```bash
kubectl create serviceaccount testing
kubectl create clusterrolebinding testing --clusterrole=cluster-admin --serviceaccount=default:testing

export KUBE_TOKEN=$(kubectl create token testing --duration=999999h)
export KUBE_APISERVER=$(kubectl config view --minify -o jsonpath='{.clusters[0].cluster.server}')
export PULUMI_ACCESS_TOKEN=<your pulumi access token>

earthly --push --no-output +run \
  --KUBE_APISERVER=$KUBE_APISERVER \
  --KUBE_TOKEN=$KUBE_TOKEN \
  --PULUMI_ACCESS_TOKEN=$PULUMI_ACCESS_TOKEN
```

### Delete the K8S cluster

```bash
k3d cluster delete
```

## Test description

The test :
* creates a K8S deployment with a single replica of the server
* then create a test pod in charge of sending requests to the web server and checking if the response is ok.
* then updates the deployment with a new image and waits for the new pod to be ready.
* then checks if the test pod is still alive. If alive, it indicates no errors during the rolling upgrade.

Under the hood, the test will create a [VCluster](https://www.vcluster.com/docs/get-started) on your k3d cluster.
This VCluster will be used to run the tests and simulate a real rolling upgrade on a K8S cluster.
