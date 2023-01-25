#!/bin/bash

script_dirname="$( cd "$( dirname "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )"
rootfslock="${script_dirname}/_output/rootfs/rootfs-ready.lock"
k3sreadylock="${script_dirname}/_output/rootfs/k3s-ready.lock"

if test -f "${k3sreadylock}"; then
    exit 0
fi

cd $script_dirname

function waitssh() {
  while ! nc -z 127.0.0.1 2222; do
    sleep 0.1
  done
  ./ssh.sh "whoami" &>/dev/null
  if [ $? -ne 0 ]; then
    sleep 1
    waitssh
  fi
}

function waitrootfs() {
  while ! test -f "${rootfslock}"; do
    sleep 0.1
  done
}

echo "🔥 Installing everything, this will be done only one time per workspace."

echo "Waiting for the rootfs to become available, it can take a while, open the terminal #2 for progress"
waitrootfs
echo "✅ rootfs available"

echo "Wait for apt lock to end"
./wait-apt.sh
sudo apt install netcat sshpass -y
echo "✅ no more apt lock"

echo "Waiting for the ssh server to become available, it can take a while, after this k3s is getting installed"
waitssh
echo "✅ ssh server available"

./ssh.sh "curl -sfL https://get.k3s.io | sh -"

mkdir -p ~/.kube
./scp.sh root@127.0.0.1:/etc/rancher/k3s/k3s.yaml ~/.kube/config

echo "✅ k3s server is ready"
sudo curl -o /usr/bin/kubectl -LO "https://dl.k8s.io/release/$(curl -L -s https://dl.k8s.io/release/stable.txt)/bin/linux/amd64/kubectl"
sudo chmod +x /usr/bin/kubectl
kubectl get pods --all-namespaces

touch "${k3sreadylock}"
