FROM gitpod/workspace-full:2023-01-16-03-31-28

# Package for k3s
RUN sudo apt update -y
RUN sudo apt upgrade -y
RUN sudo apt update -y
RUN sudo apt install qemu qemu-system-x86 linux-image-generic libguestfs-tools sshpass netcat -y

# Package for Team
RUN brew install kubectl kustomize helm pre-commit
RUN brew install go-task/tap/go-task
RUN curl -sL https://get.garden.io/install.sh | bash
ENV PATH=$PATH:$HOME/.garden/bin
RUN sudo apt install rsync -y
