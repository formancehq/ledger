# [Formance Stack](https://formance.com) • [![Ledger Stars](https://img.shields.io/github/stars/formancehq/ledger?label=Ledger%20stars)](https://github.com/formancehq/ledger/stargazers) [![License MIT](https://img.shields.io/badge/license-mit-purple)](https://github.com/formancehq/ledger/blob/main/LICENSE) [![YCombinator](https://img.shields.io/badge/Backed%20by-Y%20Combinator-%23f26625)](https://www.ycombinator.com/companies/formance-fka-numary) [![slack](https://img.shields.io/badge/slack-formance-brightgreen.svg?logo=slack)](https://bit.ly/formance-slack)

Formance is a highly modular developer platform to build and operate complex money flows of any size and shapes. It comes with several components, that can be used as a whole as the Formance Stack or separately as standalone micro-services and libraries:

- **Formance Ledger** - Programmable double-entry, immutable source of truth to record internal financial transactions and money movements
- **Formance Payments** - Unified API and data layer for payments processing
- **Formance Numscript** - DSL and virtual machine monetary computations and transactions modeling
- **Formance Control** - Web application dashboard for payments operators with unified views on all your data

## ⚡️ Getting started with Formance Cloud Sandbox

### Install Formance CLI

```SHELL
# macOS
brew tap formancehq/tap
brew install fctl
```

###
```SHELL
# login to formance cloud
fctl login

# create a sandbox stack deployment
fctl stack create foobar

# commit your first ledger transaction
fctl ledger send world foo 100 EUR/2 --ledger=demo

# checkout the control dashboard
fctl ui
```

## 💻 Getting started locally

### Requirements
1. Make sure docker is installed on your machine.
2. Make sure Docker Compose is installed and available (it should be the case if you have chosen to install Docker via Docker Desktop); and
3. Make sure Git is also installed on your machine.


### Run the app
To start using Formance Stack, run the following commands in a shell:

```
# Get the code
git clone https://github.com/formancehq/stack.git

# Go to the cloned stack directory
cd stack

# Start the stack containers
docker-compose up
```

You can now open your browser and go to http://localhost to connect to the application. The Stack's API is exposed at http://localhost/api.
You should use either use the pre-configured Github App (only for local testing) or the User/Password method to connect to the application, with the provisioned credentials:

```
User:     demo@formance.com
Password: demo
```

## ☁️ Cloud Native Deployment

The Formance Stack is distributed as a collection of binaries, with optional packaging as Docker images and configuration support through command line options and environment variables. The recommended, standard way to deploy the collection of services is to a Kubernetes cluster through our Formance official Helm charts, which repository is available at [helm.formance.com](https://helm.formance.com/).

## 📚 Documentation

The full documentation for the formance stack can be found at [docs.formance.com](https://docs.formance.com)

## 💽 Codebase

Formance is transitioning to a unified public facing monorepo (this one) that imports versioned services submodules and provides a common infrastructure layer. As we are finalizing this transition, this monorepo is structured as below

### Architecture

```
formancehq/stack/
  |- services  # Stack services imported as fixed-version submodules
  |- docs      # Exhaustive documentation deployed at docs.formance.com
  |- config    # Boilerplate configuration for stack dependencies
```

### Technologies

The Formance Stack is built on open-source, battle tested technologies including:

- **PostgreSQL** - Main storage backend
- **Redis** - Caching and services instances syncing
- **Kafka/Redpanda** - Cross-services async communication
- **Traefik** - Main HTTP gateway
