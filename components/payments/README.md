# Formance Payments [![test](https://github.com/formancehq/payments/actions/workflows/main.yml/badge.svg)](https://github.com/formancehq/payments/actions/workflows/main.yml) [![goreportcard](https://goreportcard.com/badge/github.com/formancehq/payments)](https://goreportcard.com/report/github.com/formancehq/payments) [![discord](https://img.shields.io/discord/846686859869814784?label=chat%20@%20discord)](https://discord.gg/xyHvcbzk4w)

# Getting started

Payments works as a standalone binary, the latest of which can be downloaded from the [releases page](https://github.com/formancehq/payments/releases). You can move the binary to any executable path, such as to `/usr/local/bin`. Installations using brew, apt, yum or docker are also [available](https://docs.formance.com/oss/payments/get-started/installation).

```SHELL
payments
```

# What is it?

Basically, a framework.

A framework to ingest payin and payout coming from different payment providers (PSP).

The framework contains connectors. Each connector is basically a translator for a PSP.
Translator, because the main role of a connector is to translate specific PSP payin/payout formats to a generalized format used at Formance.

Because it is a framework, it is extensible. Please follow the guide below if you want to add your connector.

# Contribute

Please follow [this guide](./docs/development.md) if you want to contribute.

# Roadmap & Community

We keep an open roadmap of the upcoming releases and features [here](https://numary.notion.site/OSS-Roadmap-4535fa5716fb4f618027201afcc6f204).

If you need help, want to show us what you built or just hang out and chat about paymentss you are more than welcome on our [Discord](https://discord.gg/xyHvcbzk4w) - looking forward to see you there!

![Frame 1 (2)](https://user-images.githubusercontent.com/1770991/134163361-d86c5728-6075-4510-8de7-06df1f6ed740.png)
