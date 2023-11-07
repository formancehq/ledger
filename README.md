# Formance Ledger [![test](https://github.com/formancehq/ledger/actions/workflows/main.yml/badge.svg)](https://github.com/formancehq/ledger/actions/workflows/main.yml) [![goreportcard](https://goreportcard.com/badge/github.com/formancehq/ledger)](https://goreportcard.com/report/github.com/formancehq/ledger) [![slack](https://img.shields.io/badge/slack-formance-brightgreen.svg?logo=slack)](https://bit.ly/formance-slack) [![codecov](https://codecov.io/gh/formancehq/ledger/branch/main/graph/badge.svg?token=3PUKLWIKX3)](https://codecov.io/gh/formancehq/ledger)

<p align="center">
  <h3 align="center">Formance Ledger is now part of the Formance Stack!</h3>
  <p align="center">Checkout the Stack <a href="https://github.com/formancehq/stack">repository</a></p>
</p>

---

Formance Ledger (fka numary) is a programmable financial ledger that provides a foundation for money-moving applications. The ledger provides atomic multi-postings transactions and is programmable in [Numscript](doc:machine-instructions), a built-in language dedicated to money movements. It can be used either as a standalone micro-service or as part of the greater Formance Stack, and will shine for apps that require a lot of custom, money-moving code, e.g:

* E-commerce with complex payments flows, payments splitting, such as marketplaces
* Company-issued currencies systems, e.g. Twitch Bits
* In-game currencies, inventories and trading systems, e.g. Fortnite V-Bucks
* Payment gateways using non-standard assets, e.g. learning credits
* Local currencies and complementary finance

<p align="center">
  <img src="https://user-images.githubusercontent.com/1770991/167574970-45d1ab7e-6c57-45a5-9b46-0e849c62f98c.svg" width="400px"/>
</p>

# Getting started

Formance Ledger works as a standalone binary, the latest of which can be downloaded from the [releases page](https://github.com/formancehq/ledger/releases). You can move the binary to any executable path, such as to `/usr/local/bin`. Installations using brew, apt, yum or docker are also [available](https://docs.formance.com/docs/installation-1).

```SHELL

ledger server start

# Submit a first transaction
echo "
send [USD/2 599] (
  source = @world
  destination = @payments:001
)

send [USD/2 599] (
  source = @payments:001
  destination = @rides:0234
)

send [USD/2 599] (
  source = @rides:0234
  destination = {
    85/100 to @drivers:042
    15/100 to @platform:fees
  }
)
" > example.num

ledger exec quickstart example.num

# Get the balances of drivers:042
curl -X GET http://localhost:3068/quickstart/accounts/drivers:042

# List transactions
curl -X GET http://localhost:3068/quickstart/transactions
```

# Documentation

You can find the complete Numary documentation at [docs.formance.com](https://docs.formance.com)

# Community

If you need help, want to show us what you built or just hang out and chat about ledgers you are more than welcome on our [Slack](https://bit.ly/formance-slack) - looking forward to see you there!

![Frame 1 (2)](https://user-images.githubusercontent.com/1770991/134163361-d86c5728-6075-4510-8de7-06df1f6ed740.png)

# Quick deploy

Want to give a shot to the latest version? You can easily deploy a test instance with Heroku and the button below:

[![Deploy](https://www.herokucdn.com/deploy/button.svg)](https://heroku.com/deploy?template=https://github.com/formancehq/ledger)

# How to contribute

Want to contribute to the project? Please read the [CONTRIBUTING.md](https://github.com/formancehq/ledger/blob/main/CONTRIBUTING.md) file.

We are using [Task](https://taskfile.dev) to easily lint or test the project locally. You can install it with:
```SHELL
go install github.com/go-task/task/v3/cmd/task@latest
```
Then you can run `task` to run both the linters and the tests. You will find other tasks in the [Taskfile](https://github.com/formancehq/ledger/blob/main/Taskfile.yaml).

# Contributors ✨

Thanks goes to these wonderful people ([emoji key](https://allcontributors.org/docs/en/emoji-key)):

<!-- ALL-CONTRIBUTORS-LIST:START - Do not remove or modify this section -->
<!-- prettier-ignore-start -->
<!-- markdownlint-disable -->
<table>
  <tbody>
    <tr>
      <td align="center"><a href="https://github.com/Azorlogh"><img src="https://avatars.githubusercontent.com/u/17968319?v=4?s=100" width="100px;" alt=""/><br /><sub><b>Alix Bott</b></sub></a><br /><a href="https://github.com/formancehq/ledger/commits?author=Azorlogh" title="Code">💻</a></td>
      <td align="center"><a href="https://www.flemzord.fr/"><img src="https://avatars.githubusercontent.com/u/1952914?v=4?s=100" width="100px;" alt=""/><br /><sub><b>Maxence Maireaux</b></sub></a><br /><a href="#infra-flemzord" title="Infrastructure (Hosting, Build-Tools, etc)">🚇</a> <a href="#platform-flemzord" title="Packaging/porting to new platform">📦</a> <a href="https://github.com/formancehq/ledger/commits?author=flemzord" title="Code">💻</a></td>
      <td align="center"><a href="https://github.com/henry-jackson"><img src="https://avatars.githubusercontent.com/u/34102861?v=4?s=100" width="100px;" alt=""/><br /><sub><b>Henry Jackson</b></sub></a><br /><a href="https://github.com/formancehq/ledger/commits?author=henry-jackson" title="Code">💻</a></td>
      <td align="center"><a href="https://matias.insaurral.de/"><img src="https://avatars.githubusercontent.com/u/20110?v=4?s=100" width="100px;" alt=""/><br /><sub><b>Matias Insaurralde</b></sub></a><br /><a href="https://github.com/formancehq/ledger/commits?author=matiasinsaurralde" title="Code">💻</a> <a href="https://github.com/formancehq/ledger/pulls?q=is%3Apr+reviewed-by%3Amatiasinsaurralde" title="Reviewed Pull Requests">👀</a></td>
      <td align="center"><a href="https://github.com/S0c5"><img src="https://avatars.githubusercontent.com/u/5241972?v=4?s=100" width="100px;" alt=""/><br /><sub><b>David barinas</b></sub></a><br /><a href="https://github.com/formancehq/ledger/commits?author=S0c5" title="Code">💻</a></td>
      <td align="center"><a href="https://github.com/djimnz"><img src="https://avatars.githubusercontent.com/u/949997?v=4?s=100" width="100px;" alt=""/><br /><sub><b>David Jimenez</b></sub></a><br /><a href="https://github.com/formancehq/ledger/commits?author=djimnz" title="Code">💻</a></td>
      <td align="center"><a href="http://32b6.com/"><img src="https://avatars.githubusercontent.com/u/1770991?v=4?s=100" width="100px;" alt=""/><br /><sub><b>Clément Salaün</b></sub></a><br /><a href="#ideas-altitude" title="Ideas, Planning, & Feedback">🤔</a></td>
    </tr>
    <tr>
      <td align="center"><a href="https://karmanyaah.malhotra.cc/"><img src="https://avatars.githubusercontent.com/u/32671690?v=4?s=100" width="100px;" alt=""/><br /><sub><b>Karmanyaah Malhotra</b></sub></a><br /><a href="#userTesting-karmanyaahm" title="User Testing">📓</a></td>
      <td align="center"><a href="https://www.linkedin.com/in/antoinegelloz/"><img src="https://avatars.githubusercontent.com/u/42968436?v=4?s=100" width="100px;" alt=""/><br /><sub><b>Antoine Gelloz</b></sub></a><br /><a href="https://github.com/formancehq/ledger/commits?author=antoinegelloz" title="Code">💻</a></td>
      <td align="center"><a href="https://github.com/jdupas22"><img src="https://avatars.githubusercontent.com/u/106673437?v=4?s=100" width="100px;" alt=""/><br /><sub><b>jdupas22</b></sub></a><br /><a href="https://github.com/formancehq/ledger/commits?author=jdupas22" title="Code">💻</a></td>
      <td align="center"><a href="https://edwardpoot.com"><img src="https://avatars.githubusercontent.com/u/1686739?v=4?s=100" width="100px;" alt=""/><br /><sub><b>Edward Poot</b></sub></a><br /><a href="https://github.com/formancehq/ledger/commits?author=edwardmp" title="Code">💻</a></td>
      <td align="center"><a href="https://github.com/nicoabie"><img src="https://avatars.githubusercontent.com/u/2797992?v=4?s=100" width="100px;" alt=""/><br /><sub><b>Nico Gallinal</b></sub></a><br /><a href="https://github.com/formancehq/ledger/issues?q=author%3Anicoabie" title="Bug reports">🐛</a></td>
      <td align="center"><a href="https://github.com/gfyrag"><img src="https://avatars.githubusercontent.com/u/9094799?v=4?s=100" width="100px;" alt=""/><br /><sub><b>Ragot Geoffrey</b></sub></a><br /><a href="https://github.com/formancehq/ledger/commits?author=gfyrag" title="Code">💻</a></td>
    </tr>
  </tbody>
</table>

<!-- markdownlint-restore -->
<!-- prettier-ignore-end -->

<!-- ALL-CONTRIBUTORS-LIST:END -->

This project follows the [all-contributors](https://github.com/all-contributors/all-contributors) specification. Contributions of any kind welcome!
