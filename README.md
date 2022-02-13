# Numary Ledger [![test](https://github.com/numary/ledger/actions/workflows/main.yml/badge.svg)](https://github.com/numary/ledger/actions/workflows/main.yml) [![goreportcard](https://goreportcard.com/badge/github.com/numary/ledger)](https://goreportcard.com/report/github.com/numary/ledger) [![discord](https://img.shields.io/discord/846686859869814784?label=chat%20@%20discord)](https://discord.gg/xyHvcbzk4w)

![Numary GitHub Header Illustration Monochrome](https://user-images.githubusercontent.com/1770991/134161854-03797c76-d580-4b15-b0cf-5f8ce0080efb.png)

Numary is a programmable financial ledger that provides a foundation for money-moving applications. The ledger provides atomic multi-postings transactions and is programmable in [Numscript](doc:machine-instructions), a built-in language dedicated to money movements. It will shine for apps that require a lot of custom, money-moving code, e.g:

* E-commerce with complex payments flows, payments splitting, such as marketplaces
* Company-issued currencies systems, e.g. Twitch Bits
* In-game currencies, inventories and trading systems, e.g. Fortnite V-Bucks
* Payment gateways using non-standard assets, e.g. learning credits
* Local currencies and complementary finance

# Getting started

Numary works as a standalone binary, the latest of which can be downloaded from the [releases page](https://github.com/numary/ledger/releases). You can move the binary to any executable path, such as to `/usr/local/bin`. Installations using brew, apt, yum or docker are also [available](https://docs.numary.com/docs/installation-1).

```SHELL

numary server start

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

numary exec quickstart example.num

# Get the balances of drivers:042
curl -X GET http://localhost:3068/quickstart/accounts/drivers:042

# List transactions
curl -X GET http://localhost:3068/quickstart/transactions
```

# Documentation

You can find the complete Numary documentation at [docs.numary.com](https://docs.numary.com)

# Dashboard

<img width="909" alt="control-screenshot" src="https://user-images.githubusercontent.com/1770991/153751534-d8bba99e-610a-4b8c-9c63-4bde6eb6f96f.png">

A simple [dashboard](https://github.com/numary/control) is built in the ledger binary, to make it easier to visualize transactions. It can be started with:

```SHELL
numary ui
```

Or by heading to [control.numary.com](https://control.numary.com)

# Roadmap & Community

We keep an open roadmap of the upcoming releases and features [here](https://numary.notion.site/OSS-Roadmap-4535fa5716fb4f618027201afcc6f204).

If you need help, want to show us what you built or just hang out and chat about ledgers you are more than welcome on our [Discord](https://discord.gg/xyHvcbzk4w) - looking forward to see you there!

![Frame 1 (2)](https://user-images.githubusercontent.com/1770991/134163361-d86c5728-6075-4510-8de7-06df1f6ed740.png)

# Contributors ✨

Thanks goes to these wonderful people ([emoji key](https://allcontributors.org/docs/en/emoji-key)):

<!-- ALL-CONTRIBUTORS-LIST:START - Do not remove or modify this section -->
<!-- prettier-ignore-start -->
<!-- markdownlint-disable -->
<table>
  <tr>
    <td align="center"><a href="https://github.com/Azorlogh"><img src="https://avatars.githubusercontent.com/u/17968319?v=4?s=100" width="100px;" alt=""/><br /><sub><b>Alix Bott</b></sub></a><br /><a href="https://github.com/numary/ledger/commits?author=Azorlogh" title="Code">💻</a></td>
    <td align="center"><a href="https://github.com/flemzord"><img src="https://avatars.githubusercontent.com/u/1952914?v=4?s=100" width="100px;" alt=""/><br /><sub><b>Maxence Maireaux</b></sub></a><br /><a href="#infra-flemzord" title="Infrastructure (Hosting, Build-Tools, etc)">🚇</a> <a href="#platform-flemzord" title="Packaging/porting to new platform">📦</a> <a href="https://github.com/numary/ledger/commits?author=flemzord" title="Code">💻</a></td>
    <td align="center"><a href="https://github.com/henry-jackson"><img src="https://avatars.githubusercontent.com/u/34102861?v=4?s=100" width="100px;" alt=""/><br /><sub><b>Henry Jackson</b></sub></a><br /><a href="https://github.com/numary/ledger/commits?author=henry-jackson" title="Code">💻</a></td>
    <td align="center"><a href="https://matias.insaurral.de/"><img src="https://avatars.githubusercontent.com/u/20110?v=4?s=100" width="100px;" alt=""/><br /><sub><b>Matias Insaurralde</b></sub></a><br /><a href="https://github.com/numary/ledger/commits?author=matiasinsaurralde" title="Code">💻</a> <a href="https://github.com/numary/ledger/pulls?q=is%3Apr+reviewed-by%3Amatiasinsaurralde" title="Reviewed Pull Requests">👀</a></td>
    <td align="center"><a href="https://github.com/S0c5"><img src="https://avatars.githubusercontent.com/u/5241972?v=4?s=100" width="100px;" alt=""/><br /><sub><b>David barinas</b></sub></a><br /><a href="https://github.com/numary/ledger/commits?author=S0c5" title="Code">💻</a></td>
    <td align="center"><a href="https://github.com/djimnz"><img src="https://avatars.githubusercontent.com/u/949997?v=4?s=100" width="100px;" alt=""/><br /><sub><b>David Jimenez</b></sub></a><br /><a href="https://github.com/numary/ledger/commits?author=djimnz" title="Code">💻</a></td>
    <td align="center"><a href="http://32b6.com/"><img src="https://avatars.githubusercontent.com/u/1770991?v=4?s=100" width="100px;" alt=""/><br /><sub><b>Clément Salaün</b></sub></a><br /><a href="#ideas-altitude" title="Ideas, Planning, & Feedback">🤔</a></td>
  </tr>
  <tr>
    <td align="center"><a href="https://karmanyaah.malhotra.cc/"><img src="https://avatars.githubusercontent.com/u/32671690?v=4?s=100" width="100px;" alt=""/><br /><sub><b>Karmanyaah Malhotra</b></sub></a><br /><a href="#userTesting-karmanyaahm" title="User Testing">📓</a></td>
  </tr>
</table>

<!-- markdownlint-restore -->
<!-- prettier-ignore-end -->

<!-- ALL-CONTRIBUTORS-LIST:END -->

This project follows the [all-contributors](https://github.com/all-contributors/all-contributors) specification. Contributions of any kind welcome!
