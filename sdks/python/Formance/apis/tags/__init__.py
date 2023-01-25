# do not import all endpoints into this module because that uses a lot of memory and stack frames
# if you need the ability to import all endpoints from this module, import them with
# from Formance.apis.tag_to_api import tag_to_api

import enum


class TagValues(str, enum.Enum):
    ACCOUNTS = "Accounts"
    BALANCES = "Balances"
    CLIENTS = "Clients"
    LEDGER = "Ledger"
    LOGS = "Logs"
    MAPPING = "Mapping"
    ORCHESTRATION = "Orchestration"
    PAYMENTS = "Payments"
    SCOPES = "Scopes"
    SCRIPT = "Script"
    SEARCH = "Search"
    SERVER = "Server"
    STATS = "Stats"
    TRANSACTIONS = "Transactions"
    USERS = "Users"
    WALLETS = "Wallets"
    WEBHOOKS = "Webhooks"
    DEFAULT = "default"
