# This file is part of Maker Keeper Framework.
#
# Copyright (C) 2019 grandizzy
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU Affero General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU Affero General Public License for more details.
#
# You should have received a copy of the GNU Affero General Public License
# along with this program.  If not, see <http://www.gnu.org/licenses/>.

import argparse
import logging
import sys
import getpass

from eth_account import Account
from market_maker_keeper.util import setup_logging
from pymaker.lifecycle import Lifecycle
from uniswap.uniswap import UniswapWrapper
from market_maker_keeper.feed import ExpiringFeed, WebSocketFeed, FixedFeed
from web3 import Web3, HTTPProvider
from pymaker.keys import register_keys
from pymaker.token import ERC20Token
from pymaker import Address


ONE_ETH = 1*10**18
class UniswapMarketMakerKeeper:
    """Keeper acting as a market maker on Uniswap.

    Adding or removing liquidity"""

    logger = logging.getLogger()

    def __init__(self, args: list, **kwargs):
        parser = argparse.ArgumentParser(prog='uniswap-market-maker-keeper')

        parser.add_argument("--rpc-host", type=str, default="localhost",
                            help="JSON-RPC host (default: `localhost')")

        parser.add_argument("--rpc-port", type=int, default=8545,
                            help="JSON-RPC port (default: `8545')")

        parser.add_argument("--rpc-timeout", type=int, default=10,
                            help="JSON-RPC timeout (in seconds, default: 10)")

        parser.add_argument("--eth-from", type=str, required=True,
                            help="Ethereum account from which to send transactions")

        parser.add_argument("--eth-key", type=str, nargs='*',
                            help="Ethereum private key(s) to use (e.g. 'key_file=aaa.json,pass_file=aaa.pass')")

        parser.add_argument("--token-address", type=str, required=True,
                            help="Ethereum address of the token")

        parser.add_argument("--uniswap-feed", type=str,
                            help="Source of liquidity feed")

        parser.add_argument("--uniswap-feed-expiry", type=int, default=86400,
                            help="Maximum age of the liquidity feed (in seconds, default: 86400)")

        parser.add_argument("--percentage-difference", type=float, default=1,
                            help="Percentage difference between Uniswap exchange rate and aggregated price"
                                 "(default: 1)")

        parser.add_argument("--debug", dest='debug', action='store_true',
                            help="Enable debug output")

        self.arguments = parser.parse_args(args)
        setup_logging(self.arguments)

        self.web3 = kwargs['web3'] if 'web3' in kwargs else Web3(HTTPProvider(endpoint_uri="https://parity0.mainnet.makerfoundation.com:8545",
                                                                              request_kwargs={"timeout": self.arguments.rpc_timeout}))
        self.web3.eth.defaultAccount = self.arguments.eth_from
        register_keys(self.web3, self.arguments.eth_key)

        private_key = self._extract_private_key(self.arguments.eth_key)
        self.uniswap = UniswapWrapper(self.arguments.eth_from,
                                      private_key,
                                      web3=self.web3)

        self.token = self.arguments.token_address

        if self.arguments.uniswap_feed:
            web_socket_feed = WebSocketFeed(self.arguments.uniswap_feed, 5)
            expiring_web_socket_feed = ExpiringFeed(web_socket_feed, self.arguments.uniswap_feed_expiry)

            self.feed = expiring_web_socket_feed

        self.erc20_token = ERC20Token(web3=self.web3, address=Address(self.token))

    def main(self):
        with Lifecycle(self.web3) as lifecycle:
            lifecycle.initial_delay(5)
            lifecycle.every(1, self.place_liquidity)

    def place_liquidity(self):

        feed_price = self.feed.get()[0]['price']
        uniswap_price = self.uniswap.get_exchange_rate(self.token)

        diff = feed_price * self.arguments.percentage_difference / 100
        self.logger.info(f"Feed price: {feed_price} Uniswap price: {uniswap_price} Diff: {diff}")

        add_liquidity = diff > abs(feed_price - uniswap_price)
        remove_liquidity = diff < abs(feed_price - uniswap_price)

        exch_contract = self.uniswap.exchange_contract[self.token]
        token_balance = self.erc20_token.balance_of(Address(self.web3.eth.defaultAccount)).value/ONE_ETH
        eth_balance = self.web3.eth.getBalance(self.web3.eth.defaultAccount)/ONE_ETH

        self.logger.info(f"ETH balance: {eth_balance}; DAI balance: {token_balance}")

        if add_liquidity:
            eth_balance_no_gas = eth_balance - 0.02
            liquidity_to_add = eth_balance_no_gas

            self.logger.info(f"Wallet liquidity {liquidity_to_add}")
            self.logger.info(f"Calculated liquidity {token_balance / uniswap_price}")
            eth_amount_to_add = min(liquidity_to_add, (token_balance*95/100)/ uniswap_price)

            current_liquidity_tokens = exch_contract.functions.balanceOf(self.web3.eth.defaultAccount).call()
            self.logger.info(f"Current liquidity tokens before adding {current_liquidity_tokens}")

            if liquidity_to_add > 0 and current_liquidity_tokens == 0:
                self.logger.info(f"{self.token} add liquidity of {eth_amount_to_add} ETH amount")
                response = self.uniswap.add_liquidity(self.token,
                                                      int(eth_amount_to_add * ONE_ETH),
                                                      int(0.5 * eth_amount_to_add * ONE_ETH))
                self.logger.info(f"Adding liquidity with transaction {response.hex()} ...")
                tx = self.web3.eth.waitForTransactionReceipt(response, timeout=6000)
                if tx.status:
                    self.logger.info(f"Successfully added {eth_amount_to_add} liquidity of {self.token}")
                else:
                    self.logger.warning(f"Failed to add {eth_amount_to_add} liquidity of {self.token}")
            else:
                self.logger.info(f"Not enough tokens")
            self.logger.info(f"Current liquidity tokens after adding {exch_contract.functions.balanceOf(self.web3.eth.defaultAccount).call()}")

        if remove_liquidity:

            liquidity_to_remove = exch_contract.functions.balanceOf(self.web3.eth.defaultAccount).call()
            self.logger.info(f"Current liquidity tokens before removing {liquidity_to_remove}")

            if liquidity_to_remove > 0:
                self.logger.info(f"Removing {liquidity_to_remove} from Uniswap pool")
                response = self.uniswap.remove_liquidity(self.token, liquidity_to_remove)
                self.logger.info(f"Removed liquidity with transaction {response.hex()}")
                tx = self.web3.eth.waitForTransactionReceipt(response, timeout=6000)
                if tx.status:
                    self.logger.info(f"Successfully removed {liquidity_to_remove} liquidity of {self.token}")
                else:
                    self.logger.warning(f"Failed to remove {liquidity_to_remove} liquidity of {self.token}")
            else:
                self.logger.info(f"No liquidity to remove")

            self.logger.info(f"Current liquidity tokens after removing {exch_contract.functions.balanceOf(self.web3.eth.defaultAccount).call()}")

    def _extract_private_key(self, key: str):
        parsed = {}
        for p in key[0].split(","):
            var, val = p.split("=")
            parsed[var] = val

        key_file = parsed.get('key_file')
        pass_file = parsed.get('pass_file')

        with open(key_file) as key_file_open:
            read_key = key_file_open.read()
            if pass_file:
                with open(pass_file) as pass_file_open:
                    read_pass = pass_file_open.read().replace("\n", "")
            else:
                read_pass = getpass.getpass(prompt=f"Password for {key_file}: ")

        return Account.decrypt(read_key, read_pass).hex()


if __name__ == '__main__':
    UniswapMarketMakerKeeper(sys.argv[1:]).main()
