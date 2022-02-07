import datetime

from elasticsearch import Elasticsearch
import time
import ccxt


class SimpleArbitrageStrategy:
    def __init__(self):
        self.es = Elasticsearch([{"host": "10.0.0.55"}], http_auth=("cryptobot", "kukuriku99"))
        self.exchange_info = {}

    def get_pairs(self):
        body = {
            "size": 0,
            "aggs": {
                "by pair": {
                    "terms": {
                        "field": "pair.keyword",
                        "size": 1000
                    }
                }
            }
        }
        result = self.es.search(index="crypto-info", body=body)
        buckets = result['aggregations']['by pair']['buckets']
        return [b['key'] for b in buckets]

    def get_comparison_table_for_pair(self, pair):
        table = {}
        query = {
            "bool": {
                "must": [
                    {
                        "match": {
                            "pair.keyword": pair
                        }
                    }
                ],
                "filter": [
                    {
                        "range": {
                            "timestamp": {
                                "gte": "now-5m"
                            }
                        }
                    }
                ]
            }
        }
        result = self.es.search(index="crypto-info", query=query, size=500)

        for hit in result['hits']['hits']:
            exchange_name = hit['_source']['exchange.name']
            if not table.get(exchange_name):
                table[exchange_name] = {
                    "ask.price": hit['_source']['ask.price'],
                    "bid.price": hit['_source']['bid.price'],
                    "ask.volume": hit['_source']['ask.volume'],
                    "bid.volume": hit['_source']['bid.volume'],
                    "taker.fee": hit['_source']['fee.percent'],
                    "exchange.name": exchange_name,
                    "exchange.id": hit['_source']['exchange.id']
                }
        return table

    def create_comparison_table(self):
        table = {}
        pairs = self.get_pairs()
        for pair in pairs:
            pair_table = self.get_comparison_table_for_pair(pair)
            table[pair] = pair_table
        return table

    def get_transfer_fees(self, exchange, coin):
        try:
            if exchange not in self.exchange_info:
                exchange_class = getattr(ccxt, exchange)
                self.exchange_info[exchange] = {
                    "coins": {},
                    "client": exchange_class()
                }
            if coin not in self.exchange_info[exchange]['coins']:
                client = self.exchange_info[exchange]['client']
                currencies = client.fetch_currencies()
                if currencies is not None and coin in currencies and 'info' in currencies[coin]:
                    self.exchange_info[exchange]['coins'][coin] = client.fetch_currencies()[coin]['info']
                else:
                    self.exchange_info[exchange]['coins'][coin] = []

            coin_info = self.exchange_info[exchange]['coins'][coin]
            if isinstance(coin_info, list) and len(coin_info) > 0 and 'fee' in coin_info[0]:
                return coin_info[0]['fee']
            else:
                return 0
        except Exception as e:
            print(e)
            return 0

    def get_arbitrage(self, exchange_a, exchange_b, pair):
        max_bid_a, bid_volume_a, min_ask_a, ask_volume_a = exchange_a['bid.price'], exchange_a['bid.volume'], \
                                                           exchange_a['ask.price'], exchange_a['ask.volume']
        max_bid_b, bid_volume_b, min_ask_b, ask_volume_b = exchange_b['bid.price'], exchange_b['bid.volume'], \
                                                           exchange_b['ask.price'], exchange_b['ask.volume']
        [coin, base] = pair.split('/')
        transaction_fee_percent_a = exchange_a['taker.fee']
        transaction_fee_percent_b = exchange_b['taker.fee']

        if min_ask_a < max_bid_b:
            volume = min(ask_volume_a, bid_volume_b)
            coin_transfer_fee = self.get_transfer_fees(exchange_a['exchange.id'], coin)
            base_transfer_fee = self.get_transfer_fees(exchange_b['exchange.id'], base)
            try:
                base_left = volume * (max_bid_b * (1 - transaction_fee_percent_a) -
                                  min_ask_a * (1 + transaction_fee_percent_b)) - float(base_transfer_fee) - \
                        (float(coin_transfer_fee) * max_bid_a)
            except Exception as e:
                print(e)
                base_left = -1

            profit = 100 * base_left / (volume * min_ask_a)
            return {
                "profit.percent": profit,
                "base.left": base_left,
                "buy.price": min_ask_a,
                "buy.exchange": exchange_a['exchange.name'],
                "buy.transaction.fee": transaction_fee_percent_a,
                "sell.price": max_bid_b,
                "sell.exchange": exchange_b['exchange.name'],
                "sell.transaction.fee": transaction_fee_percent_b,
                "volume": volume,
                "coin.transfer.fee": coin_transfer_fee,
                "base.transfer.fee": base_transfer_fee,
                "pair": pair
            }
        elif min_ask_b < max_bid_a:
            volume = min(ask_volume_b, bid_volume_a)
            coin_transfer_fee = self.get_transfer_fees(exchange_b['exchange.id'], coin)
            base_transfer_fee = self.get_transfer_fees(exchange_a['exchange.id'], base)
            try:
                base_left = volume * (max_bid_a * (1 - transaction_fee_percent_a) -
                                      min_ask_b * (1 + transaction_fee_percent_b)) - float(base_transfer_fee) - \
                            (float(coin_transfer_fee) * max_bid_b)
            except Exception as e:
                print(e)
                base_left = -1

            profit = 100 * base_left / (volume * min_ask_b)
            return {
                "profit.percent": profit,
                "base.left": base_left,
                "buy.price": min_ask_b,
                "buy.exchange": exchange_b['exchange.name'],
                "buy.transaction.fee": transaction_fee_percent_b,
                "sell.price": max_bid_a,
                "sell.exchange": exchange_a['exchange.name'],
                "sell.transaction.fee": transaction_fee_percent_a,
                "volume": volume,
                "coin.transfer.fee": coin_transfer_fee,
                "base.transfer.fee": base_transfer_fee,
                "pair": pair
            }

        return None

    def find_arbitrage(self):
        try:
            table = self.create_comparison_table()

            for pair in table:
                exchanges = table[pair]
                for exchange in exchanges:
                    for compare_exchange in exchanges:
                        if compare_exchange == exchange:
                            continue
                        arbitrage = self.get_arbitrage(exchanges[exchange], exchanges[compare_exchange], pair)
                        if arbitrage is not None:
                            self.record_arbitrage(arbitrage)

        except Exception as e:
            print(e)
            time.sleep(2)

    def record_arbitrage(self, arbitrage):
        pair = arbitrage['pair']
        now = datetime.datetime.utcnow()
        doc = {
            "timestamp": now,
            "pair": pair,
            "strategy": "arbitrage",
            "buy.exchange": arbitrage['buy.exchange'],
            "buy.price": arbitrage['buy.price'],
            "sell.exchange": arbitrage['sell.exchange'],
            "sell.price": arbitrage['sell.price'],
            "profit.amount": arbitrage['base.left'],
            "profit.percent": arbitrage['profit.percent'],
            "buy.transaction.fee": arbitrage["buy.transaction.fee"],
            "sell.transaction.fee": arbitrage["sell.transaction.fee"],
            "volume": arbitrage['volume'],
            "coin.transfer.fee": arbitrage["coin.transfer.fee"],
            "base.transfer.fee": arbitrage["base.transfer.fee"]
        }
        print(doc)
        try:
            self.es.index(index="crypto-strategy", id=str(now.timestamp()) + "arbitrage" + pair, document=doc)
        except Exception as e:
            print(e)

    def run(self):
        while True:
            self.find_arbitrage()
            time.sleep(2)


if __name__ == "__main__":
    strategy = SimpleArbitrageStrategy()
    res = strategy.get_pairs()
    strategy.run()
    print(res)
