import datetime

from elasticsearch import Elasticsearch
import time

class SimpleArbitrageStrategy:
    def __init__(self):
        self.es = Elasticsearch([{"host":"10.0.0.55"}], http_auth=("cryptobot", "kukuriku99"))

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
        #print(result)
        for hit in result['hits']['hits']:
            exchange_name = hit['_source']['exchange']
            if not table.get(exchange_name):
                table[exchange_name] = {
                    "ask.price": hit['_source']['ask.price'],
                    "bid.price": hit['_source']['bid.price'],
                    "ask.volume": hit['_source']['ask.volume'],
                    "bid.volume": hit['_source']['bid.volume'],
                    "fee": hit['_source']['fee.percent']
                }
        return table

    def create_comparison_table(self):
        table = {}
        pairs = self.get_pairs()
        for pair in pairs:
            pair_table = self.get_comparison_table_for_pair(pair)
            table[pair] = pair_table
        return table

    def find_arbitrage(self):
        try:
            table = self.create_comparison_table()

            for pair in table:
                exchanges = table[pair]
                for exchange in exchanges:
                    for compare_exchange in exchanges:
                        if compare_exchange == exchange:
                            continue
                        exch_a = exchanges[exchange]
                        exch_b = exchanges[compare_exchange]
                        if exch_a['ask.price'] < exch_b['bid.price']:
                            amount_to_buy = min(exch_a['ask.volume'], exch_b['bid.volume'])
                            self.record_arbitrage(pair, exchange, exch_a['ask.price'], exch_a['fee'],  compare_exchange,
                                                  exch_b['bid.price'], exch_b['fee'], amount_to_buy)
                        if exch_b['ask.price'] < exch_a['bid.price']:
                            amount_to_buy = min(exch_b['ask.volume'], exch_a['bid.volume'])
                            self.record_arbitrage(pair, compare_exchange, exch_b['ask.price'], exch_b['fee'], exchange,
                                                  exch_a['bid.price'], exch_a['fee'], amount_to_buy)
        except Exception as e:
            print(e)
            time.sleep(2)

    def record_arbitrage(self, pair, buy_exchange, buy_price, buy_fee, sell_exchange, sell_price, sell_fee, amount):
        bought_price = amount * buy_price * (1 - (buy_fee if buy_fee is not None else 0))
        sold_price = amount * sell_price * (1 - (sell_fee if sell_fee is not None else 0))
        profit_percent = sold_price / bought_price - 1
        if profit_percent <=0:
            return
        now = datetime.datetime.utcnow()
        doc = {
            "timestamp": now,
            "pair": pair,
            "strategy": "arbitrage",
            "buy.exchange": buy_exchange,
            "buy.price": buy_price,
            "sell.exchange": sell_exchange,
            "sell.price": sell_price,
            "profit.amount": amount * (sell_price - buy_price),
            "profit.percent": profit_percent
        }
        print(doc)
        self.es.index(index="crypto-strategy", id=str(now.timestamp()) + "arbitrage" + pair, document=doc)

    def run(self):
        while True:
            self.find_arbitrage()
            time.sleep(3)
            #print('.')

if __name__ == "__main__":
    strategy = SimpleArbitrageStrategy()
    res = strategy.get_pairs()
    strategy.run()
    print(res)