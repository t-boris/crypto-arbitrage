from elasticsearch import Elasticsearch
import asyncio
import time

class SimpleArbitrageStrategy:
    def __init__(self):
        self.es = Elasticsearch(http_auth=("cryptobot", "kukuriku99"))

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
                    "bid.volume": hit['_source']['bid.volume']
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
        table = self.create_comparison_table()
        for pair in table:
            exchanges = table[pair]
            max_ask = 0
            ask_volume = 0
            buy_on = ""
            min_bid = 10000000
            bid_volume = 0
            sell_on = ""
            for exchange in exchanges:
                if exchanges[exchange]['ask.price'] > max_ask:
                    max_ask = exchanges[exchange]['ask.price']
                    ask_volume = exchanges[exchange]['ask.volume']
                    sell_on = exchange
                if exchanges[exchange]['bid.price'] < min_bid:
                    min_bid = exchanges[exchange]['bid.price']
                    bid_volume = exchanges[exchange]['bid.volume']
                    buy_on = exchange
            if min_bid > max_ask > 0:
                print('found arbitrage')

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