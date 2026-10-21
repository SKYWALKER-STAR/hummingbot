"""Read-only MarketID/name/book probe. Run from a compiled Hummingbot environment."""
import argparse
import asyncio
import os

from hummingbot.connector.exchange.predict.predict_exchange import PredictExchange


async def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("market_ids", nargs="+", type=int)
    args = parser.parse_args()
    connector = PredictExchange(predict_api_key=os.environ.get("PREDICT_API_KEY", ""),
                                market_ids=args.market_ids, trading_required=False)
    try:
        markets = await connector.initialize_markets()
        for market_id, outcomes in markets.items():
            market = connector.markets_by_id[market_id]
            data = await connector.fetch_book(market_id)
            print(f"Market {market_id}: {market.title}, status={market.trading_status}, tick={market.tick_size}")
            for outcome in outcomes:
                book = market.book(data, outcome)
                print(f"  {outcome.name!r}: {outcome.trading_pair}, token={outcome.token_id}")
                print(f"    best bid={book['bids'][:1]}, best ask={book['asks'][:1]}")
    finally:
        await connector.client.close()


if __name__ == "__main__":
    asyncio.run(main())
