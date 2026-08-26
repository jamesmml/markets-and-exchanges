"""
Contains the functions used in the L2 Order Book Exploration.

This file can be used either as a standalone script or its functions can be imported and used elsewhere.

Author: James Milgram
"""

# Order book parsing 
import websockets
import asyncio
import json

# Data analysis/computation
import pandas as pd
from decimal import Decimal

def compute_stats(currency: str, order_book_timestamp: str, order_book: list) -> dict:
    """
    Computes various order book statistics including the spread, mid price, and order book imbalance.

    Args:
        currency : The selected cryptocurrency
        order_book_timestamp : The order matching engine's timestamp
        order_book : A list constituting the pre-aggregated L2 order book

    Returns:
        stats : A dict containing the computed statistics
    """
    # Obtaining the top 5 bids and top 5 asks
    # Coinbase response already sorts bids from highest to lowest and asks from lowest to highest
    top_bids = []
    top_asks = []
    for order in order_book:
        # Bids: what prices are people offering to buy a cryptocurrency in USD?
        if order["side"] == "bid" and len(top_bids) < 5:
            top_bids.append(order)
        # Asks: what prices are people asking for to sell a cryptocurrency in USD?
        elif order["side"] == "offer" and len(top_asks) < 5:
            top_asks.append(order)

    best_bid = Decimal(top_bids[0]["price_level"])
    best_ask = Decimal(top_asks[0]["price_level"])

    # Spread: the price difference between the lowest sell (ask) price and the highest buy (bid) price
    spread = best_ask - best_bid

    # Mid price: signals the "equilibrium price" where buyers and sellers meet
    mid_price = (best_ask + best_bid) / 2

    bid_volume = sum([Decimal(order["new_quantity"]) for order in top_bids])
    ask_volume = sum([Decimal(order["new_quantity"]) for order in top_asks])
    total_volume = bid_volume + ask_volume

    # Order book imbalance: a fraction that gives insight into how a market "feels" about an asset
    # A positive OBI means that orders are buying more than selling where a negative OBI means that orders are selling more than buying
    obi = (bid_volume - ask_volume) / total_volume

    stats = {
        "currency": currency,
        "order_book_timestamp": order_book_timestamp,
        "best_bid": str(best_bid),
        "best_ask": str(best_ask),
        "spread": str(spread),
        "mid_price": str(mid_price),
        "obi": str(obi)
    }

    return stats

def update_order_book(order_book_update: list, local_order_book: list) -> None:
    """
    Updates a local order book in place to reflect new order information.

    Args:
        order_book_update : An order book update
        local_order_book : The current order book
    """
    # Loop through all updates and amend orders in the local book as needed
    for update in order_book_update:
        order_found = False
        side = update["side"]
        price_level = Decimal(update["price_level"])
        new_quantity = update["new_quantity"]

        while (not order_found):
            order_book = local_order_book

            # Logic for inserting new orders differs between bids and asks
            if side == "bid":
                for idx, order in enumerate(order_book):
                    order_price = Decimal(order["price_level"])
                    if order["side"] != "bid" or order_price > price_level:
                        continue
                    elif order_price == price_level:
                        order_found = True
                        if new_quantity == "0":
                            local_order_book.pop(idx)
                        else:
                            local_order_book[idx] = update

                        break
                    # Using elif to be explicit: add a new order to the local book
                    elif order_price < price_level:
                        order_found = True
                        local_order_book.insert(idx, update)
                    else:
                        raise ValueError(f"Unexpected behavior: current order - {order}\n" + \
                                f"current update - {update}")
            else:
                for idx, order in enumerate(order_book):
                    order_price = Decimal(order["price_level"])
                    if order["side"] != "offer" or order_price < price_level:
                        continue
                    elif order_price == price_level:
                        order_found = True
                        if new_quantity == "0":
                            local_order_book.pop(idx)
                        else:
                            local_order_book[idx] = update

                        break
                    # Using elif to be explicit: add a new order to the local book
                    elif order_price > price_level:
                        order_found = True
                        local_order_book.insert(idx, update)
                    else:
                        raise ValueError(f"Unexpected behavior: current order - {order}\n" + \
                                f"current update - {update}")

async def parse_order_books(url: str, message: dict, currencies: list) -> None:
    """
    Function for parsing L2 order book data of various cryptocurrencies.

    Args:
        url : The URL to Coinbase's Advance Trade WebSocket
        message : A desired subscription message
        currencies : A list of the desired cryptocurrencies
    """
    DESIRED_CHANNEL = "l2_data"

    # Creation of local order books
    local_order_books = {currency:[] for currency in currencies}

    # DataFrame for tracking order book evolution
    stats_df = pd.DataFrame()

    try:
        # Connect to Coinbase server
        ws = await websockets.connect(url, max_size = 10*1024*1024)

        # Send subscription message to receive order book data for the requested currencies
        await ws.send(json.dumps(message))

        # Loop for processing order book data
        while True:
            response = json.loads(await ws.recv())
            response_contents = response["events"][0]
            response_type = response_contents["type"]

            # Ignore responses that don't contain order book data
            if response["channel"] != DESIRED_CHANNEL:
                continue

            # Access order book according to JSON structure
            currency = response_contents["product_id"]
            order_book = response_contents["updates"]
            order_book_timestamp = order_book[0]["event_time"]

            # Construct local order book from initial snapshot
            if response_type == "snapshot":
                local_order_books[currency] = order_book
            # Otherwise, update local order book
            elif response_type == "update":
                update_order_book(order_book_update = order_book, local_order_book = local_order_books[currency])
            else:
                raise ValueError(f"Unexpected response type: {response_type}")

            stats = compute_stats(currency = currency, order_book_timestamp = order_book_timestamp, order_book = local_order_books[currency])

            with open(f"{currency}_{order_book_timestamp}.json", "w", encoding = "utf-8") as f:
                json.dump(stats, f, indent = 2)

    finally:
        # Close server connection
        print("Closing connection...", end = "\r")
        await ws.close()
        print("\x1b[2K" + "Connection closed!")

if __name__ == "__main__":
    # Coinbase WebSocket URL
    coinbase_url = "wss://advanced-trade-ws.coinbase.com"

    # WebSocket message for BTC, ETH, XRP
    currencies = ["BTC-USD", "ETH-USD", "XRP-USD"]

    message = {
        "type": "subscribe",
        "product_ids": currencies,
        "channel": "level2"
    }

    asyncio.run(parse_order_books(url = coinbase_url, message = message, currencies = currencies))