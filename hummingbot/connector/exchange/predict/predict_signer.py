"""SDK adapter. No approvals or transactions are sent during initialization."""
from datetime import datetime, timezone
from decimal import Decimal
import time


def is_rpc_limit_error(exc):
    response = getattr(exc, "rpc_response", None)
    if isinstance(response, dict):
        error = response.get("error", response)
        if isinstance(error, dict) and error.get("code") == -32005:
            return True
    return "-32005" in str(exc) and "limit" in str(exc).lower()


def fill_log_chunks(event, start, head, order_hash):
    """Retry the same range on limits; never skip unverified blocks."""
    size, failures = 2000, 0
    while start <= head:
        end = min(head, start + size - 1)
        try:
            logs = event.get_logs(from_block=start, to_block=end,
                                  argument_filters={"orderHash": order_hash})
        except Exception as exc:
            if not is_rpc_limit_error(exc) or failures >= 11:
                raise
            time.sleep(min(2 ** failures, 8))
            size = max(1, (end - start + 1) // 2)
            failures += 1
            continue
        yield logs
        start = end + 1
        failures = 0


class PredictSigner:
    def __init__(self, private_key, domain="predict", predict_account=""):
        from eth_account import Account
        from predict_sdk import ChainId, OrderBuilder, OrderBuilderOptions

        self.account = Account.from_key(private_key)
        self.address = predict_account or self.account.address
        self.predict_account = predict_account
        self.builder = OrderBuilder.make(
            ChainId.BNB_MAINNET if domain == "predict" else ChainId.BNB_TESTNET,
            private_key, OrderBuilderOptions(predict_account=predict_account) if predict_account else None)

    def auth_message(self, message):
        from eth_account.messages import encode_defunct

        if self.predict_account:
            signature = self.builder.sign_predict_account_message(message)
        else:
            signature = "0x" + self.account.sign_message(encode_defunct(text=message)).signature.hex().removeprefix("0x")
        return {"signer": self.address, "message": message, "signature": signature}

    def sign_order(self, market, outcome, side, quantity, price, expiration, post_only):
        from predict_sdk import BuildOrderInput, LimitHelperInput, Side

        amounts = self.builder.get_limit_order_amounts(LimitHelperInput(
            side=Side.BUY if side == "BUY" else Side.SELL,
            price_per_share_wei=int(price * 10**18), quantity_wei=int(quantity * 10**18)))
        order = self.builder.build_order("LIMIT", BuildOrderInput(
            side=Side.BUY if side == "BUY" else Side.SELL, token_id=outcome.token_id,
            maker_amount=amounts.maker_amount, taker_amount=amounts.taker_amount,
            fee_rate_bps=market.fee_rate_bps, maker=self.address, signer=self.address,
            expires_at=datetime.fromtimestamp(expiration, tz=timezone.utc)))
        typed = self.builder.build_typed_data(order, is_neg_risk=market.is_neg_risk,
                                             is_yield_bearing=market.is_yield_bearing)
        signed = self.builder.sign_typed_data_order(typed)
        order_hash = signed.hash or self.builder.build_typed_data_hash(typed)
        api_order = {"hash": order_hash, "salt": str(order.salt), "maker": order.maker,
                     "signer": order.signer, "taker": order.taker, "tokenId": str(order.token_id),
                     "makerAmount": str(order.maker_amount), "takerAmount": str(order.taker_amount),
                     "expiration": int(order.expiration), "nonce": str(order.nonce),
                     "feeRateBps": str(order.fee_rate_bps), "side": int(order.side.value),
                     "signatureType": int(order.signature_type.value), "signature": signed.signature}
        return {"data": {"order": api_order, "pricePerShare": str(amounts.price_per_share),
                         "strategy": "LIMIT", "isPostOnly": post_only,
                         "selfTradePrevention": "CANCEL_TAKER"}}

    async def collateral_balance(self):
        import asyncio
        balance = await asyncio.to_thread(
            self.builder.contracts.usdt.functions.balanceOf(self.address).call)
        return Decimal(balance) / Decimal(10**18)

    def _contract(self, market, kind):
        prefix = "yield_bearing_" if market.is_yield_bearing else ""
        prefix += "neg_risk_" if market.is_neg_risk else ""
        return getattr(self.builder.contracts, prefix + kind)

    def shares_balance(self, market, outcome):
        contract = self._contract(market, "conditional_tokens")
        return Decimal(contract.functions.balanceOf(self.address, int(outcome.token_id)).call()) / Decimal(10**18)

    def confirmed_fills(self, market, outcome, order_hash, created_at):
        """Read exact fills by hash, including fills missed during WS downtime.

        Both WS notifications and polling use these same tx/log identities, avoiding
        double accounting. Two blocks are withheld from the chain head.
        """
        contract = self._contract(market, "ctf_exchange")
        w3 = contract.w3
        head = max(0, w3.eth.block_number - 2)
        low, high = 0, head
        target = max(0, int(created_at) - 60)
        while low < high:
            mid = (low + high) // 2
            if w3.eth.get_block(mid)["timestamp"] < target:
                low = mid + 1
            else:
                high = mid
        fills = []
        for events in fill_log_chunks(contract.events.OrderFilled(), low, head, order_hash):
            for event in events:
                args = event["args"]
                if args["maker"].lower() != self.address.lower():
                    raise ValueError("Fill maker mismatch")
                buying = int(args["makerAssetId"]) == 0
                token = args["takerAssetId"] if buying else args["makerAssetId"]
                if str(token) != outcome.token_id:
                    raise ValueError("Fill token mismatch")
                base = Decimal(args["takerAmountFilled"] if buying else args["makerAmountFilled"]) / Decimal(10**18)
                quote = Decimal(args["makerAmountFilled"] if buying else args["takerAmountFilled"]) / Decimal(10**18)
                if base <= 0:
                    raise ValueError("Invalid on-chain fill amount")
                fills.append({"id": f'{event["transactionHash"].hex()}:{event["logIndex"]}',
                              "timestamp": w3.eth.get_block(event["blockNumber"])["timestamp"],
                              "base": base, "quote": quote, "price": quote / base,
                              "fee": Decimal(args["fee"]) / Decimal(10**18),
                              "fee_asset": outcome.base_asset if buying else "USDT"})
        return fills
