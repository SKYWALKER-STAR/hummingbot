import asyncio

from hummingbot.core.data_type.user_stream_tracker_data_source import UserStreamTrackerDataSource
from hummingbot.core.web_assistant.connections.data_types import WSJSONRequest


class PredictAPIUserStreamDataSource(UserStreamTrackerDataSource):
    def __init__(self, connector, api_factory):
        super().__init__()
        self.connector = connector
        self.api_factory = api_factory
        self.connected = False

    async def listen_for_user_stream(self, output):
        delay = 1
        while True:
            try:
                token = await self.connector.client.jwt()
                ws = await self.api_factory.get_ws_assistant()
                self._ws_assistant = ws
                await ws.connect(ws_url=self.connector.ws_url, ws_headers=self.connector.client.headers,
                                 message_timeout=40)
                await ws.send(WSJSONRequest(payload={"method": "subscribe", "requestId": 1,
                                                     "params": [f"predictWalletEvents/{token}"]}))
                async for response in ws.iter_messages():
                    data = response.data
                    if data.get("type") == "R":
                        if not data.get("success"):
                            self.connector.client.invalidate_jwt()
                            raise IOError("Predict account subscription rejected")
                        self.connected = True
                        delay = 1
                        output.put_nowait({"type": "reconnected"})
                    elif data.get("topic") == "heartbeat":
                        await ws.send(WSJSONRequest(payload={"method": "heartbeat", "data": data["data"]}))
                        if await self.connector.client.jwt() != token:
                            break  # Reconnect with refreshed token; never log the topic.
                    elif data.get("type") == "M" and data.get("topic") == f"predictWalletEvents/{token}":
                        output.put_nowait(data["data"])
            except asyncio.CancelledError:
                raise
            except Exception:
                self.logger().warning("Predict account stream unavailable; reconnecting.")
            finally:
                self.connected = False
                self.connector.account_reconciled = False
                if self._ws_assistant:
                    await self._ws_assistant.disconnect()
                    self._ws_assistant = None
            await asyncio.sleep(delay)
            delay = min(30, delay * 2)
