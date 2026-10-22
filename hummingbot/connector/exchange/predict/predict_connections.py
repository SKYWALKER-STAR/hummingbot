"""Predict-only WebSocket routing; never change other connectors' sessions."""
from hummingbot.core.web_assistant.connections.ws_connection import WSConnection


class PredictConnectionsFactory:
    def __init__(self, client):
        self.client = client

    async def get_ws_connection(self):
        return WSConnection(aiohttp_client_session=self.client)

    async def close(self):
        await self.client.close()
