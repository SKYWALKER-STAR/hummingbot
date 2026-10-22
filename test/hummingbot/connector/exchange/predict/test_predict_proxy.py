import unittest
from unittest.mock import AsyncMock, Mock, patch

from core_loader import load

module = load('predict_client')


class ProxyTests(unittest.IsolatedAsyncioTestCase):
    async def test_rest_and_ws_use_same_proxy(self):
        client = module.PredictClient(proxy_url='http://127.0.0.1:7890')
        response = Mock(status=200, json=AsyncMock(return_value={'success': True}))
        ctx = Mock(__aenter__=AsyncMock(return_value=response), __aexit__=AsyncMock(return_value=False))
        client._session = Mock(request=Mock(return_value=ctx), ws_connect=AsyncMock())
        await client._send('GET', '/v1/markets')
        await client.ws_connect('wss://ws.predict.fun/ws', headers={})
        self.assertEqual(client.proxy_url, client._session.request.call_args.kwargs['proxy'])
        self.assertEqual(client.proxy_url, client._session.ws_connect.call_args.kwargs['proxy'])

    def test_env_explicit_override_and_direct(self):
        with patch.dict('os.environ', {'PREDICT_PROXY_URL': 'http://127.0.0.1:7890'}):
            self.assertEqual('http://127.0.0.1:7890', module.PredictClient().proxy_url)
            self.assertEqual('', module.PredictClient(proxy_url='').proxy_url)

    def test_reject_bad_urls_without_echoing_credentials(self):
        for url in ('socks5://secret:password@localhost:123', 'http://localhost:bad', 'http://', 'http://localhost/?secret'):
            with self.assertRaises(ValueError) as error:
                module.PredictClient(proxy_url=url)
            self.assertNotIn('secret', str(error.exception))

    def test_proxy_credentials_redacted(self):
        client = module.PredictClient(proxy_url='http://alice:secret%21@localhost:7890')
        result = client._safe_diagnostics({'message': 'alice secret!'}, {})
        self.assertNotIn('alice', result['message'])
        self.assertNotIn('secret!', result['message'])
