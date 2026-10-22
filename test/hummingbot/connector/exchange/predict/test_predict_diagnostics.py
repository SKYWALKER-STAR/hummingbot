import json
import unittest
from unittest.mock import AsyncMock, Mock, patch

from core_loader import load

module = load('predict_client')


class DiagnosticsTests(unittest.IsolatedAsyncioTestCase):
    async def send_error(self, body, *, status=403, headers=None, data=None):
        client = module.PredictClient(api_key='secret-api-key')
        client._jwt = 'cached-jwt-secret'
        response = Mock(status=status, headers=headers or {'Content-Type': 'application/json'})
        response.content.read = AsyncMock(side_effect=[body, b''])
        response.json = AsyncMock(return_value=json.loads(body) if status < 400 else {})
        context = Mock()
        context.__aenter__ = AsyncMock(return_value=response)
        context.__aexit__ = AsyncMock(return_value=False)
        client._session = Mock(request=Mock(return_value=context))
        with self.assertLogs(module.logger, level='WARNING') as logs:
            with self.assertRaises(module.PredictAPIError) as error:
                await client._send('POST', '/v1/orders', token='active-jwt-secret', data=data)
        self.assertEqual(error.exception.status, status)
        return '\n'.join(logs.output) + str(error.exception)

    async def test_error_fields_and_trace_without_secrets(self):
        signature = '0x' + 'ab' * 65
        body = {'code': 'FORBIDDEN', 'message': 'Denied secret-api-key active-jwt-secret cached-jwt-secret ' + signature,
                'data': {'privateKey': 'do-not-print'}, 'error': {'code': 'ACCOUNT_RESTRICTED'}}
        logs = await self.send_error(json.dumps(body).encode(), headers={
            'Content-Type': 'application/json', 'X-Request-ID': 'trace-123', 'CF-Ray': 'ray-123',
            'Set-Cookie': 'never-print'}, data={'order': {'signature': signature}})
        for expected in ('FORBIDDEN', 'ACCOUNT_RESTRICTED', 'trace-123', 'ray-123', '403'):
            self.assertIn(expected, logs)
        for secret in ('secret-api-key', 'active-jwt-secret', 'cached-jwt-secret', signature,
                       'do-not-print', 'never-print'):
            self.assertNotIn(secret, logs)

    async def test_html_and_oversized_bodies_not_logged(self):
        for body, kind in [(b'<html>secret-body</html>', 'non_json'), (b'x' * 16385, 'oversized')]:
            logs = await self.send_error(body)
            self.assertIn(kind, logs)
            self.assertNotIn('secret-body', logs)
            self.assertNotIn('x' * 100, logs)

    async def test_success_false_uses_same_diagnostics(self):
        logs = await self.send_error(b'{"success": false, "error": "Denied"}', status=200)
        self.assertIn('Denied', logs)

    async def test_response_read_failure_preserves_http_error(self):
        client = module.PredictClient()
        response = Mock(status=403, headers={})
        response.content.read = AsyncMock(side_effect=RuntimeError('secret-exception'))
        with self.assertLogs(module.logger, level='WARNING') as logs:
            await client._log_api_failure(response, 'POST', '/v1/orders')
        self.assertIn('unavailable', logs.output[0])
        self.assertNotIn('secret-exception', logs.output[0])

    def test_redaction_in_headers_and_log_injection(self):
        client = module.PredictClient(api_key='test-key')
        safe = client._safe_diagnostics({'message': 'denied\n\x1b[31m authorization=unknown-secret'},
                                        {'X-Request-ID': 'test-key'})
        self.assertNotIn('unknown-secret', safe['message'])
        self.assertNotIn('\n', safe['message'])
        self.assertNotIn('\x1b', safe['message'])
        self.assertEqual('[REDACTED]', safe['x-request-id'])

    async def test_failure_event_contains_diagnostics_even_with_logger_disabled(self):
        client = module.PredictClient(api_key='secret-api-key')
        response = Mock(status=400, headers={'X-Request-ID': 'trace-400'})
        response.content.read = AsyncMock(side_effect=[
            b'{"code":"INVALID_ORDER","message":"Denied secret-api-key"}', b''])
        ctx = Mock(__aenter__=AsyncMock(return_value=response), __aexit__=AsyncMock(return_value=False))
        client._session = Mock(request=Mock(return_value=ctx))
        with patch.object(module.logger, 'disabled', True):
            with self.assertRaises(module.PredictAPIError) as caught:
                await client._send('POST', '/v1/orders')
        # ExchangePyBase copies str(exception) into OrderUpdate.misc_updates.
        message = str(caught.exception)
        self.assertIn('diagnostics=', message)
        self.assertIn('INVALID_ORDER', message)
        self.assertIn('trace-400', message)
        self.assertNotIn('secret-api-key', message)

    async def test_403_does_not_retry(self):
        client = module.PredictClient()
        client.jwt = AsyncMock(return_value='token')
        client._send = AsyncMock(side_effect=module.PredictAPIError(403, '/v1/orders'))
        with self.assertRaises(module.PredictAPIError):
            await client.request('POST', '/v1/orders', private=True)
        self.assertEqual(1, client._send.await_count)
