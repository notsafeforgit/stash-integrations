"""Verify browser transport through gallery-dl's real yt-dlp download bridge."""

from http.cookiejar import Cookie
from io import BytesIO
import json
import logging
from types import SimpleNamespace
import unittest
from unittest.mock import patch
from urllib.parse import urlsplit
from urllib.request import Request

from gallery_dl import config, extractor
from gallery_dl.downloader.ytdl import YoutubeDLDownloader
from yt_dlp.networking import Response
from yt_dlp.networking._curlcffi import CurlCFFIRH


class YTDLTransportTests(unittest.TestCase):
    def setUp(self):
        config.clear()
        self.addCleanup(config.clear)
        config.set(('extractor', 'reddit'), 'api', 'rest')
        config.set(('extractor', 'reddit'), 'ytdl', {
            'cmdline-args': ['--impersonate', 'chrome'],
        })
        config.set(('downloader', 'ytdl'), 'raw-options', {
            'quiet': True, 'no_warnings': True, 'cachedir': False,
            'retries': 0, 'extractor_retries': 0,
        })
        config.set(('downloader', 'ytdl'), 'retries', 0)
        self.log = logging.getLogger('transport-fixture')
        self.log.addHandler(logging.NullHandler())
        self.addCleanup(self.log.handlers.clear)
        self.log.propagate = False

    def bridge(self, owner):
        job = SimpleNamespace(extractor=owner, out=None, get_logger=lambda _: self.log)
        return YoutubeDLDownloader(job)

    def test_reddit_bridge_uses_chrome_and_forwards_scoped_cookies(self):
        owner = extractor.find('https://www.reddit.com/comments/abc123/')
        owner.initialize()
        owner.session.cookies.set_cookie(Cookie(
            0, 'reddit_session', 'fixture-session', None, False, '.reddit.com',
            True, True, '/', True, True, None, True, None, None, {},
        ))
        bridge = self.bridge(owner)
        self.addCleanup(lambda: bridge.ytdl_instance and bridge.ytdl_instance.close())
        requests = []
        post = [{'data': {'children': [{'data': {
            'id': 'abc123', 'title': 'Fixture', 'author': 'fixture', 'over_18': False,
            'url': 'https://v.redd.it/nativevideo', 'is_video': True,
            'media': {'reddit_video': {
                'fallback_url': 'https://v.redd.it/nativevideo/DASH_720.mp4',
                'hls_url': 'https://v.redd.it/nativevideo/HLSPlaylist.m3u8',
                'dash_url': 'https://v.redd.it/nativevideo/DASHPlaylist.mpd',
                'duration': 1, 'width': 1280, 'height': 720,
            }},
        }}]}}]
        hls = ('#EXTM3U\n#EXT-X-STREAM-INF:BANDWIDTH=800000,RESOLUTION=1280x720\n'
               'https://v.redd.it/nativevideo/video.m3u8\n')
        dash = '''<MPD xmlns="urn:mpeg:dash:schema:mpd:2011" mediaPresentationDuration="PT1S">
          <Period><AdaptationSet mimeType="video/mp4"><Representation id="video"
            bandwidth="800000" codecs="avc1.64001f" width="1280" height="720">
            <BaseURL>https://v.redd.it/nativevideo/DASH_720.mp4</BaseURL>
          </Representation></AdaptationSet>
          <AdaptationSet mimeType="audio/mp4"><Representation id="audio"
            bandwidth="128000" codecs="mp4a.40.2">
            <BaseURL>https://v.redd.it/nativevideo/DASH_AUDIO_128.mp4</BaseURL>
          </Representation></AdaptationSet></Period></MPD>'''

        def response(handler, request):
            url = urlsplit(request.url)
            cookie_request = Request(request.url)
            handler.cookiejar.add_cookie_header(cookie_request)
            requests.append((url.hostname, cookie_request.get_header('Cookie', '')))
            if url.hostname == 'www.reddit.com':
                self.assertIn('/comments/abc123/', url.path)
                body, content_type = json.dumps(post), 'application/json'
            elif url.hostname == 'v.redd.it' and url.path.endswith('.m3u8'):
                body, content_type = hls, 'application/vnd.apple.mpegurl'
            elif url.hostname == 'v.redd.it' and url.path.endswith('.mpd'):
                body, content_type = dash, 'application/dash+xml'
            else:
                self.fail('Unexpected fixture request: ' + request.url)
            return Response(BytesIO(body.encode()), request.url, {'Content-Type': content_type})

        # Run the real Reddit extractor and manifest parsing, without network or
        # media writes. Patching only CurlCFFI also verifies handler selection.
        with patch.object(CurlCFFIRH, 'send', response), \
                patch.object(bridge, '_download_video', return_value=True) as download:
            self.assertTrue(bridge.download('ytdl:https://www.reddit.com/comments/abc123/',
                                            SimpleNamespace(kwdict={})))
        self.assertEqual(bridge.ytdl_instance.params['impersonate'].client, 'chrome')
        info = download.call_args.args[2]
        self.assertEqual((info['display_id'], info['id']), ('abc123', 'nativevideo'))
        self.assertTrue(any(f.get('acodec') not in (None, 'none') for f in info['formats']))
        self.assertTrue(any(f.get('vcodec') not in (None, 'none') for f in info['formats']))
        self.assertEqual([host for host, _ in requests], ['www.reddit.com', 'v.redd.it', 'v.redd.it'])
        self.assertIn('reddit_session=fixture-session', requests[0][1])
        self.assertTrue(all(not cookies for _, cookies in requests[1:]))

    def test_reddit_transport_does_not_apply_to_other_extractors(self):
        # DownloaderBase performs the same category lookup for every extractor.
        owner = SimpleNamespace(_cfgpath=('extractor', 'twitter', 'tweet'),
                                session=None, _proxies=None, _retries=0,
                                _timeout=10, _verify=True)
        bridge = self.bridge(owner)
        self.assertIsNone(bridge.config('cmdline-args'))


if __name__ == '__main__':
    unittest.main()
