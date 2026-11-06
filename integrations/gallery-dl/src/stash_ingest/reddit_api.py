"""Opt-in REST-first JSON requests with a narrowly scoped OAuth fallback.

Only Reddit's explicit HTTP 403 network-security page selects the alternative.
Ordinary account errors, 429s, timeouts and broken responses retain their normal
handling. The pinned upstream API still owns authentication and JSON parsing.
"""

from contextvars import ContextVar
import re
import time
from urllib.parse import urlsplit

from .encoding import InvalidData

REST = 'https://www.reddit.com'
RECHECK_SECONDS = 300
_attempt = ContextVar('reddit_rest_fallback_attempt', default=None)


def route(endpoint):
    if re.fullmatch(r'(?:/r/[A-Za-z0-9_+]+)?/search/\.json', endpoint):
        return 'search'
    if re.fullmatch(r'/comments/[a-z0-9]+/\.json', endpoint):
        return 'post'
    if re.fullmatch(r'/user/[A-Za-z0-9_-]+/about\.json', endpoint):
        return 'profile'
    if re.fullmatch(r'/user/[A-Za-z0-9_-]+(?:/(?:submitted|comments))?/\.json', endpoint):
        return 'user-listing'
    if re.fullmatch(r'/r/[A-Za-z0-9_+]+(?:/(?:new|hot|top|controversial|rising))?/\.json', endpoint):
        return 'subreddit-listing'
    return None  # Private saved/voted feeds and unrecognized APIs stay unchanged.


def can_fallback(response, url=None):
    """Called within this exact REST attempt by request guards and accounting."""
    expected = _attempt.get()
    if expected is None or response is None or response.status_code != 403:
        return False
    value = url if url is not None else getattr(response, 'url', None)
    if not isinstance(value, str):
        return False
    try:
        # A redirected response must still identify the same API endpoint.
        for candidate in (value, getattr(response, 'url', value)):
            if not isinstance(candidate, str):
                return False
            parsed = urlsplit(candidate)
            if (parsed.scheme != 'https' or parsed.hostname != 'www.reddit.com'
                    or parsed.port not in (None, 443) or parsed.username is not None
                    or parsed.password is not None or parsed.fragment or parsed.path != expected):
                return False
    except ValueError:
        return False
    from .request_budget import network_security
    return network_security(response, 'service:reddit')


class RestBlocked(Exception):
    pass


class OAuthExtractor:
    """Share this extractor's guarded requests without changing its settings."""
    def __init__(self, target):
        self.target = target

    def __getattr__(self, name):
        return getattr(self.target, name)

    def config(self, key, default=None):
        return 'oauth' if key == 'api' else self.target.config(key, default)


def install_api(target):
    """Run after RedditAPI creation and before the durable pagination wrapper."""
    mode = target.config('native-api-fallback', 'none')
    if mode == 'none':
        return
    if mode != 'oauth':
        raise InvalidData('Unsupported Reddit native-api-fallback setting')
    api = target.api
    if api.root != REST or getattr(api, '_native_rest_fallback', False):
        return
    if target.config('native-search-api', 'api') != 'api':
        raise InvalidData('Reddit OAuth fallback requires the JSON search transport')
    if not target.config('client-id') or not target.config('refresh-token'):
        raise InvalidData('Reddit OAuth fallback requires configured client-id and refresh-token')
    api._native_rest_fallback = True
    original_call, original_request = api._call, target.request
    unavailable = {}
    oauth = None

    def request(url, *args, **kwargs):
        response = original_request(url, *args, **kwargs)
        if can_fallback(response, url):
            response.close()
            raise RestBlocked()
        return response

    def call(endpoint, params):
        nonlocal oauth
        family = route(endpoint)
        if family is None:
            return original_call(endpoint, params)
        if time.monotonic() >= unavailable.get(family, 0):
            token = _attempt.set(endpoint)
            try:
                return original_call(endpoint, params)
            except RestBlocked:
                unavailable[family] = time.monotonic() + RECHECK_SECONDS
                target.log.info('Reddit REST %s blocked by network security; using OAuth JSON for this route', family)
            finally:
                _attempt.reset(token)
        if oauth is None:
            from gallery_dl.extractor.reddit import RedditAPI
            oauth = RedditAPI(OAuthExtractor(target))
        # Reuse exactly the same query, sort, time window and after cursor.
        # The caller's one durable page-start checkpoint already precedes both.
        return oauth._call(endpoint, params)

    target.request, api._call = request, call


def install(target):
    """Metadata-only post extractors do not have the pagination installer."""
    if target.config('native-api-fallback', 'none') == 'none':
        return
    original = target.submissions

    def submissions():
        install_api(target)
        return original()

    target.submissions = submissions
