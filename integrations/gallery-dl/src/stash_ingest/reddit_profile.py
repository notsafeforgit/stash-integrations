"""Reddit profile observations, independent of media selection and post windows.

Only the profile's social-link controls are inspected. Redirector requests use
an empty session; a Reddit cookie or Authorization header never leaves Reddit.
"""

import copy
import html
from html.parser import HTMLParser
import ipaddress
import json
import re
import types
from urllib.parse import urljoin, urlsplit

import requests
from gallery_dl import exception
from gallery_dl.extractor.reddit import RedditUserExtractor

from .encoding import InvalidData

MAX_HTML = 2 << 20
MAX_LINKS = 32
REDIRECTORS = frozenset({'myredlink.com', 'www.myredlink.com', 't.co',
                        'bit.ly', 'tinyurl.com', 'www.tinyurl.com'})
BIO_URL = re.compile(r'''https?://[^\s<>"'`]+''', re.I)


def public_url(value):
    if not isinstance(value, str) or not 1 <= len(value) <= 4096 or any(ord(c) <= 32 for c in value) or '\\' in value:
        return None
    try:
        parsed = urlsplit(value)
        if (parsed.scheme not in {'https', 'http'} or not parsed.hostname or parsed.username is not None
                or parsed.password is not None or parsed.port not in (None, 80, 443)):
            return None
        host = parsed.hostname.lower()
        if '.' not in host or host.endswith(('.local', '.localhost', '.internal')):
            return None
        try:
            ipaddress.ip_address(host)
        except ValueError:
            return value
    except ValueError:
        pass
    return None


def identity(user):
    if not isinstance(user, dict):
        raise InvalidData('Reddit profile has no stable identity')
    uid, name = user.get('id'), user.get('name')
    if not isinstance(uid, str) or not re.fullmatch(r'(?:t2_)?[a-z0-9]+', uid):
        raise InvalidData('Reddit profile has no stable identity')
    if not isinstance(name, str) or not re.fullmatch(r'[A-Za-z0-9_-]{1,64}', name):
        raise InvalidData('Reddit profile has no handle')
    return uid if uid.startswith('t2_') else 't2_' + uid, name.lower()


class ProfilePage(HTMLParser):
    def __init__(self):
        super().__init__(convert_charrefs=True)
        self.identities, self.links = set(), []

    def handle_starttag(self, tag, attrs):
        attrs = dict(attrs)
        if tag in {'shreddit-screenview-data', 'reddit-page-data'}:
            try:
                self.identities.add(identity(json.loads(attrs.get('data', '{}'))['profile']))
            except (ValueError, KeyError, TypeError):
                pass
        if tag != 'faceplate-tracker' or attrs.get('source') != 'profile' or attrs.get('noun') != 'social_link':
            return
        try:
            link = json.loads(attrs['data-faceplate-tracking-context'])['social_link']['url']
        except (ValueError, KeyError, TypeError):
            return
        if public_url(link) and link not in self.links and len(self.links) < MAX_LINKS:
            self.links.append(link)


def social_links(body, user):
    if len(body.encode('utf-8')) > MAX_HTML:
        raise InvalidData('Reddit profile page exceeds the size limit')
    page = ProfilePage()
    page.feed(body)
    if page.identities != {identity(user)}:
        raise InvalidData('Reddit profile page identity differs from the API profile')
    return [{'url': value} for value in page.links]


class RedirectPage(HTMLParser):
    def __init__(self):
        super().__init__(convert_charrefs=True)
        self.targets = set()

    def handle_starttag(self, tag, attrs):
        attrs = dict(attrs)
        if tag == 'a' and attrs.get('id') == 'bncyContinue':
            self.targets.add(attrs.get('href', ''))
        elif tag == 'meta' and attrs.get('http-equiv', '').lower() == 'refresh':
            match = re.fullmatch(r'''\s*\d+(?:\.\d+)?\s*;\s*url\s*=\s*(.*?)\s*''', attrs.get('content', ''), re.I)
            if match:
                self.targets.add(match[1].strip('"\''))


def redirect_target(body):
    page = RedirectPage()
    page.feed(body)
    # myredlink's public landing page uses this literal and its Continue link.
    # Parse the literal only; never execute JavaScript or follow arbitrary anchors.
    for match in re.finditer(r'''window\.BOUNCY_REDIRECT_URL\s*=\s*(['"])(https?://[^'"\s\\]+)\1\s*;''', body):
        page.targets.add(html.unescape(match[2]))
    return next(iter(page.targets)) if len(page.targets) == 1 else None


def resolve_link(url, fetch):
    """Resolve bounded known redirectors; never request a destination profile."""
    original, visited = url, set()
    for _ in range(5):
        if not public_url(url) or url in visited:
            return None
        if urlsplit(url).hostname.lower() not in REDIRECTORS:
            return url if url != original else None
        visited.add(url)
        status, location, body = fetch(url)
        if status in {301, 302, 303, 307, 308}:
            target = location
        elif status == 200:
            target = redirect_target(body)
        else:
            return None
        if not target:
            return None
        url = urljoin(url, target)
    return None


def bio_links(user):
    page = user.get('subreddit') or {}
    links = [link['url'] for link in user.get('social_links', [])
             if isinstance(link, dict) and public_url(link.get('url'))]
    for field in ('description', 'public_description'):
        text = page.get(field, '')
        if not isinstance(text, str):
            continue
        for value in BIO_URL.findall(html.unescape(text)):
            value = value.rstrip('.,;!?')
            for left, right in (('(', ')'), ('[', ']'), ('{', '}')):
                while value.endswith(right) and value.count(right) > value.count(left):
                    value = value[:-1]
            if public_url(value):
                links.append(value)
    return list(dict.fromkeys(links))[:MAX_LINKS]


def refresh(extractor, producer, user):
    identity(user)
    user = copy.deepcopy(user)
    # Use the current layout explicitly: www can serve the old UI according to
    # session preferences, and that UI does not include Social Links.
    url = 'https://sh.reddit.com/user/' + user['name'] + '/'
    try:
        response = extractor.request(url, fatal=False, allow_redirects=False)
        try:
            if response.status_code != 200:
                raise InvalidData('Reddit social links request failed')
            user['social_links'] = social_links(response.text, user)
        finally:
            response.close()
    except (InvalidData, exception.ExtractionError, requests.exceptions.RequestException):
        extractor.log.warning('Reddit social links unavailable; retaining API bio, retrying on the next profile fetch')

    with requests.Session() as session:
        session.trust_env = False
        session.headers.clear()
        session.headers['User-Agent'] = 'stash-ingest profile-link resolver'

        def fetch(link):
            producer.reserve_source(link)
            permit = producer.request_source(link)
            response = None
            try:
                response = session.get(link, allow_redirects=False, timeout=15, stream=True)
                producer.report_source(permit, response)
                if response.status_code == 429:
                    producer.fail_source('rate_limited', permit['source_scope'])
                content = bytearray()
                if response.status_code == 200:
                    for chunk in response.iter_content(16384):
                        producer.check()
                        content.extend(chunk)
                        if len(content) > MAX_HTML:
                            raise InvalidData('Profile redirect page exceeds the size limit')
                return response.status_code, response.headers.get('Location'), content.decode('utf-8', 'replace')
            except requests.exceptions.RequestException:
                if response is None:
                    producer.report_source(permit)
                raise
            finally:
                if response is not None:
                    response.close()

        resolutions = []
        for link in bio_links(user):
            try:
                destination = resolve_link(link, fetch)
            except (InvalidData, requests.exceptions.RequestException):
                destination = None
            if destination is None and urlsplit(link).hostname.lower() in REDIRECTORS:
                extractor.log.warning('Profile link resolution unavailable; retaining original link')
            if destination:
                resolutions.append({'url': link, 'resolved_url': destination})
        if resolutions:
            user['resolved_links'] = resolutions
    return user


def install(extractor, producer):
    if not isinstance(extractor, RedditUserExtractor) or extractor.groups[1] not in (None, '/submitted'):
        return
    original = extractor.submissions

    def submissions(self):
        result = original()  # user_about runs before listing iteration, even if empty.
        user = self.kwdict.get('user')
        identity(user)
        user = refresh(self, producer, user)
        self.kwdict['user'] = user
        producer.observe_profile({'category': 'reddit', 'user': user})
        return result

    extractor.submissions = types.MethodType(submissions, extractor)
