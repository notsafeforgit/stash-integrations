"""Opt-in classic Reddit search, with full posts from the pinned API adapter.

HTML supplies only ordered post IDs and the next ``after`` cursor. Post metadata,
galleries, date/ID filters and comment handling still belong to gallery-dl. All
requests use its existing session, including native request budgets and backoff.
"""

from html.parser import HTMLParser
import re
from urllib.parse import parse_qsl, urljoin, urlsplit

from .encoding import InvalidData

ROOT = 'https://old.reddit.com'
MAX_HTML = 2 << 20
MAX_POSTS = 100
POST = re.compile(r't3_[a-z0-9]{1,20}\Z')
SEARCH = re.compile(r'(?:/r/[A-Za-z0-9_+]+)?/search/\.json\Z')
VOID = frozenset({'area', 'base', 'br', 'col', 'embed', 'hr', 'img', 'input',
                  'link', 'meta', 'param', 'source', 'track', 'wbr'})


class SearchPage(HTMLParser):
    def __init__(self):
        super().__init__(convert_charrefs=True)
        self.stack, self.posts, self.next_links = [], [], []
        self.fields = {'q': set(), 'sort': set(), 't': set()}
        self.body = self.closed = self.listings = self.footers = 0
        self.empty = False

    def inside(self, name):
        return any(name in classes for _, classes in self.stack)

    def handle_starttag(self, tag, attrs):
        attrs = dict(attrs)
        classes = set(attrs.get('class', '').split())
        if tag == 'body':
            self.body += int('search-page' in classes)
        if tag == 'input' and attrs.get('name') in self.fields:
            self.fields[attrs['name']].add(attrs.get('value', ''))
        if 'search-result-listing' in classes:
            self.listings += 1
        if self.inside('search-result-listing'):
            if tag == 'footer':
                self.footers += 1
            if 'search-result-link' in classes:
                fullname = attrs.get('data-fullname', '')
                if (not POST.fullmatch(fullname) or fullname in self.posts
                        or len(self.posts) >= MAX_POSTS):
                    raise InvalidData('Invalid or repeated Reddit search result')
                self.posts.append(fullname)
            if tag == 'a' and self.inside('nextprev'):
                if 'next' in attrs.get('rel', '').split():
                    self.next_links.append(attrs.get('href', ''))
                elif 'after=' in attrs.get('href', ''):
                    raise InvalidData('Unrecognized Reddit search continuation link')
        if tag not in VOID:
            self.stack.append((tag, classes))

    def handle_endtag(self, tag):
        if tag == 'body':
            self.closed += 1
        for index in range(len(self.stack) - 1, -1, -1):
            if self.stack[index][0] == tag:
                del self.stack[index:]
                break

    def handle_data(self, value):
        if (self.inside('search-result-listing') and self.stack
                and self.stack[-1] == ('p', {'info'})
                and value.strip() == "there doesn't seem to be anything here"):
            self.empty = True


def parse_page(body, path, params):
    if not isinstance(body, str) or len(body.encode('utf-8')) > MAX_HTML:
        raise InvalidData('Reddit search page exceeds the size limit')
    page = SearchPage()
    page.feed(body)
    page.close()
    if page.body != 1 or page.closed != 1 or page.listings != 1 or not page.footers:
        raise InvalidData('Unrecognized or incomplete Reddit search page')
    for key in ('q', 'sort', 't'):
        if page.fields[key] != {params[key]}:
            raise InvalidData('Reddit search page differs from the requested query')
    if (not page.posts and not page.empty) or (page.posts and page.empty):
        raise InvalidData('Reddit search page has no verified result listing')
    if not page.next_links:
        return page.posts, None
    if len(page.next_links) != 1 or not page.posts:
        raise InvalidData('Invalid Reddit search continuation')
    # Never follow a supplied URL. Validate it, then reuse only its after token
    # on our fixed endpoint with the original query/sort/time/scope parameters.
    try:
        url = urlsplit(urljoin(ROOT + path, page.next_links[0]))
        if (url.scheme != 'https' or url.hostname not in {'old.reddit.com', 'www.reddit.com'}
                or url.port not in (None, 443) or url.username is not None
                or url.password is not None or url.path != path or url.fragment):
            raise ValueError
        pairs = parse_qsl(url.query, keep_blank_values=True, max_num_fields=32)
        query = dict(pairs)
        if len(query) != len(pairs) or 'before' in query:
            raise ValueError
        after = query.pop('after', '')
        count = query.pop('count', '0')
        expected = {k: v for k, v in params.items() if k not in {'after', 'count'}}
        if (query != expected or not count.isdigit() or not POST.fullmatch(after)
                or after != page.posts[-1] or after == params.get('after')):
            raise ValueError
    except ValueError:
        raise InvalidData('Reddit search continuation changes scope or repeats a page') from None
    return page.posts, after


def install(target):
    """Called after RedditAPI creation, before the durable page wrapper."""
    mode = target.config('native-search-api', 'api')
    if mode == 'api':
        return
    if mode != 'browser':
        raise InvalidData('Unsupported Reddit native-search-api setting')
    if target.subcategory not in {'home-search', 'subreddit-search'}:
        return
    api = target.api
    if api.root != 'https://www.reddit.com':
        raise InvalidData('Browser Reddit search requires api=rest')
    original_call, original_submission = api._call, api.submission
    cached = None
    visited = set()

    def submission(post_id):
        nonlocal cached
        if cached is not None and cached[0] == post_id:
            return cached[1]
        return original_submission(post_id)

    def children(fullnames):
        nonlocal cached
        for fullname in fullnames:
            # Lazy lookup lets the producer checkpoint each post before the next
            # request. A failed lookup keeps this page and its completed item.
            cached = None
            result = original_submission(fullname[3:])
            post = result[0]
            if (not isinstance(post, dict) or post.get('id') != fullname[3:]
                    or post.get('name') != fullname
                    or not all(key in post for key in ('created_utc', 'num_comments', 'title', 'url', 'permalink'))):
                raise InvalidData('Reddit search post metadata has a different or incomplete identity')
            cached = fullname[3:], result
            yield {'kind': 't3', 'data': post}
        cached = None

    def call(endpoint, params, *args, **kwargs):
        if not SEARCH.fullmatch(endpoint):
            return original_call(endpoint, params, *args, **kwargs)
        query = {key: str(value) for key, value in params.items()}
        query.setdefault('sort', 'relevance')
        query.setdefault('t', 'all')
        query['raw_json'] = '1'
        if (not query.get('q') or len(query['q']) > 4096 or 'before' in query
                or (query.get('after') and not POST.fullmatch(query['after']))):
            raise InvalidData('Unsupported Reddit browser search query or cursor')
        cursor = query.get('after', '')
        if cursor in visited:
            raise InvalidData('Reddit search repeated a pagination cursor')
        visited.add(cursor)
        path = endpoint[:-5]  # /search/.json -> /search/
        response = target.request(ROOT + path, params=query,
                                  headers={'Accept': 'text/html', 'Accept-Language': 'en-US,en;q=0.9'},
                                  allow_redirects=False)
        try:
            if response.status_code != 200 or 'text/html' not in response.headers.get('Content-Type', '').lower():
                raise InvalidData('Reddit browser search did not return a successful HTML page')
            fullnames, after = parse_page(response.text, path, query)
        finally:
            response.close()
        return {'data': {'children': children(fullnames), 'after': after}}

    api.submission, api._call = submission, call
