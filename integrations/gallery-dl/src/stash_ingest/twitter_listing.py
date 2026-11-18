"""Resumable Twitter profile listings and the one-time available-history search.

gallery-dl now reads a profile's media tab as two timelines, all photos and
then all videos, and it no longer has the combined timeline/search traversal.
Here a media or posts extractor becomes an ordered list of phases. Each phase
is one gallery-dl pagination call. A page checkpoint names its phase, search
bound and remote cursor, so a resumed run never applies one timeline's cursor
to another.

Routine checks stop each timeline separately at its archived boundary. Runs
without a lower date bound (initial imports and explicit recovery) then search
`from:<account>` for older posts. The search starts where the earliest-ending
timeline stopped, the point down to which every timeline was complete. Each
timeline's end is its last listed post, as in upstream's removed traversal: an
out-of-order post can then only repeat posts, never skip a range. The search
steps `max_id` below each finished search, until one finds nothing older.
As upstream's removed traversal did, it searches posts with links first and
falls back to an unfiltered search only when that finds nothing. X search can
still omit posts; this is the available history, not a completeness proof.
"""

import json
import re

from gallery_dl import exception

from .encoding import InvalidData

PREFIX = 'twitter-listing-v1:'
TIMELINES = {'photos': 'user_media_photos', 'videos': 'user_media_videos', 'posts': 'user_original_timeline'}
SEARCHES = ('search-links', 'search-all')
ENDPOINTS = {'photos': 'UserPhotoTimeline', 'videos': 'UserVideoTimeline', 'posts': 'UserOriginalsTimeline',
             'search-links': 'SearchTimeline', 'search-all': 'SearchTimeline'}
FIELDS = {'phase', 'cursor', 'bound', 'last', 'floors', 'found'}
MAX_ID = (1 << 63) - 1
# The removed timeline extractor read this sub-timeline before searching.
STRATEGIES = {'media': 'media', 'tweets': 'tweets', 'with_replies': 'with-replies'}


def strategy(target):
    """The profile route equivalent to the removed `timeline` include."""
    value = (target.config('timeline') or {}).get('strategy')
    if value is None or value == 'auto':
        return 'tweets' if target.config('retweets', False) or target.config('text-tweets', False) else 'media'
    if value not in STRATEGIES:
        raise InvalidData('Unsupported Twitter timeline strategy')
    return STRATEGIES[value]


def timelines(target):
    if target.subcategory == 'media':
        selected = target.groups[1]
        if selected is None:
            return ('photos', 'videos')
        if selected.startswith('photo'):
            return ('photos',)
        if selected.startswith('video'):
            return ('videos',)
        return None  # Upstream reports the invalid filter.
    if target.subcategory == 'tweets':
        return ('posts',)
    return None


def full_history(producer):
    window = producer.window
    return producer.scan_mode != 'routine' and (window.since is None or window.traversal)


def initial(phase):
    return {'phase': phase, 'cursor': '', 'bound': None, 'last': None, 'floors': {}, 'found': False}


def encode(state, phases):
    if state == initial(phases[0]):
        # A first page keeps the plain checkpoint saved by earlier releases.
        return ''
    return PREFIX + json.dumps(state, sort_keys=True, separators=(',', ':'))


def _identifier(value):
    return value is None or (type(value) is int and 1 <= value <= MAX_ID)


def decode(cursor, phases):
    if cursor == '':
        return initial(phases[0])
    if not isinstance(cursor, str) or not cursor.startswith(PREFIX):
        raise InvalidData('Twitter listing checkpoint has an unsupported format')
    try:
        value = json.loads(cursor[len(PREFIX):])
    except ValueError:
        value = None
    if not isinstance(value, dict) or set(value) != FIELDS or value['phase'] not in phases:
        raise InvalidData('Invalid Twitter listing checkpoint')
    phase, floors = value['phase'], value['floors']
    earlier = phases[:phases.index(phase)]
    if (not isinstance(value['cursor'], str) or len(value['cursor'].encode()) > 4096
            or any(ord(c) < 32 or ord(c) == 127 for c in value['cursor'])
            or not _identifier(value['bound']) or not _identifier(value['last'])
            or (value['bound'] is not None and phase not in SEARCHES)
            or not isinstance(floors, dict) or not set(floors) <= set(earlier) - set(SEARCHES)
            or not all(_identifier(item) and item is not None for item in floors.values())
            or type(value['found']) is not bool or (value['found'] and phase != 'search-links')
            or encode(value, phases) != cursor):
        raise InvalidData('Invalid Twitter listing checkpoint')
    return value


def _has_photo(tweet):
    legacy = tweet.get('legacy', tweet)
    media = (legacy.get('extended_entities') or {}).get('media')
    return isinstance(media, list) and any(isinstance(item, dict) and item.get('type') == 'photo' for item in media)


class Listing:
    def __init__(self, target, phases, *, producer=None, routine=False, resume=''):
        self.target, self.phases, self.producer, self.routine = target, tuple(phases), producer, routine
        self.state = decode(resume, self.phases)
        self.pending = self.state['cursor'] or None
        self.seen = set()

    def init_cursor(self):
        # Only the resumed phase's first request continues a remote cursor.
        value, self.pending = self.pending, None
        return value

    def page(self, endpoint, variables):
        """Return the checkpoint for the listing page about to be requested."""
        phase = self.state['phase']
        if endpoint.rsplit('/', 1)[-1] != ENDPOINTS[phase]:
            raise InvalidData('Twitter listing request differs from its checkpoint phase')
        if phase in SEARCHES and variables.get('rawQuery') != self.query():
            raise InvalidData('Twitter history search differs from its checkpoint bound')
        cursor = variables.get('cursor') or ''
        if not isinstance(cursor, str):
            raise InvalidData('Invalid Twitter listing cursor')
        self.state['cursor'] = cursor
        return encode(self.state, self.phases)

    def query(self):
        name = (self.target._user or {}).get('name')
        if not isinstance(name, str) or not re.fullmatch(r'[A-Za-z0-9_]{1,50}', name):
            raise InvalidData('Twitter history search has no account handle')
        query = 'from:' + name
        if self.state['bound'] is not None:
            query += ' max_id:' + str(self.state['bound'])
        if self.state['phase'] == 'search-links':
            query += ' filter:links'
        return query

    def _advance(self, phase, *, bound=None):
        self.state = {**initial(phase), 'floors': self.state['floors'], 'bound': bound}

    def _track(self, iterator):
        """Record each phase's last listed post and skip this run's repeats."""
        skipped = None
        for tweet in iterator:
            if not isinstance(tweet, dict):
                yield tweet
                continue
            legacy = tweet.get('legacy') or {}
            quoted_by = legacy.get('quoted_by_id_str')
            if quoted_by is not None:
                # gallery-dl yields a quoted post after the post quoting it.
                if quoted_by != skipped:
                    yield tweet
                continue
            identity = tweet.get('rest_id') or legacy.get('id_str')
            position = tweet.get('_retweet_id_str') or identity
            if not isinstance(position, str) or not position.isascii() or not position.isdecimal():
                yield tweet
                continue
            if self.state['phase'] == 'search-links':
                self.state['found'] = True
            number = int(position)
            if not tweet.get('pinned') and 1 <= number <= MAX_ID:
                # A pinned post is listed first and can be far older; it is
                # never the last post of a page that continues after it.
                self.state['last'] = number
            if identity in self.seen:
                skipped = identity
                continue
            skipped = None
            self.seen.add(identity)
            yield tweet

    def _timeline(self, phase):
        api = self.target.api
        iterator = self._track(getattr(api, TIMELINES[phase])(self.target.user))
        if not self.routine:
            yield from iterator
            return
        from . import recent
        # A post with a photo also appears in the photo timeline. Seen earlier
        # in this run, or before a resume, it is not the video boundary.
        boundary = (lambda data: not _has_photo(data)) if phase == 'videos' else None
        yield from recent.posts(iterator, self.producer, 'twitter', boundary=boundary)

    def _search(self):
        api = self.target.api
        while True:
            yield from self._track(api.search_timeline(self.query()))
            last, bound = self.state['last'], self.state['bound']
            if last is None or last <= 1 or (bound is not None and last > bound):
                return
            # The next search starts below the last post this one reached.
            self.state = {**self.state, 'cursor': '', 'bound': last - 1, 'last': None}

    def _check_access(self):
        user = self.target._user_obj or {}
        relationship = user.get('relationship_perspectives') or user.get('legacy') or {}
        privacy = user.get('privacy') or user.get('legacy') or {}
        if privacy.get('protected') is True and relationship.get('following') is not True:
            # An empty response must not complete history this viewer cannot see.
            name = (self.target._user or {}).get('name') or 'account'
            raise exception.AuthorizationError(f"{name}'s Tweets are protected")

    def tweets(self):
        target = self.target
        # Resolve the account before any phase; this also assigns its profile.
        target.api._user_id(target.user)
        index = self.phases.index(self.state['phase'])
        yielded = False
        while index < len(self.phases):
            phase = self.phases[index]
            if phase in TIMELINES:
                for tweet in self._timeline(phase):
                    yielded = True
                    yield tweet
                floors = dict(self.state['floors'])
                if self.state['last'] is not None:
                    floors[phase] = self.state['last']
                self.state = {**self.state, 'floors': floors}
                following = self.phases[index + 1] if index + 1 < len(self.phases) else None
                if following is None or following in SEARCHES:
                    if not yielded and not self.state['floors']:
                        self._check_access()
                if following is not None:
                    anchor = max(floors.values()) if floors and following in SEARCHES else None
                    self._advance(following, bound=anchor)
            else:
                yield from self._search()
                if phase == 'search-links' and self.state['found']:
                    return
                if index + 1 < len(self.phases):
                    self._advance(self.phases[index + 1], bound=self._anchor())
            index += 1

    def _anchor(self):
        floors = self.state['floors']
        return max(floors.values()) if floors else None


def install(target, producer):
    """Replace a media/posts extractor's listing; return None for other routes."""
    phases = timelines(target)
    if phases is None:
        return None
    from . import recent
    category = 'twitter:' + target.subcategory
    saved = producer.saved_pagination
    resume = ''
    if saved is not None:
        same = (saved['extractor'], saved['url']) == (category, target.url)
        if saved['extractor'] == 'twitter:timeline' or (same and saved['cursor'] and not saved['cursor'].startswith(PREFIX)):
            # Upstream removed the combined timeline and replaced its endpoints.
            # Its cursors cannot address these timelines; restart this listing.
            producer.restart_listing()
        elif same:
            resume = saved['cursor']
    if full_history(producer):
        phases += ('search-all',) if target.config('text-tweets', False) else SEARCHES
    listing = Listing(target, phases, producer=producer, routine=recent.chronological(target, producer),
                      resume=resume)
    configure(target)
    target._native_listing = listing
    target._init_cursor = listing.init_cursor
    target.tweets = listing.tweets
    return listing


def configure(target):
    original = target.config

    def configuration(key, default=None):
        # Checkpoints carry the search bound; gallery-dl must not rewrite it.
        if key in ('search-pagination', 'search-results'):
            return None
        return original(key, default)

    target.config = configuration
