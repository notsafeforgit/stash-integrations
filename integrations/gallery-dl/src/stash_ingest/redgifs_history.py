"""Finish explicitly reviewed Redgifs history after exhausting supported feeds.

The ordinary listing is tried first. A confirmed page ceiling opens separate
image/video feeds, each from both ends when needed. Cursor state carries the
remaining traversal and any inaccessible middle range across worker restarts.
The opt-in profile defines success as all provider-accessible history; it never
turns rate limits, missing accounts or malformed responses into completion.
"""

import json
from urllib.parse import urlsplit

from .encoding import InvalidData
from .runs import SourceFailure

PREFIX = 'redgifs-history-v1:'
FIELDS = {'phase', 'page', 'limit', 'images', 'videos'}


def decode(cursor):
    if cursor.isascii() and cursor.isdecimal() and 1 <= int(cursor) <= 1000000:
        return dict(phase=0, page=int(cursor), limit=0, images=0, videos=0)
    try:
        value = json.loads(cursor[len(PREFIX):]) if cursor.startswith(PREFIX) else None
    except ValueError:
        value = None
    if (not isinstance(value, dict) or set(value) != FIELDS
            or any(type(v) is not int for v in value.values())
            or not 0 <= value['phase'] <= 5 or not 1 <= value['page'] <= 1000000
            or not 0 <= value['limit'] <= 1000000
            or not 0 <= value['images'] <= 1000000 or not 0 <= value['videos'] <= 1000000
            or (value['phase'] and not value['limit'])):
        raise InvalidData('Invalid Redgifs history checkpoint')
    return value


def encode(value):
    return PREFIX + json.dumps(value, sort_keys=True, separators=(',', ':'))


def install(target, producer, resume):
    parsed = urlsplit(target.url)
    # A filtered/popular feed cannot be silently reinterpreted as full history.
    if parsed.query or parsed.fragment or producer.replay_archive:
        raise InvalidData('Available Redgifs history requires an unfiltered profile without recovery replay')
    state = decode(resume['cursor']) if resume else dict(phase=0, page=1, limit=0, images=0, videos=0)
    original_cursor = resume['cursor'] if resume else None
    endpoint = '/v2/users/' + target.key.lower() + '/search'

    def gifs():
        nonlocal original_cursor
        while state['phase'] <= 4:
            phase, page = state['phase'], state['page']
            producer.redgifs_history_expanding = phase != 0
            key = 'images' if phase in (1, 2) else 'videos'
            reverse = phase in (2, 4)
            # The opposite end needs only the pages absent from the newest end.
            if reverse and page > min(state['limit'], max(0, state[key] - state['limit'])):
                state.update(phase=phase + 1, page=1)
                continue
            cursor = original_cursor or encode(state)
            producer.begin_page({'extractor': 'redgifs:user', 'url': target.url, 'cursor': cursor, 'item': ''})
            original_cursor = None
            params = {'page': page, 'count': target.per_page, 'order': 'old' if reverse else 'new'}
            if phase:
                params['type'] = 'i' if phase < 3 else 'g'
            try:
                data = target.api._call(endpoint, params)
            except SourceFailure as exc:
                if (exc.code != 'source_history_limit' or exc.scope != 'service:redgifs'
                        or not exc.page_limit or page <= exc.page_limit
                        or producer.source_failure is not exc):
                    raise
                # Only this exact consumed limit is handled. Other sticky
                # failures retain their normal retry/ownership semantics.
                producer.source_failure = None
                state['limit'] = exc.page_limit
                if phase == 0:
                    state.update(phase=1, page=1)
                else:
                    state.update(phase=phase + 1, page=1)
                continue
            if (not isinstance(data, dict) or type(data.get('pages')) is not int
                    or not 0 <= data['pages'] <= 1000000 or not isinstance(data.get('gifs'), list)):
                raise InvalidData('Invalid Redgifs history listing')
            if phase:
                state[key] = max(state[key], data['pages'])
            yield from data['gifs']
            if page >= data['pages']:
                if phase == 0:
                    return
                state.update(phase=phase + 1, page=1)
            elif phase and page >= state['limit']:
                state.update(phase=phase + 1, page=1)
            else:
                state['page'] += 1
        # Keep the coverage note in the existing native page checkpoint, not
        # just the process log. A lost finish response can replay this terminal
        # marker without requesting inaccessible history again.
        producer.begin_page({'extractor': 'redgifs:user', 'url': target.url,
                             'cursor': original_cursor or encode(state), 'item': ''})
        producer.redgifs_history_coverage = {
            'policy': 'available', 'page_limit': state['limit'],
            'image_pages': state['images'], 'video_pages': state['videos'],
            'limited_types': [key for key in ('images', 'videos') if state[key] > state['limit'] * 2],
        }

    target.gifs = gifs
