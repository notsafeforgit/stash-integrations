"""Finish Redgifs history only when the checked feeds cover its reported range.

The ordinary listing is tried first. A confirmed page ceiling opens separate
image/video feeds, each from both ends when needed. Cursor state carries the
remaining traversal and any inaccessible middle range across worker restarts.
An inaccessible middle range remains unresolved: a page ceiling alone does not
prove that no other supported listing can expose it. Rate limits, missing
accounts and inconsistent responses likewise never establish completion.
"""

import json
from urllib.parse import urlsplit

from .client import Unavailable
from .encoding import InvalidData, encode as encode_request
from .runs import SourceFailure

PREFIX = 'redgifs-history-v1:'
FIELDS = {'phase', 'page', 'limit', 'images', 'videos'}


def scan_mode(client, run):
    """A new account needs history even when admitted by a routine scheduler.

    Files previously embedded in Reddit are not an initial-account baseline.
    Only successful native coverage for this exact collection/root can allow
    the archive boundary to stop a routine Redgifs pass.
    """
    after = 0
    for _ in range(200):
        page = client._request('POST', '/runs/list',
                               encode_request({'collection_uuid': run['collection_uuid'], 'after': after}),
                               max_response_bytes=4 << 20)
        if not isinstance(page, list) or len(page) > 50:
            raise Unavailable('invalid_source_history')
        for previous in page:
            if (not isinstance(previous, dict) or type(previous.get('sequence')) is not int
                    or previous['sequence'] <= after):
                raise Unavailable('invalid_source_history')
            after = previous['sequence']
        for previous in page:
            if (previous.get('state') != 'succeeded' or previous.get('operation') != 'download'
                    or previous.get('collection_uuid') != run['collection_uuid']
                    or previous.get('collection_revision') != run['collection_revision']
                    or previous.get('root_uuid') != run['root_uuid']
                    or previous.get('root_revision') != run.get('root_revision')
                    or previous.get('target_url') != run['target_url']
                    or previous.get('retrieval_url') or previous.get('attachment_retry')):
                continue
            completed = previous.get('completed')
            if not isinstance(completed, list):
                raise Unavailable('invalid_source_history')
            if any(isinstance(window, dict) and 'since' in window and window['since'] is None
                   and isinstance(window.get('until'), str) and not window.get('basis') for window in completed):
                return 'routine'
        if len(page) < 50:
            return 'initial'
    raise Unavailable('source_history_page_limit')


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
            empty_account = page == 1 and type(data.get('total')) is int and data['total'] == 0
            if (('total' in data and (type(data['total']) is not int or data['total'] < 0))
                    or (data['pages'] == 0 and (data['gifs'] or data.get('total', 0)))
                    or (page <= data['pages'] and not data['gifs'] and not empty_account)
                    or len(data['gifs']) > target.per_page
                    or ('page' in data and (type(data['page']) is not int or data['page'] != page))):
                raise InvalidData('Inconsistent Redgifs history listing cannot establish coverage')
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
        # Keep the checked range in the existing checkpoint. A replay of an
        # unresolved middle remains unresolved, even after a worker restart.
        producer.begin_page({'extractor': 'redgifs:user', 'url': target.url,
                             'cursor': original_cursor or encode(state), 'item': ''})
        producer.redgifs_history_coverage = {
            'policy': 'available', 'page_limit': state['limit'],
            'image_pages': state['images'], 'video_pages': state['videos'],
            'limited_types': [key for key in ('images', 'videos') if state[key] > state['limit'] * 2],
        }
        if producer.redgifs_history_coverage['limited_types']:
            producer.fail_source('source_history_limit', 'service:redgifs', page_limit=state['limit'])

    target.gifs = gifs
