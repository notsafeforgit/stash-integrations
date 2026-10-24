"""Captured post/file evidence for the pinned Tumblr and gallery extractors.

URL references identify an observed source attachment, never local filenames.
Only Tumblr's explicit photos list and Chevereto's single-file page establish
complete membership. An HTML scan or extractor output count cannot do that.
"""

import copy
import hashlib
import re
import types
from urllib.parse import urlsplit, urlunsplit

from .encoding import InvalidData

FIELD = 'web_media'
CATEGORIES = ('tumblr', 'jpgfish', 'imglike', 'putmega', 'leakgallery', 'erome')


def reference(value):
    if (not isinstance(value, str) or not value.startswith(('https://', 'http://')) or len(value) > 8192
            or any(ord(c) <= 32 or ord(c) >= 127 or c == '\\' for c in value)
            or re.search(r'%(?![0-9A-Fa-f]{2})', value)):
        raise InvalidData('Source attachment requires an explicit HTTP URL')
    try:
        url = urlsplit(value)
        host = url.hostname
        if (url.scheme not in ('https', 'http') or url.username is not None
                or url.password is not None or url.port is not None or not host
                or not re.fullmatch(r'[a-z0-9][a-z0-9.-]*', host)
                or url.netloc.lower() != host):
            raise ValueError()
        canonical = urlunsplit((url.scheme, host, url.path or '/', url.query, ''))
    except ValueError:
        raise InvalidData('Source attachment requires an explicit HTTP URL') from None
    return 'url:' + hashlib.sha256(canonical.encode()).hexdigest()


def post(data, category):
    from .source import _agree, _id, _numeric
    if category == 'erome':
        value = _id(data.get('album_id'))
        if not re.fullmatch(r'[A-Za-z0-9_]{1,256}', value):
            raise InvalidData('Erome post requires its source album ID')
    elif category == 'tumblr':
        value = _numeric(_agree([data.get('id'), data.get('id_string')]))
    elif category == 'leakgallery':
        creator = _id(data.get('creator'))
        if not re.fullmatch(r'[A-Za-z0-9_.-]{1,256}', creator) or creator == 'unknown':
            raise InvalidData('LeakGallery post requires its explicit creator page')
        value = creator + '/' + _numeric(_id(data.get('id')))
    elif category in ('jpgfish', 'imglike', 'putmega'):
        value = _id(data.get('id'))
        if not re.fullmatch(r'[A-Za-z0-9_-]{1,256}', value):
            raise InvalidData('Chevereto post requires its source file-page ID')
    else:
        raise InvalidData('Unsupported web-media source')
    return {'namespace': 'native:' + category, 'value': value}


def manifest(data, category):
    marker = data.get(FIELD)
    if marker is None:
        return None
    if (not isinstance(marker, dict)
            or set(marker) != {'version', 'post_id', 'complete', 'album', 'items'}
            or type(marker['version']) is not int or marker['version'] != 1
            or type(marker['complete']) is not bool or type(marker['album']) is not bool
            or marker['post_id'] != post(data, category)['value']
            or not isinstance(marker['items'], list) or not 1 <= len(marker['items']) <= 4096):
        raise InvalidData('Invalid captured web-media membership')
    if category not in ('tumblr', 'erome') and marker['album']:
        raise InvalidData('A file page or HTML media scan does not declare an album')
    if category in ('leakgallery', 'erome') and marker['complete']:
        raise InvalidData('HTML media output cannot establish complete post membership')
    if category in ('jpgfish', 'imglike', 'putmega') and (len(marker['items']) != 1 or not marker['complete']):
        raise InvalidData('Chevereto capture requires its single source file')
    known = set()
    for item in marker['items']:
        if (not isinstance(item, dict) or set(item) != {'url', 'kind'}
                or item['kind'] not in ('image', 'video', 'unknown')):
            raise InvalidData('Invalid captured web-media attachment')
        key = reference(item['url'])
        if key in known:
            raise InvalidData('Captured source lists the same attachment twice')
        known.add(key)
    if marker['album'] and len(known) < 2:
        raise InvalidData('An album requires multiple observed attachments')
    return marker


def attachment(data, category):
    marker = manifest(data, category)
    selected = reference(data.get('web_media_url'))
    if marker is None or not any(reference(item['url']) == selected for item in marker['items']):
        raise InvalidData('Downloaded file is absent from the captured source list')
    return {'namespace': post(data, category)['namespace'], 'value': selected}


def _url(value):
    value = value.removeprefix('ytdl:')
    reference(value)
    return value


def _kind(data):
    extension = str(data.get('extension', '')).lower()
    if extension in ('jpg', 'jpeg', 'png', 'webp', 'avif', 'jxl', 'gif', 'bmp', 'tiff'):
        return 'image'
    if extension in ('mp4', 'm4v', 'webm', 'mov', 'mkv', 'avi'):
        return 'video'
    return 'unknown'


def _marker(data, category, items, *, complete, album=False):
    marker = {'version': 1, 'post_id': post(data, category)['value'],
              'complete': complete, 'album': album, 'items': items}
    manifest({**data, FIELD: marker}, category)
    return marker


def install(extractor, producer, scope):
    from gallery_dl.extractor.common import Message
    category = extractor.category
    if category == 'erome':
        install_erome(extractor)
        return
    if category == 'tumblr':
        if extractor.avatar or extractor.external:
            raise InvalidData('Native Tumblr intake requires avatar=false and external=false')
        original = extractor._extract_files

        def files(self, data):
            photos = data.get('photos') or []
            origins = {id(photo): _url(photo['original_size']['url']) for photo in photos}
            result = original(data)
            if not result:
                return result
            items = []
            for kind, url, file in result:
                if kind != Message.Url:
                    raise InvalidData('Tumblr external media needs its own source adapter')
                source_url = origins.get(id(file.get('photo'))) or _url(url)
                items.append({'url': source_url, 'kind': _kind(file)})
                file['web_media_url'] = source_url
            complete = bool(photos) and len(result) == len(photos)
            data[FIELD] = _marker(data, category, items, complete=complete, album=len(photos) > 1)
            return result

        extractor._extract_files = types.MethodType(files, extractor)
        return

    original = extractor.items
    if category == 'leakgallery':
        # The pinned extractor logs and swallows pagination errors. Preserve
        # its diagnostic, but never record a failed page as a completed scan.
        logger = extractor.log

        class CheckedLog:
            def __getattr__(self, name):
                return getattr(logger, name)

            def error(self, *args, **kwargs):
                logger.error(*args, **kwargs)
                producer.fail_source('extraction_failed', scope)

        extractor.log = CheckedLog()

    def messages():
        if category == 'leakgallery' and extractor.subcategory == 'post':
            # A single HTML page is bounded; retaining its observed URLs
            # together avoids repeating a different post body for each file.
            captured = []
            for message in original():
                if len(captured) >= 8192:
                    raise InvalidData('Source post exceeds the attachment limit')
                captured.append(message)
            files = [(url, data) for kind, url, data in captured if kind == Message.Url]
            if files:
                first = post(files[0][1], category)
                if any(post(data, category) != first for _, data in files):
                    raise InvalidData('LeakGallery page returned different post identities')
                items = [{'url': _url(url), 'kind': _kind(data)} for url, data in files]
                for _, data in files:
                    data[FIELD] = _marker(data, category, items, complete=False)
            stream = captured
        else:
            stream = original()
        for kind, url, data in stream:
            if kind == Message.Url:
                source_url = _url(url)
                if FIELD not in data:
                    data[FIELD] = _marker(data, category, [{'url': source_url, 'kind': _kind(data)}],
                                          complete=category != 'leakgallery')
                data['web_media_url'] = source_url
            yield kind, url, data

    extractor.items = messages


def install_erome(extractor):
    """Retain one bounded album observation before any file can be archived.

    Erome's HTML traversal provides order but cannot prove that inaccessible
    items are absent. Keep it partial, and never use num as attachment identity.
    Parent/profile extractors retain their ordinary child traversal.
    """
    from gallery_dl.extractor.common import Message
    if extractor.subcategory != 'album':
        return
    configured = extractor.config('archive-format')
    if configured and (not isinstance(configured, str) or '{web_media_id}' not in configured):
        raise InvalidData('Native Erome archive-format must include {web_media_id}')
    extractor.archive_fmt = 'native-v1_{album_id}_{web_media_id}'
    original = extractor.items

    def messages():
        captured, files = [], []
        for kind, url, data in original():
            if len(captured) >= 4097 or kind not in (Message.Directory, Message.Url):
                raise InvalidData('Invalid or oversized Erome album traversal')
            # The real extractor reuses and mutates one dict for every file.
            data = copy.deepcopy(data)
            captured.append((kind, url, data))
            if kind == Message.Url:
                files.append((url, data))
        if not files:
            raise InvalidData('Erome album has no readable media')
        first = post(files[0][1], 'erome')
        if any(post(data, 'erome') != first or type(data.get('count')) is not int
               or data['count'] != len(files) for _, _, data in captured):
            raise InvalidData('Erome album output has inconsistent identity or item count')
        items = [{'url': _url(url), 'kind': _kind(data)} for url, data in files]
        marker = _marker(files[0][1], 'erome', items, complete=False, album=len(items) > 1)
        for kind, url, data in captured:
            data[FIELD] = marker
            data['_http_headers'] = {**data.get('_http_headers', {}), 'Referer': 'https://www.erome.com/a/' + first['value']}
            if kind == Message.Url:
                data['web_media_url'] = _url(url)
                data['web_media_id'] = reference(data['web_media_url']).removeprefix('url:')
            yield kind, url, data

    extractor.items = messages
