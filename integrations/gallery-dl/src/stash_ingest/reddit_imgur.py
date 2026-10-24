"""Retain Imgur's album membership before gallery-dl expands it into files."""

import html
import re
from urllib.parse import urlsplit

from .encoding import InvalidData

FIELD = 'imgur_media'
ID = re.compile(r'[A-Za-z0-9]{5,12}')
ALBUM = re.compile(r'/(a|gallery)/(?:[^/?#]+-)?([A-Za-z0-9]{5,12})/?')


def link(value):
    from .web_media import reference
    if not isinstance(value, str):
        return None
    value = html.unescape(value)
    try:
        reference(value)
        parsed = urlsplit(value)
    except (InvalidData, ValueError):
        return None
    if parsed.hostname not in ('imgur.com', 'www.imgur.com', 'm.imgur.com'):
        return None
    match = ALBUM.fullmatch(parsed.path)
    return (match[1], match[2]) if match else None


def target(data):
    from . import reddit_external
    from .source import reddit_media
    links = [link(data.get(key)) for key in ('url', 'url_overridden_by_dest')]
    links = [value for value in links if value is not None]
    if not links:
        return None
    if (any(value[1] != links[0][1] for value in links[1:])
            or any(reddit_external.linked(data.get(key)) or reddit_media(data.get(key))
                   for key in ('url', 'url_overridden_by_dest'))):
        raise InvalidData('Reddit post media identifiers disagree')
    return links[0][1]


def manifest(data, source):
    album = source.get('album')
    marker = album.get(FIELD) if isinstance(album, dict) else None
    if marker is None:
        return None
    identity = target(data)
    if (source.get('category') != 'imgur' or data.get('comment') or identity is None
            or album.get('id') != identity or album.get('is_album') is not True
            or not isinstance(marker, dict) or set(marker) != {'version', 'items'}
            or type(marker['version']) is not int or marker['version'] != 1
            or not isinstance(marker['items'], list) or not 1 <= len(marker['items']) <= 4096
            or type(album.get('image_count')) is not int or album['image_count'] != len(marker['items'])):
        raise InvalidData('Imgur album membership does not match the linked Reddit album')
    seen = set()
    for entry in marker['items']:
        if (not isinstance(entry, dict) or set(entry) != {'id', 'kind'}
                or not isinstance(entry['id'], str) or not ID.fullmatch(entry['id'])
                or entry['id'] in seen or entry['kind'] not in ('image', 'video', 'unknown')):
            raise InvalidData('Invalid Imgur album member')
        seen.add(entry['id'])
    return marker


def attachment(data, source, download):
    from .reddit_external import same_url
    evidence = manifest(data, source)
    if evidence is None:
        return None
    if not same_url(source.get('url'), download):
        raise InvalidData('Selected Imgur rendition differs from the captured media URL')
    if any(entry['id'] == source.get('id') for entry in evidence['items']):
        return {'namespace': 'native:imgur', 'value': source['id']}
    raise InvalidData('Selected Imgur media is absent from the linked album')


def install(extractor):
    """Copy the API list, not output num/count or local download order."""
    if extractor.subcategory != 'album' or getattr(extractor, '_native_imgur_evidence', False):
        return
    original = extractor.api.album

    def album(identity):
        result = original(identity)
        media = result.get('media')
        if (result.get('id') != identity or result.get('is_album') is not True
                or not isinstance(media, list) or not 0 <= len(media) <= 4096
                or type(result.get('image_count')) is not int or result['image_count'] != len(media)):
            raise InvalidData('Imgur response lacks its complete ordered album membership')
        entries, seen = [], set()
        for item in media:
            if (not isinstance(item, dict) or not isinstance(item.get('id'), str)
                    or not ID.fullmatch(item['id']) or item['id'] in seen):
                raise InvalidData('Invalid Imgur album member')
            seen.add(item['id'])
            extension = item.get('ext', '').lower()
            kind = ('unknown' if item.get('is_animated') or extension == 'gif' else
                    'video' if extension in ('mp4', 'webm', 'mov', 'm4v', 'mkv') else
                    'image' if extension in ('jpg', 'jpeg', 'png', 'webp', 'avif', 'jxl', 'bmp') else 'unknown')
            entries.append({'id': item['id'], 'kind': kind})
        result[FIELD] = {'version': 1, 'items': entries}
        return result

    extractor.api.album = album
    extractor._native_imgur_evidence = True
