"""Native Redgifs items, independent of Reddit posts that embed the same media."""

import copy
from datetime import datetime, timezone
import re

from .encoding import InvalidData

ID = re.compile(r'[A-Za-z0-9]{1,256}')


def post(data):
    value = data.get('id')
    if not isinstance(value, str) or not ID.fullmatch(value):
        raise InvalidData('Redgifs item requires its API ID')
    return {'namespace': 'native:redgifs', 'value': value.lower()}


def attachment(data, download):
    from .reddit_external import same_url
    urls = data.get('urls')
    if isinstance(urls, dict):
        for key in ('hd', 'sd', 'gif'):
            value = urls.get(key)
            # gallery-dl rewrites this CDN alias before choosing a rendition.
            if isinstance(value, str) and same_url(value.replace('//thumbs2.', '//thumbs3.', 1), download):
                return post(data)
    raise InvalidData('Selected Redgifs rendition is absent from the captured API item')


def publication(data):
    value = data.get('createDate')
    if value is None:
        return None
    if type(value) is not int or not 0 <= value < 253402300800:
        raise InvalidData('Invalid Redgifs publication timestamp')
    return datetime.fromtimestamp(value, timezone.utc)


def install_gallery(extractor):
    from gallery_dl.extractor.redgifs import RedgifsImageExtractor
    if not isinstance(extractor, RedgifsImageExtractor) or hasattr(extractor, '_native_gallery_members'):
        return
    extractor._native_gallery_members = None
    original_gif, original_gallery = extractor.api.gif, extractor.api.gallery
    seed = None

    def gif(identity):
        nonlocal seed
        result = original_gif(identity)
        seed = copy.deepcopy(result)
        return result

    def gallery(identity):
        result = original_gallery(identity)
        members = result.get('gifs')
        if not isinstance(seed, dict) or seed.get('gallery') != identity or not isinstance(members, list):
            raise InvalidData('Redgifs gallery has no matching requested item')
        items = {}
        for item in members:
            if not isinstance(item, dict) or item.get('gallery') != identity:
                raise InvalidData('Redgifs gallery contains an unrelated item')
            key = post(item)['value']
            if key in items:
                raise InvalidData('Redgifs gallery contains duplicate item identities')
            items[key] = copy.deepcopy(item)
        selected = post(seed)['value']
        if selected not in items:
            raise InvalidData('Redgifs gallery omits the requested item')
        # Only the current gallery is needed. Later responses replace this
        # in-memory binding, without adding an enduring membership ledger.
        extractor._native_gallery_members = (identity, selected, items)
        return result

    extractor.api.gif, extractor.api.gallery = gif, gallery


def linked_gallery_source(extractor, data):
    from . import reddit_external, source
    binding = getattr(extractor, '_native_gallery_members', None)
    if binding is None or data.get('category') != 'redgifs':
        return data
    parent, category = source._context(data)
    if category != 'reddit':
        return data
    identity, selected, members = binding
    item = post(data)['value']
    if item == selected:
        return data
    parent = source._reddit_attachment_context(parent)
    linked = reddit_external.target(parent)
    image = reddit_external.redgifs_image_id(parent.get('url'))
    expected = linked['value'] if linked and linked['namespace'] == 'native:redgifs' else image
    if expected != selected or data.get('gallery') != identity or item not in members:
        raise InvalidData('Additional Redgifs gallery item has no verified Reddit link')
    result = copy.deepcopy(members[item])
    attachment(result, data.get('_url'))
    result.update(category='redgifs', subcategory=extractor.subcategory,
                  source_extractor_url=data['source_extractor_url'], _url=data['_url'])
    return result


def install(extractor, producer=None, *, standalone=False):
    install_gallery(extractor)
    from gallery_dl.extractor.redgifs import RedgifsUserExtractor
    if not isinstance(extractor, RedgifsUserExtractor) or getattr(extractor, '_native_redgifs_evidence', False):
        return
    original = extractor.metadata

    def metadata():
        # Upstream inserts the requested profile name over the API publisher.
        # Keep the actual per-item userName, including in mixed-owner feeds.
        return {key: value for key, value in original().items() if key != 'userName'}

    extractor.metadata = metadata
    extractor._native_redgifs_evidence = True
    if standalone and producer is not None and extractor.config('native-profile-directory', False):
        if producer.path_prefix != '.':
            # This destination is pinned by the claimed collection revision.
            # Escape braces: directory names are data, never format expressions.
            directory = [part.replace('{', '{{').replace('}', '}}')
                         for part in producer.path_prefix.split('/')]
            original_config = extractor.config

            def config(key, default=None):
                if key == 'directory':
                    return directory
                if key == 'base-directory':
                    return str(producer.root.path)
                return original_config(key, default)

            extractor.config = config
