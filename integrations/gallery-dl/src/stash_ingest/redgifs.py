"""Native Redgifs items, independent of Reddit posts that embed the same media."""

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


def install(extractor, producer=None, *, standalone=False):
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
