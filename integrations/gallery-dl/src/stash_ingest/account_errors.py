"""Keep explicit gallery-dl account states without inferring permanent deletion."""

from gallery_dl import exception


def access_reason(extractor, error):
    if (extractor.category != 'twitter'
            or extractor.subcategory not in {'timeline', 'tweets', 'with-replies', 'highlights', 'media'}
            or not isinstance(error, exception.AuthorizationError)
            or error.message == 'Account temporarily locked'):
        return None
    user = getattr(extractor, '_user_obj', None)
    if not isinstance(user, dict):
        return None
    relationship = user.get('relationship_perspectives') or user.get('legacy') or {}
    privacy = user.get('privacy') or user.get('legacy') or {}
    # The saved account lookup also survives gallery-dl's optional guest retry.
    # Use its explicit access flags without parsing a handle from an error.
    if relationship.get('blocked_by') is True:
        return 'blocked'
    if privacy.get('protected') is True and relationship.get('following') is not True:
        return 'protected'
    return None


def missing_code(extractor, error):
    if not isinstance(error, exception.NotFoundError):
        return 'not_found'
    message = error.message
    category = extractor.category
    if category == 'reddit':
        if message == 'Suspended User':
            return 'account_suspended'
        if extractor.subcategory.startswith('user') and message in {
                'Requested user could not be found',
                f'Requested {extractor.subcategory} could not be found'}:
            return 'account_not_found'
    elif category == 'twitter':
        # gallery-dl retains the lookup result before translating its failure
        # into NotFoundError. A missing handle is not proof of a deleted GUID.
        user = getattr(extractor, '_user_obj', None)
        if isinstance(user, dict) and user.get('__typename') == 'UserUnavailable':
            reason = user.get('reason')
            if reason == 'Suspended' or message in {'User has been suspended.', 'This account is suspended.'}:
                return 'account_suspended'
            if reason == 'Deactivated':
                return 'account_deactivated'
            if reason == 'NotFound' or message == 'User not found.':
                return 'account_not_found'
            return 'account_unavailable'
        if message in {'Requested user could not be found', 'User not found.'}:
            return 'account_not_found'
    return 'not_found'
