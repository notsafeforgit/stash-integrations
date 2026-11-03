"""Keep explicit gallery-dl account states without inferring permanent deletion."""

from gallery_dl import exception


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
