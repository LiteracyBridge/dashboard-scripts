import argparse
from os.path import expanduser
from pathlib import Path
from typing import Union


class StorePathAction(argparse.Action):
    """
    An argparse.Action to store a Path object. A leading ~ is expanded to the user's home directory.
    If the option 'trailing_slash' is True, the string is forced to have a trailing slash character,
    but that doesn't seem to matter to the Path object.
    """

    def _expand(self, v: str) -> Union[None, str]:
        """
        Does the work of expanding.
        :param v: A string, possibly with a leading ~ to be expanded ot user's home directory.
        :return: A Path object that encapsulates the given path. Note that there is no guarantee of
            any actual file system object at that path.
        """
        if v is None:
            return None
        v = expanduser(v)
        if self._trailing_slash and v[-1:] != '/':
            v += '/'
        return Path(v)

    def __init__(self, option_strings, dest, nargs=None, trailing_slash=False, default=None, **kwargs):
        self._trailing_slash = trailing_slash
        super(StorePathAction, self).__init__(option_strings, dest, default=self._expand(default), nargs=nargs,
                                              **kwargs)

    def __call__(self, parser, namespace, values, option_string=None):
        values = [self._expand(v) for v in values] if isinstance(values, list) else self._expand(values)
        setattr(namespace, self.dest, values)


class StoreFileExtension(argparse.Action):
    """
    An argparse.Action to store a file extension, with leading dot.
    """

    def _fix(self, v: str) -> Union[None, str]:
        """
        Make sure there's a leading dot.
        """
        if v is None or not isinstance(v, str):
            return None
        if v[0] != '.':
            v = '.' + v
        return v

    def __init__(self, option_strings, dest, nargs=None, default=None, **kwargs):
        super(StoreFileExtension, self).__init__(option_strings, dest, default=self._fix(default), nargs=nargs,
                                                 **kwargs)

    def __call__(self, parser, namespace, values, option_string=None):
        values = [self._fix(v) for v in values] if isinstance(values, list) else self._fix(values)
        setattr(namespace, self.dest, values)