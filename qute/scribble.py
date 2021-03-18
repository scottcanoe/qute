"""
author: Scott Knudstrup
"""
import appdirs
import collections
import json
import os
from pathlib import Path
import shelve
import sys
from types import ModuleType
from typing import (
    Any,
    ItemsView,
    Iterator,
    KeysView,
    Mapping,
    ValuesView,
)
import weakref

__all__ = [
    ""
]
# -- This is a special environment variable which the user can set in
# -- order to tailor exactly where scribble will write out its data
ENVIRONMENT_VARIABLE = "SCRIBBLE_STORAGE_DIR"

# -- Determine where we should store our data files. This is dependent
# -- upon a couple of factors. Firstly, we allow for hte user to define
# -- an environment variable which specifies the storage location
STORAGE_DIRECTORY = os.environ.get(ENVIRONMENT_VARIABLE, None)
if STORAGE_DIRECTORY is not None:
    STORAGE_DIRECTORY = Path(STORAGE_DIRECTORY)

# -- If we have not been provided with a specific storage location
# -- then we need to resolve to a default location, but this is platform
# -- dependent
if sys.platform in ("linux", "linux2"):
    if "XDG_CONFIG_HOME" in os.environ:
        STORAGE_DIRECTORY = Path(os.environ["XDG_CONFIG_HOME"]) / "scribble"
    else:
        STORAGE_DIRECTORY = Path(os.environ["HOME"]) / ".config/scribble"

elif sys.platform == "win32":
    STORAGE_DIRECTORY = Path(os.environ["APPDATA"]) / "scribble"

elif sys.platform == "darwin":
    STORAGE_DIRECTORY = Path(os.environ["HOME"]) / "Documents/scribble"

else:
    raise Exception(
        (
            "{} is not supported by default. In order to utilise this module "
            "you must define an environment variable ({}) specifying the "
            "storage path"
        ).format(sys.platform, ENVIRONMENT_VARIABLE)
    )


# ------------------------------------------------------------------------------


_CACHE = weakref.WeakValueDictionary()


class PersistentData(collections.abc.MutableMapping):

    _name: str
    _data: dict
    _backend: ModuleType
    _changed: bool
    _save_on_destroy: bool = False

    def __new__(cls, name: str, *args, **kw):

        if not isinstance(name, str):
            raise ValueError("name must be a string")
        
        # Attempt to return the cached instance.
        if name in _CACHE:
            self = _CACHE[name]
            if args or kw:
                self.update(dict(*args, **kw))
            return self
        except KeyError:
            pass

        # Create and initialize the instance.
        self = object.__new__(cls)
        self._init(name, *args, **kw)

        # Cache and return the instance.
        _CACHE[self._name] = self
        return self


    def _init(self, name: str, *args, **kw) -> None:
        """
        Called by __new__ when creating a new instance.
        """

        self._name = name
        self._data = {}
        self._changed = False
        self.load()
        self.update(dict(*args, **kw))

    # --------------------------------------------------------------------
    # properties

    @property
    def name(self) -> str:
        return self._name

    @property
    def path(self) -> Path:
        """
        Returns the location of the persistent file. This is regardless of
        whether the file exists or not.
        
        :return: pathlib.Path
        """
        return STORAGE_DIRECTORY / f"{self.name}.json"

    @property
    def save_on_destroy(self) -> bool:
        return self._save_on_destroy

    @save_on_destroy.setter
    def save_on_destroy(self, val: bool) -> None:
        assert isinstance(val, bool)
        self._save_on_destroy = val

    # --------------------------------------------------------------------
    # persistence

    def load(self) -> None:
        """
        Loads the data from the disk if it exists and updates this
        dictionary.
        """
        if self.path.exists():
            with open(self.path, "r") as f:
                data = json.load(f)
            self.update(data)

    def save(self) -> None:
        """
        Saves the ScribbleDictionary data to a persistent state
        """
        # -- Ensure the location to save to exists, otherwise the
        # -- write will fail
        if not STORAGE_DIRECTORY.exists():
            os.makedirs(STORAGE_DIRECTORY)

        # -- Serialise to json - we wrap this in a try/except in case
        # -- the data is not json serialisable.
        try:
            s = json.dumps(self, indent=2, sort_keys=True)

        except BaseException:
            raise Exception(
                "Could not encode the data within the Scribble Dictionary "
                "to JSON. Please ensure any stored data can be serialised "
                "to JSON."
            )

        # -- Write the data out
        with open(self.path, "w") as f:
            f.write(s)

    # --------------------------------------------------------------------
    # utils

    def _check_key(self, key: Any) -> None:

        if not isinstance(key, str):
            raise KeyError("key must be a string")

    # --------------------------------------------------------------------
    # mapping interface

    def keys(self) -> KeysView:
        return self._data.keys()

    def values(self) -> ValuesView:
        return self._data.values()

    def items(self) -> ItemsView:
        return self._data.items()

    def clear(self) -> None:
        self._data.clear()

    def pop(self, key: str, default: Any = ...) -> Any:

        self._check_key(key)
        try:
            return self._data.pop(key)
        except KeyError:
            if default is ...:
                raise
            return default

    def update(self, d: Mapping) -> None:
        if any(not isinstance(key, str) for key in d):
            raise ValueError("all keys must be strings")
        self._data.update(d)


    def __bool__(self) -> bool:
        return bool(self._data)

    def __contains__(self, key: str) -> bool:
        return key in self._data

    def __getitem__(self, key):
        return self._data[key]

    def __setitem__(self, key: str, val: Any) -> None:
        self._check_key(key)
        self._data[key] = val

    def __delitem__(self, key: str) -> None:
        del self._data[key]
        
    def __eq__(self, other: Any) -> bool:
        return self._data == other
    
    def __iter__(self) -> Iterator:
        return iter(self._data)

    def __len__(self) -> int:
        return len(self._data)

    def __repr__(self) -> str:
        return f"{type(self).__name__}({self._data})"

    def __del__(self) -> None:
        if self.save_on_destroy:
            self.save()


# ----------------------------------------------------------------------------


def get(name: str) -> ScribbleDictionary:
    """
    This will attempt to retrieve a Scribble Dictionary, first from
    the cached instances and then from the STORAGE_DIRECTORY. If it
    does not exist, an empty Scribble Dictionary will be created which
    will be savable with the given name.
    
    :param name: Unique name to access a specific scribble
        dictionary.
    :type name: str
    
    :return: ScribbleDictionary
    """
    return ScribbleDictionary(name)


def exists(name: str) -> bool:
    return name in _CACHE or _CACHE[name].path.exists()
