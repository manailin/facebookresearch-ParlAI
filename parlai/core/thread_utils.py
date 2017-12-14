# Copyright (c) 2017-present, Facebook, Inc.
# All rights reserved.
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree. An additional grant
# of patent rights can be found in the PATENTS file in the same directory.
"""Provides utilities useful for multiprocessing."""

<<<<<<< Updated upstream
from multiprocessing import Lock, RawArray
try:
    # python3
    from collections.abc import MutableMapping
except ImportError:
    # python2
    from collections import MutableMapping
=======
from multiprocessing import Lock, RawArray, RawValue
from collections.abc import MutableMapping
>>>>>>> Stashed changes
import ctypes
import sys

class SharedTable(MutableMapping):
    """Provides a simple shared-memory table of integers, floats, or strings.
    Use this class as follows:

    .. code-block:: python

        tbl = SharedTable({'cnt': 0})
        with tbl.get_lock():
            tbl['startTime'] = time.time()
        for i in range(10):
            with tbl.get_lock():
                tbl['cnt'] += 1
    """

    TYPES = {
        str: ctypes.c_wchar_p,
        int: ctypes.c_int,
        float: ctypes.c_float,
        bool: ctypes.c_bool,
    }

    INDS = {
        str: 0,
        0: str,
        int: 1,
        1: int,
        float: 2,
        2: float,
        bool: 3,
        3: bool,
    }

    def __init__(self, init_dict=None, extra_space=8):
        """Creates a set of shared memory arrays of fixed size.
        Any data in the init dictionary will be put in these shared arrays,
        and can be accessed and modified in child processes.
        A fixed amount of extra space will be allocated for adding new keys.

        For each key, we store the type for that key along with the index
        pointing to the data associated with that key.
        Linear search is used to match keys with their indices, as the
        amount of keys / data is expected to be small.
        """
        self.len = RawValue('i', 0)
        # handle size of init_dict plus extra_space keys per type
        keysz = extra_space * len(self.TYPES)
        if init_dict is not None:
            keysz += len(init_dict)
        self.keys = RawArray(ctypes.c_wchar_p, keysz)
        # pair array of indices (where in its typed array is each value?)
        self.indices = RawArray(ctypes.c_int, keysz)
        # pair array indicating the type of each value
        self.types = RawArray(ctypes.c_byte, keysz)
        # pair array indicated when keys are active (del sets this to false)
        self.active = RawArray(ctypes.c_bool, keysz)

        # arrays is dict of {value_type: array_of_ctype}
        self.arrays = {}

        # current size of each array
        self.sizes = {typ: RawValue('i', 0) for typ in self.TYPES.keys()}
        # stores tensors (they have to maintain their own shared memory)
        self.tensors = {}

        if init_dict:
            tensor_keys = []
            for k, v in init_dict.items():
                if 'Tensor' in str(type(v)):
                    # add tensor to tensor dict--don't try to put in rawarray
                    # we'll pop it from the dict afterwards
                    self.tensors[str(k)] = v
                    tensor_keys.append(k)
                elif type(v) not in self.TYPES:
                    raise TypeError('SharedTable does not support values of ' +
                                    'type ' + str(type(v)))
                else:
                    self.sizes[type(v)].value += 1

            # pop tensors from init_dict
            for k in tensor_keys:
                init_dict.pop(k)

            # create raw arrays for each type we found
            for typ, sz in self.sizes.items():
                self.arrays[typ] = RawArray(self.TYPES[typ], sz.value + extra_space)

            for k, v in init_dict.items():
                val_type = type(v)
                typ_arr = self.arrays[val_type]
                typ_sz = self.sizes[val_type]
                key_idx = self.len.value
                self.keys[key_idx] = str(k)
                self.types[key_idx] = self.INDS[val_type]
                self.indices[key_idx] = typ_sz.value
                self.active[key_idx] = True
                typ_arr[typ_sz.value] = v
                typ_sz.value += 1
                self.len.value += 1

        # initialize any remaining arrays
        for typ, ctyp in self.TYPES.items():
            if typ not in self.arrays:
                self.arrays[typ] = RawArray(ctyp, extra_space)
        self.lock = Lock()

    def __len__(self):
        return self.len.value

    def __iter__(self):
        return iter(self.keys)

    def __contains__(self, key):
        key_idx = self.index(self.keys, key)
        return key_idx >= 0 and self.active[key_idx]

    def __getitem__(self, key):
        """Returns shared value if key is available."""
        key = str(key)
        if key in self.tensors:
            return self.tensors[key]

        key_idx = self.index(self.keys, key)
        if key_idx < 0 or not self.active[key_idx]:
            raise KeyError('Key [{}] not found in SharedTable'.format(key))

        arr_idx = self.indices[key_idx]
        typ = self.INDS[self.types[key_idx]]

        return self.arrays[typ][arr_idx]

    def __setitem__(self, key, value):
        """If key is in table, update it. Otherwise, extend the array to make
        room. This uses additive resizing not multiplicative, since the number
        of keys is not likely to change frequently during a run, so do not abuse
        it.
        Raises an error if you try to change the type of the value stored for
        that key--if you need to do this, you must delete the key first.
        """
        key = str(key)
        val_type = type(value)
        if 'Tensor' in str(val_type):
            self.tensors[key] = value
            return

        if val_type not in self.TYPES:
            raise TypeError('SharedTable does not support type ' + str(type(value)))

        if val_type == str:
            value = sys.intern(value)

        key_idx = self.index(self.keys, key)
        if key_idx >= 0:
            arr_idx = self.indices[key_idx]
            typ = self.INDS[self.types[key_idx]]
            if typ != val_type:
                raise TypeError(('Cannot change stored type for {key} from ' +
                                 '{v1} to {v2}. You need to del the key first' +
                                 ' if you need to change value types.'
                                 ).format(key=key, v1=typ, v2=val_type))
            self.arrays[typ][arr_idx] = value
            self.active[key_idx] = True
        else:
            # add item to array or raise error if out of space
            key_idx = self.len.value
            typ_arr = self.arrays[val_type]
            typ_sz = self.sizes[val_type]
            if key_idx == len(self.keys) or typ_sz == len(typ_arr):
                raise RuntimeError('SharedTable full of data--allocate table '
                                   'with more extra space.')

            self.keys[key_idx] = key
            self.active[key_idx] = True
            self.types[key_idx] = self.INDS[val_type]
            self.indices[key_idx] = typ_sz.value
            typ_arr[typ_sz.value] = value
            typ_sz.value += 1
            self.len.value += 1

    def __delitem__(self, key):
        key = str(key)
        if key in self.tensors:
            del self.tensors[key]

        key_idx = self.index(self.keys, key)
        if key_idx < 0 or not self.active[key_idx]:
            raise KeyError('Key [{}] not found in SharedTable'.format(key))

        # set key to inactive, but remember the key and reuse in the future
        # we don't track empty slots so we can't get the old space back
        self.active[key_idx] = False


    def __str__(self):
        """Returns simple dict representation of the mapping."""
        return '{{{}}}'.format(
            ', '.join(
                '{k}: {v}'.format(k=key, v=self.arrays[typ][idx])
                for key, (idx, typ) in self.idx.items()
            )
        )

    def __repr__(self):
        """Returns the object type and memory location with the mapping."""
        representation = super().__repr__()
        return representation.replace('>', ': {}>'.format(str(self)))

    def get_lock(self):
        return self.lock

    def index(self, arr, key):
        res = [i for i, j in enumerate(arr) if j == key]
        if len(res) == 0:
            return -1
        else:
            return res[0]
