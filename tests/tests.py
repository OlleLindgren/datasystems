import os
import shutil
from pathlib import Path

# Magic trickery make it possible to import datasystem
import sys
from typing import Callable
sys.path.append(str(Path(__file__).parents[1]))

from datasystems import DataSystem

# Define temporary DataSystem root
root = os.path.join(Path(__file__).parent, 'tmp') # tests/tmp

assert not os.path.isdir(root), 'temp datasystem root exists'

os.mkdir(root)
assert os.path.isdir(root), 'failed to create temp datasystem root'

hierarchy = ['a', 'asdf', 'af1rf3fdf 21']

ds = DataSystem(root=root, hierarchy=hierarchy)
assert ds.hierarchy==hierarchy

# Test naming function
assert Path(os.path.join(root, 'u', 'v', 'w'))==ds.name('u', 'v', 'w')
assert Path(os.path.join(root, '0', 'v', '1e2d21w'))==ds.name('0', 'v', '1e2d21w')

# Test naming function error checking
def exception_raised(fun: Callable, *args, **kwargs):
    try:
        fun(*args, **kwargs)
        return False
    except Exception as e:
        return True

assert exception_raised(ds.name, 'asdf')
assert not exception_raised(ds.name, 'asdf', 'asdfwfADS', '2')
assert exception_raised(ds.name, '-9-9-9-', 'q')
assert exception_raised(ds.name, 'asdf', {'b':'c'})
assert exception_raised(ds.name, 'k', **{h:h for h in hierarchy})
assert not exception_raised(ds.name, **{h:'asdf' for h in hierarchy})
assert exception_raised(ds.name, **{h:'asdf' for h in hierarchy} ,kfe='q')

# Create additional DataSystem
ds2 = DataSystem(root=root, hierarchy=hierarchy)

# Ensure ds2 results are the same
assert ds.name('a', 'b', 'c')==ds2.name('a', 'b', 'c')

# Cleanup
shutil.rmtree(root)
assert not os.path.isdir(root), 'failed to delete datasystem'

print('done')