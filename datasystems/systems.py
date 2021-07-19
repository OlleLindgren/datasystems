from __future__ import annotations # from DataSystems

import os
import json
import re
from pathlib import Path
from typing import Callable, Counter, Iterable, List, Tuple

FILENAME_REGEX = re.compile(r"[^a-zA-Z0-9-]")

class DataSystem:

    root: Path
    hierarchy: List[str]
    __config_filename: str = '.datasystems-config.json'

    def __init__(self, root: Path, hierarchy: List[str]) -> None:
        # Initialize a DataSystem object

        # Validate and set root
        assert os.path.isabs(root), 'root must be absolute'
        assert os.path.isdir(Path(root).parent), 'root parent must exist'
        self.root = Path(root)
        
        # Find config if exists, and validate hierarchy
        config_file = self.find_config(self.root)
        config = self.read_config(config_file) if config_file else {}
        if config_file:
            _hierarchy = config['hierarchy']
            assert _hierarchy==hierarchy, f'Invalid hierarchy for root={self.root}. Given={hierarchy}, found={_hierarchy}'

        # Set hierarchy
        self.hierarchy = hierarchy

        # Create root if necessary
        if not os.path.isdir(self.root):
            os.mkdir(self.root)

        # Create config if necessary
        if not config_file:
            config['hierarchy'] = hierarchy
            config['structure'] = {}
            self.write_config(self.root, config)

    @staticmethod
    def write_config(root: Path, config: dict) -> None:
        # Write configuration to file
        with open(os.path.join(root, DataSystem.__config_filename), 'w+') as f:
            f.write(json.dumps(config, indent=2))

    @staticmethod
    def read_config(config_file: Path) -> dict:
        # Read config file
        with open(config_file, 'r') as f:
            return json.loads(f.read())

    @staticmethod
    def find_config(root: Path) -> Path:
        # Find config file in root directory
        _config_filename = Path(os.path.join(root, DataSystem.__config_filename))
        if os.path.isfile(_config_filename):
            return _config_filename
        else:
            return None

    @staticmethod
    def is_system(root: Path) -> bool:
        # Check if a directory is a datasystem
        return DataSystem.find_config(root) is not None

    @staticmethod
    def from_config(root: Path, config: dict) -> DataSystem:
        # Create a DataSystem from a root folder and a config file
        return DataSystem(root, **config)

    @staticmethod
    def from_root(root: Path) -> DataSystem:
        # Create a DataSystem from a root folder
        config_file = DataSystem.find_config(root)
        if config_file:
            return DataSystem(root, **DataSystem.read_config(config_file))
        else:
            raise ValueError(f'Cannot create DataSystem: root {root} does not contain {DataSystem.__config_filename}')

    def name(self, *args, **kwargs) -> Path:
        # Get filename of entry

        # Set index of argument currently being parsed
        _index=0

        # Start path with self.root
        path=[self.root]

        # Add args to path
        for arg in args:
            path.append(DataSystem.sanitize_string(arg))
            _index += 1

        # Add kwargs to path. assert kwargs correctly given in relation to hierarchy.
        for k, v in kwargs.items():
            assert _index < len(self.hierarchy), f'Too many arguments: {args}, {kwargs}'
            assert k==self.hierarchy[_index], f'Invalid argument order: {args}, {kwargs}'
            path.append(DataSystem.sanitize_string(v))
            _index += 1

        # Remember, path is a list starting with self.root. 
        # hence len(path)==len(hierarchy)+1 should hold
        assert len(path) == len(self.hierarchy)+1, f'Argument count must exactly match hierarchy: {len(path)-1}!={len(self.hierarchy)}'

        return Path(os.path.join(*path))

    @staticmethod
    def navigate_structures(keys, structure) -> dict:
        refs = [structure]
        for key in keys:
            if key in refs[-1]:
                refs.append(refs[-1][key])
            else:
                refs[-1][key] = {}
                refs.append(refs[-1][key])
        return refs[-1]

    def make_meta_entry(self, path: Path, schema: dict) -> dict:
        # Make structures entry

        # If path is absolute, convert path to relative path from DATA_ROOT/PACKAGE_KEY 
        # i.e. /usr/../home/ise/ise-data-storage/fmp/fooo/bar -> foo/bar
        if os.path.isabs(path):
            relpath = os.path.relpath(path, self.root)
        else:
            relpath = path

        return {"path": relpath, "schema": schema}

    def add(self, path: Path, schema: dict) -> None:

        meta_entry = self.make_meta_entry(path, schema)
        keys = meta_entry['path'].split(os.path.sep)

        config_file = self.find_config(self.root)
        config = self.read_config(config_file)

        self.navigate_structures(keys, config['structure']).update(meta_entry)

        self.write_config(self.root, config)

    def infer_keys(self, path: Path):
        # Infer keys from path
        if os.path.isabs(path):
            return os.path.relpath(path, self.root).split(os.path.sep)
        else:
            return str(path).split(os.path.sep)

    def structure(self) -> dict:
        return self.read_config(self.find_config(self.root))['structure']

    @staticmethod
    def __iter_dict(dictionary: dict) -> Iterable[dict]:
        yield from dictionary.values()

    def iter_entries(self) -> Iterable[dict]:
        # Iterate over entries in strucures. 
        _structure = self.structure()
        for entry in self.__iter_dict(_structure):
            if isinstance(entry, dict):
                if 'schema' in entry:
                    yield entry
                else:
                    yield from self.__iter_dict(entry)

    def sanitize_string(string: str) -> str:
        return FILENAME_REGEX.sub('', string).strip().lower()

    def iter_filter(self, **filters) -> Iterable[dict]:
        # Iterate over entries where key is in schema, and filters are OK

        # Track what entries have been yielded
        yielded: Counter = Counter()
        _hash: Callable = lambda key: sum((hash(k) for k in entry['schema'].keys())) + hash(entry['path'])

        # For every entry in system
        for entry in self.iter_entries():
            # Iterate over schema keys
            # If not already yielded, and matching key
            if not yielded[_hash(entry)]:
                # If any filters, check them. If not, yield.
                if filters:
                    # Infer system keys, and use system keys to get absolute name
                    system_keys = self.infer_keys(entry['path'])

                    # Build proper system key dictionary. This way, we are not dependent on
                    # the order of the keys in the datasystem hierarchy.
                    system_keys_dict = {
                        k: v
                        for k, v in zip(self.hierarchy, system_keys)
                    }

                    # All filters ok, key ok => yield entry
                    if all((
                        k in system_keys_dict and 
                        DataSystem.sanitize_string(system_keys_dict[k])==DataSystem.sanitize_string(v)
                        for k, v in filters.items()
                    )):
                        yielded[_hash(entry)] = True
                        yield entry
                else:
                    # No filters, key ok => yield entry
                    yielded[_hash(entry)] = True
                    yield entry

    def find(self, key: str, **filters) -> Iterable[dict]:
        # Iterate over entries where key is in schema, and filters are OK

        # Track what entries have been yielded
        yielded: Counter = Counter()
        _hash: Callable = lambda entry: sum((hash(k) for k in entry['schema'].keys())) + hash(entry['path'])

        # For every entry in system
        for entry in self.iter_filter(**filters):
            # Iterate over schema keys
            for k in entry['schema'].keys():
                # If not already yielded, and matching key
                if not yielded[_hash(entry)] and DataSystem.sanitize_string(key) == DataSystem.sanitize_string(k):
                    # If any filters, check them. If not, yield.
                    yielded[_hash(entry)] = True
                    yield entry

    def infer_structure(self, glob_string: str, schema_fun: Callable, cut_levels: int=0) -> None:
        for i, file in enumerate(sorted(self.root.rglob(glob_string))):
            try:
                # Infer keys from file
                keys = self.infer_keys(file)
                # If too many, remove the last one (hopefully the filename, so the folder will fit)
                if len(keys) > len(self.hierarchy):
                    keys = keys[:-1]
                # Check valid hierarchy
                assert len(keys) == len(self.hierarchy), f'failed to fit file {file} int hierarchy {self.hierarchy}'
                # Create schema with user-provided callable
                schema = schema_fun(file)
                # Add path is the (cut_levels)'th parent of this leaf
                if cut_levels == 0:
                    add_path = self.name(*keys)
                else:
                    assert len(keys)+cut_levels >= len(self.hierarchy), f'invalid file: {file}'
                    while len(keys) < len(self.hierarchy):
                        keys.append('a')
                    add_path = self.name(*keys).parents[cut_levels-1]
                self.add(add_path, schema)
            except AssertionError as a:
                print(f'Skipping file {i+1}: {file} due to AssertionError: {a}')
