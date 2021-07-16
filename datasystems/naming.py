from __future__ import annotations # from DataSystems

import os
import json
from pathlib import Path
from typing import Iterable, List

class DataSystem:

    root: Path
    hierarchy: List[str]
    __config_name: str = '.datasystems-config.json'

    def __init__(self, root: Path|str, hierarchy: List[str]) -> None:
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
        with open(os.path.join(root, DataSystem.__config_name), 'w+') as f:
            f.write(json.dumps(config))

    @staticmethod
    def read_config(config_file: Path) -> dict:
        # Read config file
        with open(config_file, 'r') as f:
            return json.loads(f.read())

    @staticmethod
    def find_config(root: Path) -> Path:
        # Find config file in root directory
        try:
            return Path(os.path.join(root, next(Path(root).rglob(DataSystem.__config_name))))
        except StopIteration:
            return None

    def name(self, *args, **kwargs) -> Path:
        # Get name of entry

        # Set index of argument currently being parsed
        _index=0

        # Start path with self.root
        path=[self.root]

        # Add args to path
        for arg in args:
            path.append(arg)
            _index += 1

        # Add kwargs to path. assert kwargs correctly given in relation to hierarchy.
        for k, v in kwargs.items():
            assert _index < len(self.hierarchy), f'Too many arguments: {args}, {kwargs}'
            assert k==self.hierarchy[_index], f'Invalid argument order: {args}, {kwargs}'
            path.append(v)
            _index += 1

        # Remember, path is a list starting with self.root. 
        # hence len(path)==len(hierarchy)+1 should hold
        assert len(path) == len(self.hierarchy)+1, f'Argument count must exactly match hierarchy: {len(path)-1}!={len(self.hierarchy)}'

        return Path(os.path.join(*path))

    @staticmethod
    def from_config(root, config: dict) -> DataSystem:
        # Create a DataSystem from a root folder and a config file
        return DataSystem(root, **config)

    @staticmethod
    def from_root(root) -> DataSystem:
        # Create a DataSystem from a root folder
        config_file = DataSystem.find_config(root)
        if config_file:
            return DataSystem(root, **DataSystem.read_config(config_file))
        else:
            raise ValueError(f'Cannot create DataSystem: root {root} does not contain {DataSystem.__config_name}')

    @staticmethod
    def navigate_structures(keys, structure):
        target = structure
        for key in keys:
            if key in target:
                target = target[key]
            else:
                target = target[key] = {}
        return target

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

        config_file = self.find_config()
        config = self.read_config(config_file)

        target = self.navigate_structures(keys, config['structure'])
        target.update(meta_entry)

        self.write_config(self.root, config)

    def structure(self):
        return self.read_config(self.find_config(self.root))['structure']

    def iter_entries(self) -> Iterable[dict]:
        # Iterate over entries in strucures. 

        l = [struct for struct in self.structure() if isinstance(struct, dict)]

        for struct in l:
            if 'schema' in struct.keys():
                yield struct
            else:
                for child in struct.values():
                    if isinstance(child, dict):
                        l.append(child)

    def find(self, key) -> Iterable[dict]:
        # Iterate over entries where key is in schema, and period is correct

        for entry in self.iter_entries():
            if key in entry['schema']:
                yield key
