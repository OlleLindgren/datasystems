from __future__ import annotations # from DataSystems

import os
from pathlib import Path
from typing import List

class DataSystem:

    root: Path
    hierarchy: List[str]
    __config_name: str = '.datasystems-config.txt'

    def __init__(self, root: Path|str, hierarchy: List[str]) -> None:
        # Initialize a DataSystem object

        # Validate and set root
        assert os.path.isabs(root), 'root must be absolute'
        assert os.path.isdir(Path(root).parent), 'root parent must exist'
        self.root = Path(root)
        
        # Find config if exists, and validate hierarchy
        config = self.find_config(self.root)
        if config:
            assert self.read_config(config) == hierarchy, f'Invalid hierarchy for root={self.root}. Given={hierarchy}, found={config}'

        # Set hierarchy
        self.hierarchy = hierarchy

        # Create root if necessary
        if not os.path.isdir(self.root):
            os.mkdir(self.root)

        # Create config if necessary
        if not config:
            self.write_config(self.root, self.hierarchy)

    @staticmethod
    def write_config(root: Path, hierarchy: List):
        # Write configuration to file
        with open(os.path.join(root, DataSystem.__config_name), 'w+') as f:
            f.write('\n'.join(hierarchy))

    @staticmethod
    def read_config(config_file: Path):
        # Read config file
        with open(config_file, 'r') as f:
            return f.read().strip().split('\n')

    @staticmethod
    def find_config(root: Path):
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
    def from_config(root, config: Path) -> DataSystem:
        # Create a DataSystem from a root folder and a config file
        return DataSystem(root, DataSystem.read_config(config))

    @staticmethod
    def from_root(root) -> DataSystem:
        # Create a DataSystem from a root folder
        config = DataSystem.find_config(root)
        if config:
            return DataSystem(root=root, config=DataSystem.read_config(config))
        else:
            raise ValueError(f'Cannot create DataSystem: root {root} does not contain {DataSystem.__config_name}')
