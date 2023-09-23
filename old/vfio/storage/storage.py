import os
from . import lookup
import hashlib


#
# class StorageMeta:
#     def __init__(
#             self,
#             batch_size: int,
#             batch_interface_name: str
#     ):
#         self.batch_size = batch_size
#         self.__batch_interface_name = batch_interface_name
#         self.batch_interface = batchif.lookup_interface(batch_interface_name)
#
#     def __eq__(self, other):
#         assert isinstance(other, StorageMeta), type(other)
#         return self.batch_size == other.batch_size \
#             and self.__batch_interface_name == other.__batch_interface_name
#
#     def to_json(self):
#         return {
#             'batch_size': self.batch_size,
#             'batch_interface': self.__batch_interface_name
#         }
#
#     @classmethod
#     def from_json(cls, data):
#         return cls(
#             batch_size=data['batch_size'],
#             batch_interface_name=data['batch_interface']
#         )
#

class StorageIO:
    def __init__(self, path, array_interface):
        os.makedirs(path, exist_ok=True)
        self.__root_path = path

        def array_interface_impl(name):
            if callable(array_interface):
                aif_name = array_interface(name)
            else:
                aif_name = array_interface
            return lookup.lookup_interface(aif_name)

        self.__array_interface = array_interface_impl

    @classmethod
    def __sub_dir_name(cls, index, name):
        if index == 'meta':
            return index
        else:
            return hashlib.md5(f'{index:016}{name}'.encode('ascii')).hexdigest()[:2]

    def __path_to_batch_data(self, index, name, aif_name=None):
        if index is None:
            index = 'meta'
        sub_dir_name = self.__sub_dir_name(index, name)
        if aif_name is None:
            search_target = os.path.join(self.__root_path, sub_dir_name)
            if os.path.exists(search_target):
                candidates = [
                    file_name
                    for file_name in os.listdir(search_target)
                    if file_name.startswith(f'{index}_{name}')
                ]
            else:
                candidates = []
            if candidates:
                assert len(candidates) == 1, candidates
                file_name = candidates[0]
                aif_name = os.path.splitext(file_name)[0].split('__')[1]
            else:
                file_name = f'{index}_{name}__unknown.dat'
        else:
            file_name = f'{index}_{name}__{aif_name}.dat'
        path = os.path.join(
            self.__root_path,
            sub_dir_name,
            file_name
        )
        return path, aif_name

    def put(self, index, name, array):
        aif = self.__array_interface(name)
        buffer = aif.forward(array)
        path, _ = self.__path_to_batch_data(index, name, aif.name)
        dir_path = os.path.dirname(path)
        os.makedirs(dir_path, exist_ok=True)
        with open(path, 'wb') as f:
            f.write(buffer)

    def load(self, index, name):
        path, aif_name = self.__path_to_batch_data(index, name)
        if not os.path.exists(path):
            return None
        with open(path, 'rb') as f:
            buffer = f.read()
        aif = lookup.lookup_interface(aif_name)
        array = aif.backward(buffer)
        return array

    # def entry_count(self):
    #     max_index = None
    #     for name in os.listdir(self.__root_path):
    #         index_part = name.split('_')[0]
    #         if index_part == 'meta':
    #             continue
    #         index = int(index_part)
    #         if max_index is None or max_index < index:
    #             max_index = index
    #     if max_index is None:
    #         return 0
    #     else:
    #         return max_index + 1
#
#
# class StorageIO:
#     def __init__(
#             self,
#             path,
#             *,
#             meta: Optional[StorageMeta] = None,
#             overwrite_meta_if_exists=True
#     ):
#         os.makedirs(path, exist_ok=True)
#         self.__root_path = path
#         self.__meta_provided = meta
#         self.__meta = None
#         self.__overwrite_meta_if_exists = overwrite_meta_if_exists
#
#         self.__process_meta()
#
#     @property
#     def meta(self):
#         return self.__meta
#
#     @property
#     def __path_to_storage_meta(self):
#         return os.path.join(self.__root_path, 'meta.json')
#
#     def __path_to_batch_data(self, data_name, batch_index):
#         return os.path.join(self.__root_path, f'{batch_index}_data_name.dat')
#
#     def __process_meta(self):
#         if self.__meta_provided is None:
#             if os.path.exists(self.__path_to_storage_meta):
#                 with open(self.__path_to_storage_meta, 'r') as f:
#                     meta_json = json.load(f)
#                 self.__meta = StorageMeta.from_json(meta_json)
#             else:
#                 raise ValueError('Metadata is not given and does not exist')
#         else:
#             if os.path.exists(self.__path_to_storage_meta):
#                 with open(self.__path_to_storage_meta, 'r') as f:
#                     meta_json = json.load(f)
#                 meta_on_disk = StorageMeta.from_json(meta_json)
#                 if meta_on_disk == self.__meta_provided:
#                     self.__meta = meta_on_disk
#                 else:
#                     if self.__overwrite_meta_if_exists:
#                         self.__meta = meta_on_disk
#                         warnings.warn('metadata overwrote from disk')
#                     else:
#                         raise ValueError('Metadata is not matched')
#             else:
#                 self.__meta = self.__meta_provided
#                 with open(self.__path_to_storage_meta, 'w') as f:
#                     json.dump(self.__meta_provided.to_json(), f, indent=2, sort_keys=True)
#
#     def put(self, batch_index, data_name, array):
#         buffer = self.__meta.batch_interface.forward(array)
#         with open(self.__path_to_batch_data(data_name, batch_index), 'wb') as f:
#             f.write(buffer)
#
#     def load(self, batch_index, data_name) -> tuple:
#         with open(self.__path_to_batch_data(data_name, batch_index), 'rb') as f:
#             buffer = f.read()
#         array = self.__meta.batch_interface.backward(buffer)
#         return array
