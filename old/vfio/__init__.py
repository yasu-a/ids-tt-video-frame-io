import os

import numpy as np
from tqdm import tqdm

from . import decode
from . import storage
from . import storage

DEFAULT_FRAME_DECODER = decode.OpenCVVideoFrameDecoder


def preprocess(image):
    return image[::2, ::2]  # quarter


DEFAULT_BATCH_SIZE = 16


class Dumper:
    def __init__(
            self,
            video_path,
            storage_path,
            array_interface_large='gzip_compress',
            array_interface_small='raw',
            batch_size=DEFAULT_BATCH_SIZE,
            frame_decoder=DEFAULT_FRAME_DECODER,
    ):
        self.__video_path = video_path

        def array_interface(name):
            if name == 'image':
                return array_interface_large
            else:
                return array_interface_small

        self.__storage_io = storage.StorageIO(storage_path, array_interface)
        self.__batch_size = batch_size
        self.__frame_decoder = frame_decoder

    def dump(self):
        with self.__frame_decoder(self.__video_path) as decoder:
            next_batch_index = self.__storage_io.load(None, 'next_batch_index')
            if next_batch_index is None:
                next_batch_index = np.zeros(1, dtype=int)
                self.__storage_io.put(None, 'next_batch_index', next_batch_index)

            batch_frame_mapping = self.__storage_io.load(None, 'batch_frame_mapping')
            if batch_frame_mapping is None:
                batch_frame_mapping = np.zeros(decoder.frame_count(), dtype=int) - 1
                self.__storage_io.put(None, 'batch_frame_mapping', batch_frame_mapping)

            frame_index_vec = np.arange(decoder.frame_count())
            skip = frame_index_vec[frame_index_vec < next_batch_index[0] * self.__batch_size]
            print(next_batch_index, skip)

            stack = dict(
                image=[],
                index=[],
                timestamp=[]
            )

            it = decoder.iter_frames(skip=skip)
            it = tqdm(it, total=decoder.frame_count())
            it.update(skip.size * self.__batch_size)
            for index, timestamp, image in it:
                stack['index'].append(index)
                stack['timestamp'].append(timestamp)
                stack['image'].append(image)

                stack_size = len(stack['index'])
                if stack_size >= self.__batch_size:
                    # update index mapping
                    batch_frame_mapping[stack['index']] = next_batch_index[0]
                    self.__storage_io.put(None, 'batch_frame_mapping', batch_frame_mapping)

                    # update stack
                    for k in stack.keys():
                        self.__storage_io.put(next_batch_index[0], k, np.stack(stack[k]))
                        stack[k].clear()

                    # update next_batch_index
                    next_batch_index[0] += 1
                    self.__storage_io.put(None, 'next_batch_index', next_batch_index)


video_name = '20230205_04_Narumoto_Harimoto'
dumper = Dumper(
    video_path=os.path.expanduser(
        rf'~\Desktop\idsttvideos\singles\{video_name}.mp4'
    ),
    storage_path=rf'G:\iDSTTVideoFrameDump\{video_name}'
)
dumper.dump()
