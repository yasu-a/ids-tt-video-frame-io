from tqdm import tqdm

from . import decode
from .storage import VideoFrameStorage

import datetime


def dump(
        video_path,
        storage_path,
        decoder=decode.OpenCVVideoFrameDecoder
):
    with VideoFrameStorage(storage_path) as sto:
        skip = sto.loaded_frame_indexes()
        with decoder(video_path) as dec:
            bar = tqdm(total=dec.frame_count())
            bar.update(skip.size)
            for index, timestamp, image in dec.iter_frames(skip=skip):
                sto.put(index, timestamp, image)
                if index % 128 == 0:
                    sto.commit()
                bar.update()
                bar.set_description(str(datetime.timedelta(seconds=timestamp)))
            sto.commit()


class StorageReader:
    def __init__(self, sto: VideoFrameStorage):
        self.__sto = sto

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.__sto.close()
        return False

    def __len__(self):
        return self.__sto.entry_count()

    def __getitem__(self, index):
        return self.__sto.get(index)


def load(storage_path):
    sto = VideoFrameStorage(storage_path)
    return StorageReader(sto)
