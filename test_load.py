import os

import vfio

if __name__ == '__main__':
    video_name = '20230205_04_Narumoto_Harimoto'
    loader = vfio.load(
        storage_path=rf'G:\iDSTTVideoFrameDump\{video_name}.db'
    )
    with loader:
        for index, ts, image in loader:
            print(index, ts)
