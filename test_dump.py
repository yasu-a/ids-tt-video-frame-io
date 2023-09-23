import os

import vfio

if __name__ == '__main__':
    video_name = '20230205_04_Narumoto_Harimoto'
    vfio.dump(
        video_path=os.path.expanduser(
            rf'~\Desktop\idsttvideos\singles\{video_name}.mp4'
        ),
        storage_path=rf'G:\iDSTTVideoFrameDump\{video_name}.db'
    )
