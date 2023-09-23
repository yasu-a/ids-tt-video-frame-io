import itertools
from typing import Optional

import cv2
import numpy as np


class AbstractVideoFrameDecoder:
    def __init__(self, path):
        self._path = path

    def frame_count(self):
        raise NotImplementedError()

    def frame_rate(self):
        raise NotImplementedError()

    def iter_frames(self, skip=None) \
            -> tuple[int, float, np.ndarray]:  # index, timestamp, image(rgb)
        raise NotImplementedError()

    def __len__(self):
        return self.frame_count()

    def __iter__(self):
        yield from self.iter_frames()

    def _setup(self):
        pass

    def _finalize(self):
        pass

    def __enter__(self):
        self._setup()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self._finalize()
        return False


class OpenCVVideoFrameDecoder(AbstractVideoFrameDecoder):
    def __init__(self, path):
        super().__init__(path)

        self._cap: Optional[cv2.VideoCapture] = None

    def frame_count(self):
        return int(self._cap.get(cv2.CAP_PROP_FRAME_COUNT))

    def frame_rate(self):
        return float(self._cap.get(cv2.CAP_PROP_FPS))

    def iter_frames(self, skip=None) \
            -> tuple[int, float, np.ndarray]:  # index, timestamp, image(rgb)
        skip = set([] if skip is None else skip)
        self._cap.set(cv2.CAP_PROP_POS_FRAMES, 0)
        for index in itertools.count():
            ret = self._cap.grab()
            if not ret or index in skip:
                continue
            else:
                timestamp = self._cap.get(cv2.CAP_PROP_POS_MSEC) / 1000.0
                _, image = self._cap.retrieve()
                image = cv2.cvtColor(image, cv2.COLOR_BGR2RGB)
                yield index, timestamp, image

    def _setup(self):
        super()._setup()
        self._cap = cv2.VideoCapture(self._path)

    def _finalize(self):
        self._cap.release()
        super()._finalize()
