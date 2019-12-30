from datetime import datetime, timedelta
from typing import Generator, List, NamedTuple


class TimeWindow(NamedTuple):
    start: datetime
    end: datetime


def sliding_time_windows(interval: TimeWindow, freq: timedelta,
                         stride: int = 100,
                         chunksize: int = 8) -> Generator[List[TimeWindow], None, None]:
    """
    Return a generator of sliding time windows.

    Args:
        interval: upper and lower bounds
        freq: resolution of each windows
        stride:
            Distance in time between a TimeWindow and another, expressed
            as percentage of freq. A value of 100 means ther's no overlap.
        chunksize: number of windows returned for each function call

    Raises:
        ValueError:
            if stride is not a value greater than 0 and less or equal to 100
        ValueError:
            if freq is greater than the period between start and end

    Returns:
        generator producing a list of TimeWindow
    """
    # A stride of 0 doesn't make sense because it would mean a 100% overlap
    # creating an infinite loop
    if not 0 < stride <= 100:
        raise ValueError(
            'Stride value must be greater than 0 and less or equal to 100.')

    start = interval.start
    end = interval.end
    if (interval.end - start) < freq:
        raise ValueError(
            'Frequency must be less than the period between start and end')
    offset = freq * stride / 100
    buffer = []
    while start + freq <= end:
        buffer.append(TimeWindow(start, end=start + freq))
        if len(buffer) >= chunksize:
            yield buffer.copy()
            buffer.clear()
        start += offset
    if buffer:
        yield buffer
