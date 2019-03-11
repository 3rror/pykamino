from collections import namedtuple

TimeWindow = namedtuple('TimeWindow', 'start, end')


def sliding_time_windows(interval: TimeWindow, freq, stride=100, chunksize=8):
    """Return a generator of sliding time windows.

    Args:
        interval (TimeWindow): upper and lower bounds
        freq (datetime.timedelta): resolution of each windows
        stride (int, optional):
            Defaults to 100. Offset of each time windows from the previous
            one, expressed as percentage of the resolution.

    Raises:
        ValueError:
            if stride is not a value greater than 0 and less or equal to 100
        ValueError:
            if frequency is greater than the period between start and end

    Returns:
        Generator[TimeWindow]:
            a generator producing tuples like (window_start, window_end)
    """
    # A stride of 0 doesn't make sense because it would mean a 100% overlap
    # creating an infinite loop
    if not 0 < stride <= 100:
        raise ValueError(
            'Stride value must be greater than 0 and less or equal to 100.')

    start = interval.start
    end = interval.end
    if (end - start) < freq:
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
