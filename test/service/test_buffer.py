from unittest import TestCase
from unittest.mock import Mock
from azul.service.responseobjects.buffer import FlushableBuffer


class FlushableBufferTest(TestCase):
    def test_not_flushed_because_of_no_data(self):
        test_min_size = 5
        mock_callback = Mock()
        with FlushableBuffer(test_min_size, mock_callback) as fb:
            fb.flush()  # The mock callback is not triggered.
            mock_callback.assert_not_called()  # The mock callback is not triggered due to insufficient data.
        # When the buffer is closed, the mock callback will not be triggered due to no remaining data.
        mock_callback.assert_not_called()
        self.assertEqual(0, fb.remaining_size)

    def test_not_flushed_because_of_insufficient_data(self):
        test_min_size = 5
        mock_callback = Mock()
        with FlushableBuffer(test_min_size, mock_callback) as fb:
            fb.write(b'?' * 4)
            fb.flush()
            mock_callback.assert_not_called()  # The mock callback is not triggered due to insufficient data.
            self.assertEqual(4, fb.remaining_size)
        mock_callback.assert_called_once()  # When the buffer is closed, the mock callback will be triggered.
        self.assertEqual(0, fb.remaining_size)

    def test_force_flush_with_insufficient_data(self):
        test_min_size = 5
        mock_callback = Mock()
        with FlushableBuffer(test_min_size, mock_callback) as fb:
            fb.write(b'?' * 4)
            mock_callback.assert_not_called()  # The mock callback is not triggered due to insufficient data.
        mock_callback.assert_called_once()  # The last call is automatically triggered when the buffer is closed.
        self.assertEqual(0, fb.remaining_size)

    def test_flush_with_sufficient_data(self):
        test_min_size = 5
        mock_callback = Mock()
        with FlushableBuffer(test_min_size, mock_callback) as fb:
            fb.write(b'?' * 5)
            mock_callback.assert_not_called()  # The mock callback is not triggered due to insufficient data.
        mock_callback.assert_called_once()  # The last call is automatically triggered when the buffer is closed.
        self.assertEqual(0, fb.remaining_size)

    def test_multiple_flush_ok(self):
        test_min_size = 5
        mock_callback = Mock()
        with FlushableBuffer(test_min_size, mock_callback) as fb:
            fb.write(b'?' * 5)
            fb.write(b'?' * 15)
            fb.flush()
            fb.write(b'?' * 7)
            fb.flush()
            self.assertEqual(2, mock_callback.call_count)
            fb.write(b'?' * 4)
            self.assertEqual(2, mock_callback.call_count)  # The callback is not triggered yet.
        # At this point, the last call is automatically triggered when the buffer is closed.
        self.assertEqual(3, mock_callback.call_count)
        self.assertEqual(0, fb.remaining_size)
