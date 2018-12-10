from unittest import TestCase
from unittest.mock import Mock
from azul.service.responseobjects.buffer import FlushableBuffer


class FlushableBufferTest(TestCase):
    def test_not_flushed_because_of_no_data(self):
        test_min_size = 5
        mock_callback = Mock()
        with FlushableBuffer(test_min_size, mock_callback) as fb:
            mock_callback.assert_not_called()
        mock_callback.assert_not_called()
        self.assertEqual(0, fb.remaining_size)

    def test_not_flushed_because_of_insufficient_data(self):
        test_min_size = 5
        mock_callback = Mock()
        with FlushableBuffer(test_min_size, mock_callback) as fb:
            fb.write(b'?' * 4)
            fb.flush()
            mock_callback.assert_not_called()
            self.assertEqual(4, fb.remaining_size)
        mock_callback.assert_called_once()
        self.assertEqual(0, fb.remaining_size)

    def test_force_flush_with_insufficient_data(self):
        test_min_size = 5
        mock_callback = Mock()
        with FlushableBuffer(test_min_size, mock_callback) as fb:
            fb.write(b'?' * 4)
            mock_callback.assert_not_called()
        mock_callback.assert_called_once()
        self.assertEqual(0, fb.remaining_size)

    def test_flush_with_sufficient_data(self):
        test_min_size = 5
        mock_callback = Mock()
        with FlushableBuffer(test_min_size, mock_callback) as fb:
            fb.write(b'?' * 5)
            mock_callback.assert_called_once()
        mock_callback.assert_called_once()
        self.assertEqual(0, fb.remaining_size)

    def test_multiple_flush_ok(self):
        test_min_size = 5
        mock_callback = Mock()
        with FlushableBuffer(test_min_size, mock_callback) as fb:
            fb.write(b'?' * 4)
            self.assertEqual(4, fb.remaining_size)
            self.assertEqual(0, mock_callback.call_count)
            fb.write(b'?' * 15)
            self.assertEqual(0, fb.remaining_size)
            self.assertEqual(1, mock_callback.call_count)
            fb.write(b'?' * 7)
            self.assertEqual(2, mock_callback.call_count)
            fb.write(b'?' * 4)
            self.assertEqual(2, mock_callback.call_count)
        self.assertEqual(3, mock_callback.call_count)
        self.assertEqual(0, fb.remaining_size)
