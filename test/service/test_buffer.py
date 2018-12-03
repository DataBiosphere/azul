from unittest import TestCase
from unittest.mock import Mock
from azul.service.responseobjects.buffer import FlushableBuffer


class FlushableBufferTest(TestCase):
    def test_not_flushed_because_of_no_data(self):
        test_min_size = 5
        mock_callback = Mock()
        fb = FlushableBuffer(test_min_size, mock_callback)
        fb.flush()
        mock_callback.assert_not_called()
        self.assertEqual(0, fb.remaining_size)

    def test_not_flushed_because_of_insufficient_data(self):
        test_min_size = 5
        mock_callback = Mock()
        fb = FlushableBuffer(test_min_size, mock_callback)
        fb.write(b'?' * 4)
        fb.flush()
        mock_callback.assert_not_called()
        self.assertEqual(4, fb.remaining_size)

    def test_force_flush_with_insufficient_data(self):
        test_min_size = 5
        mock_callback = Mock()
        fb = FlushableBuffer(test_min_size, mock_callback)
        fb.write(b'?' * 4)
        fb.flush(check_limit=False)
        mock_callback.assert_called_once()
        self.assertEqual(0, fb.remaining_size)

    def test_flush_with_sufficient_data(self):
        test_min_size = 5
        mock_callback = Mock()
        fb = FlushableBuffer(test_min_size, mock_callback)
        fb.write(b'?' * 5)
        fb.flush()
        mock_callback.assert_called_once()
        self.assertEqual(0, fb.remaining_size)

    def test_multiple_flush_ok(self):
        test_min_size = 5
        mock_callback = Mock()
        fb = FlushableBuffer(test_min_size, mock_callback)
        fb.write(b'?' * 5)
        fb.write(b'?' * 15)
        fb.flush()
        fb.write(b'?' * 7)
        fb.flush()
        fb.write(b'?' * 4)
        fb.flush(check_limit=False)
        self.assertEqual(3, mock_callback.call_count)
        self.assertEqual(0, fb.remaining_size)
