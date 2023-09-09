import unittest

from csv_db.core import hello


class TestHello(unittest.TestCase):
    def test_hello(self):
        self.assertEqual("Hello", hello())


if __name__ == "__main__":
    unittest.main()
