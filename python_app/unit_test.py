import unittest
from calculator import Calculator


class TestOperations(unittest.TestCase):

    def setUp(self):
        self.operator = Calculator()
        return super().setUp()

    def test_sum(self):
        self.assertEqual(self.operator.sum(2, 2, 2, 2), 8, 'The sum is wrong.')

    def test_subtract(self):
        self.assertEqual(self.operator.subtract(2, 2, 2), -2, 'This subtraction is wrong.')

    def test_product(self):
        self.assertEqual(self.operator.product(2, 2, 2, 2, 2, 2), 64, 'This product is wrong.')

    def tearDown(self):
        return super().tearDown()


if __name__ == "__main__":
    unittest.main()