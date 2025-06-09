from .functions import float_from_str


def test_float_from_str():

    values = ["+4", "-4", "+3.0", "-3.0", "a4", "4a", "a4.0", "4.0a"]
    result = [4.0, -4.0, 3.0, -3.0, None, None, None, None]

    for value, result in zip(values, result):
        assert float_from_str(value) == result
