from functools import reduce
from typing import Callable

def zip_reduce(a: list, b: list, f: Callable) -> list:
	'''Zip two numpy arrays and reduce by a function'''
	zipped = zip(a,b)
	reduced = map(lambda _: reduce(f, _), zipped)

	return list(reduced)