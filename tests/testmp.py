from functools import wraps
from gistalt import autoargs, GS_DEBUG
import inspect

def dec(func):
	func2 = fn2gen(func)
	@wraps(func2)
	def decorated(param):
		if GS_DEBUG: print(f'{func2.__name__}({param})')
		return func2(param)
	return decorated


def fn2gen(func):
	if inspect.isgeneratorfunction(func):
		if GS_DEBUG: print(f'{func.__name__} is generator')
		return func

	@wraps(func)
	def decorated(param):
		yield func(param)

	return decorated


# 1 => 3, 5, 7 => 6, 10, 14 => 17, 21, 25 => 21, 25
# 2 => 6, 10, 14 => 12, 20, 28 => 23, 31, 39 => 23, 31, 39
# 3 => 9, 15, 21 => 18, 30, 42 => 29, 41, 53 => 29, 41, 53


@dec
def g1(param):
	yield 3 * param
	yield None
	yield 5 * param 
	yield 7 * param

@dec
def g2(param):
	return param * 2

@dec
def g3(param):
	yield param + 11

@dec
def g4(param):
	if param > 20:
		return param

	if GS_DEBUG: print('param < 20, not returninng')
	return

params = [1, 2, 3]

@autoargs
def main2():
	def do(params, funcs):
		next_func = funcs[0]
		for p in params:
			if p is None:
				continue

			gen = next_func(p)
			if len(funcs) > 1:
				for val in do(gen, funcs[1:]):
					if val is not None:
						yield val
			else:
				for val in gen:
					if val is not None:
						yield val

	for val in do(params, [g1, g2, g3, g4]):
		print(f'result {val}')		

def main():
	def do(*params):
		chain_stack = []
		current_chain = iter(params)
		level = -1
		funcs = [g1, g2, g3, g4]

		while len(chain_stack) > 0 or current_chain is not None:
			if level + 1 == len(funcs):
				for i in current_chain:
					if i is not None:
						yield i

				current_chain = chain_stack.pop()
				level -= 1
				continue

			try:
				next_val = next(current_chain)
			except StopIteration:
				if len(chain_stack) == 0:
					break

				current_chain = chain_stack.pop()
				level -= 1
				if GS_DEBUG: print(f'level down, {level}')
				continue

			if next_val is None:
				continue

			level += 1
			if GS_DEBUG: print(f'level up, {level}')
			chain_stack.append(current_chain)
			current_chain = iter(funcs[level](next_val))

	for val in do(*params):
		print(f'result {val}')

