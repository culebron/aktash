from os.path import exists
from aktash import autoargs_once
from argh import CommandError

@autoargs_once
def main(input_filename):
	if not exists(input_filename):
		raise CommandError(f'file {input_filename} does not exist')
		
	from aktash.utils import file_info, FILE_INFO_DESC
	for k, v in file_info(input_filename).items():
		print(f'{FILE_INFO_DESC[k]}: {v}')
