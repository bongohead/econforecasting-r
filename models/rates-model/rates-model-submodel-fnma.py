def parse_fnma_pdf(input_file):
	import camelot
	print(input_file)
	parsed_df = camelot.read_pdf(
		input_file,
		pages = '1',
		flavor = 'stream',
		table_areas = ['150, 550, 650, 250']
		)[0].df
	return parsed_df

def print_model(file):
	print(file)
