import camelot

def read_model(file):
	parsed_df = camelot.read_pdf(
		file,
		pages = '1',
		flavor = 'stream',
		table_areas = ['150, 550, 650, 250']
		)[0].df
	return parsed_df
	
