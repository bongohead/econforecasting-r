#' Init:
# cd D:\OneDrive\__Projects\econforecasting\.virtualenvs
# econforecasting/Scripts/activate.bat
# econforecasting/Scripts/python.exe

# %% Load
import camelot
import tempfile
import requests
import os
import matplotlib
from PyPDF2 import PdfReader
# %% Doc
url = 'https://wellsfargo.bluematrix.com/sellside/EmailDocViewer?encrypt=88d2eafa-3a64-4cca-b013-4093132d9c99&mime=pdf&co=wellsfargo&id=replaceme@bluematrix.com&source=mail'
r = requests.get(url) 
pdf_path = os.path.join(tempfile.gettempdir(), 'test.pdf')
with open(pdf_path, 'wb') as f:
    f.write(r.content)

# %% Parse Text ONly
import re
reader = PyPDF2.PdfFileReader(pdf_path)
text_raw = reader.pages[0].extractText()
# re.DOTALL is necessary for multiline due to linebreaks
# Capturing group to includ Real Gross Domestic Product but not Forecast Rates
text = re.findall(r'(Real Gross Domestic Product.*)Forecast as of', text_raw, re.DOTALL)[0]

# %% Parse
text_split = [x for x in text.splitlines() if not re.match('q\d', x) and not re.match('Inflation Indicators', x) and not re.match('Quarter-End Interest Rates', x)]
row_length = [i for i, x in enumerate(text_split) if re.match('Personal Consumption', x)][0] 
text_by_row = [text_split[x:x+row_length] for x in range(0, len(text_split), row_length)]

col_dict = {}
for i, t in enumerate(text_split):
    if i % row_length == 0 :
        col_dict[t] = text_split[(i + 1):(i + row_length)]

pd.DataFrame(col_dict)

# %% 
head_text = re.findall(r'(.*)Real Gross Domestic Product', text_raw, re.DOTALL)[0]
head_split = [x for x in map(lambda x: x.strip(), head_text.splitlines()) if len(x) in [2, 4]]
#row_length = [i for i, x in enumerate(text_split) if re.match('Personal Consumption', x)][0] 

# %%
# Full
# tables = camelot.read_pdf(pdf_path, flavor = 'stream', table_areas = ['35,470,750,90'])

# Body ONly
table_body_parse = camelot.read_pdf(pdf_path, flavor = 'stream', table_areas = ['30,425,780,90'])

camelot.plot(table_body_parse[0], kind='contour').show()
table_body = table_body_parse[0].df

table_head_parse = camelot.read_pdf(pdf_path, flavor = 'stream', table_areas = ['30,445,780,430'])
camelot.plot(table_head_parse[0], kind='contour').show()
camelot.plot(table_head_parse[0], kind='grid').show()
table_head = table_head_parse[0].df


# %% Clean
table_head

# svglib = import('svglib')
# reportlab = import('reportlab')
# camelot = import('camelot')
# 
# url = 'https://wellsfargo.bluematrix.com/sellside/EmailDocViewer?encrypt=88d2eafa-3a64-4cca-b013-4093132d9c99&mime=pdf&co=wellsfargo&id=replaceme@bluematrix.com&source=mail'
# img_path = file.path(wfc_dir, paste0('zzz', 'testdated', '.svg'))
# pdf_path = file.path(wfc_dir, paste0('zzz', 'testdated', '.pdf'))
# 
# httr::GET(url, httr::write_disk(img_path, overwrite = TRUE))
# img = svglib$svglib$svg2rlg(img_path)
# reportlab$graphics$renderPDF$drawToFile(img, pdf_path)
# normalizePath(pdf_path)

# %%
