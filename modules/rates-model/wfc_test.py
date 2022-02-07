#' Init:
cd D:\OneDrive\__Projects\econforecasting\.virtualenvs
econforecasting/Scripts/activate.bat
econforecasting/Scripts/python.exe

import camelot
import tempfile
import requests
import os
import matplotlib

url = 'https://wellsfargo.bluematrix.com/sellside/EmailDocViewer?encrypt=88d2eafa-3a64-4cca-b013-4093132d9c99&mime=pdf&co=wellsfargo&id=replaceme@bluematrix.com&source=mail'
r = requests.get(url) 
pdf_path = os.path.join(tempfile.gettempdir(), 'test.pdf')
with open(pdf_path, 'wb') as f:
    f.write(r.content)

tables = camelot.read_pdf(pdf_path, flavor = 'stream', table_areas = ['35,470,750,90'])
camelot.plot(tables[0], kind='contour').show()
tables[0].df


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
