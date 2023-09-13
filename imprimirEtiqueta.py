from reportlab.lib.pagesizes import landscape
from reportlab.lib.units import cm
from reportlab.pdfgen import canvas
import qrcode
import tempfile
import os
from reportlab.graphics import barcode
from PIL import Image
import cups #Importe o pacote pycups



def imprimir_pdf(pdf_file):
    conn = cups.Connection()
    printers = conn.getPrinters()
    printer_name = printers.keys()[0]
    job_id = conn.printFile(printer_name,pdf_file,"Etiqueta",{'PageSize': 'Custom.10x2.5cm', 'FitToPage': True, 'Scaling': 100})
    print(f"ID {job_id} enviado para impressão")


imprimir_pdf("305815-1.pdf")
os.remove("305815-1.pdf")