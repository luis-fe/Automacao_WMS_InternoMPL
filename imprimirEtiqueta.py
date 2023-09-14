import cups
import os
from reportlab.lib.pagesizes import landscape
from reportlab.lib.units import cm
from reportlab.pdfgen import canvas
import tempfile
from reportlab.graphics import barcode
import qrcode


def criar_pdf(saida_pdf, titulo, cliente, pedido, transportadora):
    # Configurações das etiquetas e colunas
    label_width = 7.8 * cm
    label_height = 1.8 * cm

    # Criar o PDF e ajustar o tamanho da página para paisagem com tamanho personalizado
    custom_page_size = landscape((label_width, label_height))

    # Criar um arquivo temporário para salvar o QR code
    with tempfile.NamedTemporaryFile(delete=False, suffix=".png") as temp_qr_file:
        qr_filename = temp_qr_file.name

        c = canvas.Canvas(saida_pdf, pagesize=custom_page_size)

        # Título centralizado
        c.setFont("Helvetica-Bold", 10)
        title = titulo
        c.drawString(0.3 * cm, 2.1 * cm, title)

        qr = qrcode.QRCode(version=1, box_size=int(2.2 * cm), border=0)
        qr.add_data(cliente)  # Substitua pelo link desejado
        qr.make(fit=True)
        qr_img = qr.make_image(fill_color="black", back_color="white")
        qr_img.save(qr_filename)  # Salvar a imagem do QR code no arquivo temporário
        c.drawImage(qr_filename, 7.5 * cm, 0.15 * cm, width=2.2 * cm, height= 2.2 * cm)

        barcode_value = cliente  # Substitua pelo valor do código de barras desejado
        barcode_code128 = barcode.code128.Code128(barcode_value, barHeight=15, humanReadable=True)
        # Desenhar o código de barras diretamente no canvas
        barcode_code128.drawOn(c, 1.9 * cm, 0.3 * cm)

        c.setFont("Helvetica", 10)
        c.drawString(0.3 * cm, 1.6 * cm, "Nº Cliente:")
        c.drawString(0.3 * cm, 1 * cm, "Nº Pedido:")
        c.drawString(0.3 * cm, 0.3 * cm, transportadora)

        c.drawString(4.5 * cm, 1.6 * cm, cliente)
        c.drawString(4.5 * cm, 1 * cm, pedido)

        c.save()

def imprimir_pdf(pdf_file):
    conn = cups.Connection()
    printers = conn.getPrinters()
    printer_name = list(printers.keys())[0]
    job_id = conn.printFile(printer_name,pdf_file,"Etiqueta",{'PageSize': 'Custom.10x0.25cm', 'FitToPage': 'True', 'Scaling': '100','Orientation':'3'})
    print(f"ID {job_id} enviado para impressão")

criar_pdf("teste.pdf", "KIBELUS MODA LTDA", "101603", "305815-1", "BRASPRESS")
#imprimir_pdf("teste.pdf")
