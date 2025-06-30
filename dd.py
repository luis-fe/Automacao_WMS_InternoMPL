from PyPDF2 import PdfReader, PdfWriter
import re

# Caminho do arquivo de entrada e saída
input_path = "/mnt/data/Ana Luisa.pdf"
output_path = "/mnt/data/Ana Luisa_editado.pdf"

# Leitura do PDF original
reader = PdfReader(input_path)
writer = PdfWriter()

# Página alvo (segunda página -> índice 1)
target_page_index = 1
page = reader.pages[target_page_index]
text = page.extract_text()

# Substituição do texto desejado
original = r"Período: Desde 09/2013;"
modificado = "Período: 09/2013 a 03/2025 ;"

# Confirmando se o texto existe na página antes de editar
if original in text:
    # Como o PyPDF2 não permite edição direta do texto, será necessário sobrescrever visualmente
    from reportlab.pdfgen import canvas
    from reportlab.lib.pagesizes import letter
    from PyPDF2.pdf import PageObject
    import io

    # Criar um PDF temporário com a alteração
    packet = io.BytesIO()
    can = canvas.Canvas(packet, pagesize=letter)
    # Definir posição para sobrescrever visualmente (ajustada com base na página)
    can.drawString(70, 100, modificado)  # posição aproximada
    can.save()
    packet.seek(0)

    # Ler o PDF temporário e mesclar com a página original
    from PyPDF2 import PdfReader as RLReader
    overlay_pdf = RLReader(packet)
    overlay_page = overlay_pdf.pages[0]

    # Copiar todas as páginas
    for i, pg in enumerate(reader.pages):
        if i == target_page_index:
            pg.merge_page(overlay_page)
        writer.add_page(pg)

    # Salvar o novo PDF
    with open(output_path, "wb") as f:
        writer.write(f)

    output_path
else:
    "Texto original não encontrado na página."