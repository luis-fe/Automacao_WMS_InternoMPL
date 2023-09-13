import cups



def imprimir_pdf(pdf_file):
    conn = cups.Connection()
    printers = conn.getPrinters()
    printer_name = list(printers.keys())[0]
    job_id = conn.printFile(printer_name,pdf_file,"Etiqueta",{'PageSize': 'Custom.10x2.5cm', 'FitToPage': 'True', 'Scaling': '100'})
    print(f"ID {job_id} enviado para impressão")


imprimir_pdf("305815-2.pdf")
