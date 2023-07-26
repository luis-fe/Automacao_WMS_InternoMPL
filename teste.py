from ipp import IPPPrinter

def imprimir_via_ipp(ip_impressora_windows, nome_impressora, mensagem):
    try:
        # Cria uma conexão com a impressora usando o endereço IP
        ipp_printer = IPPPrinter("http://" + ip_impressora_windows + "/ipp/print")

        # Define os atributos do trabalho de impressão
        attributes = {
            "document-format": "text/plain",
            "requesting-user-name": "Flask Server",
            "job-name": "Trabalho de impressao",
        }

        # Envia o trabalho de impressão
        ipp_printer.print_job(attributes, mensagem.encode("utf-8"))

        print("Impressão concluída.")
    except Exception as e:
        print(f"Erro ao conectar à impressora: {e}")

# Exemplo de uso:
ip_impressora_windows = "192.168.0.101"  # IP da máquina com a impressora Windows
nome_impressora_compartilhada = "NomeDaImpressoraCompartilhada"
mensagem_para_imprimir = "Teste de impressão via IPP."

imprimir_via_ipp(ip_impressora_windows, nome_impressora_compartilhada, mensagem_para_imprimir)
