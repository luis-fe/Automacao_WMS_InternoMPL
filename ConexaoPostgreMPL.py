import psycopg2

def conexao():
    db_name = "Reposicao"
    db_user = "postgres"
    db_password = "Master100"
    db_host = "localhost"
    portbanco = "5432"

    return psycopg2.connect(dbname=db_name, user=db_user, password=db_password, host=db_host, port=portbanco)
