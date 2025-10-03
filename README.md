# DataBank-with-steam-CSV

## Instalação
Clone o repositório para seu ambiente de trabalho

  git clone <repository-url>
  cd Trabalho-banco-de-dados

Instale os requisitos

  pip install -r requirements.txt

Preencha a sessão de parametros com as informações do seu banco de dados, localizado no arquivo sql_insert.py na linha 13

  db_params = {
      'host': 'localhost',
      'database': 'your_database_name',
      'user': 'your_username',
      'password': 'your_password',
      'port': '5432'
  }
