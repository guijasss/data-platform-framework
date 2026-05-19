# Design
* give preference to functional programming
* don't repeat yourself and keep things simple
* make modular, testable and atomic code
* client code should only handle processing logic, not knowing read and write internals

# Reader
* allow read only selected columns, not everything

# CLI
* fw contract init
    - inicializa um novo data contract

* fw contract check
    - verifica se o data contract criado é válido

* fw task init <layer> <>
    - inicia um novo arquivo de ETL
    ? precisa definir onde é o diretório root de tasks
