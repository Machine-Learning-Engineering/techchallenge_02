# Tech Challenge 02

Um projeto Python básico com estrutura organizada e boas práticas.

## 📁 Estrutura do Projeto

```
techchallenge_02/
├── src/                    # Código fonte principal
│   ├── __init__.py
│   ├── main.py            # Módulo principal
│   └── utils.py           # Utilitários e funções auxiliares
├── tests/                 # Testes automatizados
│   ├── __init__.py
│   ├── test_main.py
│   └── test_utils.py
├── config/                # Configurações
│   ├── __init__.py
│   └── settings.py
├── docs/                  # Documentação
│   └── README.md
├── data/                  # Arquivos de dados
│   └── README.md
├── requirements.txt       # Dependências do projeto
├── pyproject.toml        # Configuração do projeto
├── setup_dev.py          # Script de configuração
├── .env.example          # Exemplo de variáveis de ambiente
├── .gitignore            # Arquivos ignorados pelo Git
└── README.md             # Este arquivo
```

## 🚀 Como Começar

### 1. Clone o repositório
```bash
git clone <url-do-repositorio>
cd techchallenge_02
```

### 2. Configure o ambiente de desenvolvimento
```bash
python setup_dev.py
```

### 3. Ative o ambiente virtual
```bash
# Linux/Mac
source venv/bin/activate

# Windows
venv\Scripts\activate
```

### 4. Execute o projeto
```bash
python src/main.py
```

## 🧪 Executando Testes

```bash
# Executar todos os testes
pytest

# Executar testes com coverage
pytest --cov=src

# Executar testes específicos
pytest tests/test_main.py
```

## 📋 Comandos Úteis

```bash
# Instalar novas dependências
pip install <pacote>
pip freeze > requirements.txt

# Formatação de código
black src/ tests/

# Linting
flake8 src/ tests/

# Type checking
mypy src/
```

## 🔧 Configuração

1. Copie `.env.example` para `.env` e ajuste as variáveis conforme necessário
2. Modifique `config/settings.py` para suas necessidades específicas

## 📝 Desenvolvimento

### Adicionando Novos Módulos
1. Crie novos arquivos em `src/`
2. Adicione testes correspondentes em `tests/`
3. Atualize a documentação conforme necessário

### Boas Práticas
- Escreva testes para todo código novo
- Use docstrings para documentar funções
- Siga as convenções PEP 8
- Mantenha o código limpo e legível

## 🤝 Contribuindo

1. Faça um fork do projeto
2. Crie uma branch para sua feature (`git checkout -b feature/AmazingFeature`)
3. Commit suas mudanças (`git commit -m 'Add some AmazingFeature'`)
4. Push para a branch (`git push origin feature/AmazingFeature`)
5. Abra um Pull Request

## 📄 Licença

Este projeto está sob a licença MIT. Veja o arquivo `LICENSE` para mais detalhes.

## 📞 Contato

Seu Nome - seu.email@exemplo.com

Link do Projeto: [https://github.com/seuusuario/techchallenge_02](https://github.com/seuusuario/techchallenge_02)