# Projeto de Exportação e Transformação de Dados de Nota Fiscal

Este projeto automatiza o processo de exportação e transformação de dados de XML de notas fiscais para análise no AWS Athena. Toda a infraestrutura do Google Cloud Platform é provisionada com **Terraform**, e o pipeline de dados é implementado em **Python** utilizando as bibliotecas `pyodbc`, `deltalake`, `pandas`, `pyarrow`, `duckdb`, e uma biblioteca própria chamada `athena-mvsh`.

![Arquitetura do projeto](img/cloud.png)

## Arquitetura do Projeto

1. **Camada Raw (GCS)**
   - **Objetivo**: Receber os arquivos XML de notas fiscais.
   - **Processo**: Um script Python extrai os dados xml de um banco `SQL SERVER`, carregando-o para um bucket GCS na camada raw e mantendo a estrutura original dos dados.

2. **Camada Bronze (GCS)**
   - **Objetivo**: Converter o XML em formato Parquet para otimizar leitura e armazenamento.
   - **Processo**: Uma Google Cloud Function lê os arquivos XML da camada raw, extrai informações relevantes e converte para Parquet. O arquivo Parquet é então salvo na camada bronze.

3. **Camada Silver (Delta Lake no GCS)**
   - **Objetivo**: Estruturar os dados no formato Delta Lake para histórico e controle de versões.
   - **Processo**: Uma segunda Google Cloud Function lê os arquivos Parquet da camada bronze e os escreve em um Delta Lake, na camada silver, permitindo manipulação de dados robusta.

4. **Exportação para Athena (AWS)**
   - **Objetivo**: Disponibilizar os dados para consulta no AWS Athena em uma tabela Iceberg.
   - **Processo**: Usando `pyarrow` e `duckdb`, o Delta Lake da camada silver, também através de uma Google Cloud Function é convertido para o formato compatível com Iceberg e transferido para o Athena, onde pode ser consultado diretamente. A biblioteca `athena-mvsh` simplifica as operações de consulta e manipulação no Athena.

## Estrutura de pastas pro Google Cloud Function

- `cloud_bronze/`: Função para conversão de XML para Parquet.
- `cloud_silver/`: Função para organizar os dados Parquet no Delta Lake.
- `cloud_gold/`: Função para exportar o Delta Lake para o AWS Athena.

## Infraestrutura com Terraform

Toda a infraestrutura, incluindo os buckets GCS, as Cloud Functions e permissões, é provisionada usando **Terraform**. O código Terraform configura:

- Buckets do GCS para as camadas raw, bronze e silver
- Cloud Functions com acionadores para cada etapa do processo
- Permissões de interação com o GCS

Para implementar a infraestrutura, basta rodar os comandos do Terraform:

```bash
cd terraform

terraform init --upgrade
terraform fmt
terraform validate
terraform apply -auto-approve
```

## Conclusão

Este projeto oferece uma oportunidade valiosa para estudar e praticar conceitos fundamentais em engenharia de dados, como ingestão, transformação e organização de dados em camadas. Ao implementar um pipeline com Google Cloud Functions, armazenamento em camadas no Google Cloud Storage e integração com AWS Athena, foi possível explorar o uso de tecnologias modernas e de boas práticas para o processamento de dados em escala.

Com a ajuda de **Terraform** para provisionamento da infraestrutura, o projeto também proporciona experiência prática com a gestão e automação de recursos em nuvem, algo essencial para profissionais de dados. A biblioteca **Python** `athena-mvsh`(https://pypi.org/project/athena-mvsh/) que é de minha autoria, facilita a interação com os recursos do AWS Athena.

Este projeto, além de fortalecer o conhecimento em ferramentas e tecnologias de dados, também oferece uma visão prática sobre os desafios e soluções na engenharia de dados moderna.