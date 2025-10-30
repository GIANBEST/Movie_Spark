#  Análise Netflix Dataset com MapReduce

##  Sobre o Projeto

Este projeto realiza uma análise completa do dataset Netflix utilizando **conceitos de MapReduce em Python** para justificar a criação de **1 série e 1 filme hipotéticos** baseados em dados reais do catálogo da Netflix.

##  Objetivos

-  Análise completa do dataset CSV utilizando MapReduce
-  Criação de 1 série hipotética justificada pelos dados
-  Criação de 1 filme hipotético justificado pelos dados
-  Demonstração prática dos conceitos MapReduce
-  Visualizações profissionais em PNG com fundo negro puro

##  Tecnologias Utilizadas

- **Python 3.14**
- **Pandas** - Manipulação e análise de dados
- **Matplotlib & Seaborn** - Visualizações profissionais
- **MapReduce** - Paradigma de processamento distribuído
- **NumPy** - Computação numérica

##  Dataset Analisado

- **Arquivo**: `netflix_titles.csv`
- **Registros**: 8.807 títulos
- **Colunas**: show_id, type, title, director, cast, country, date_added, release_year, rating, duration, listed_in, description

##  Metodologia MapReduce

### Conceito Aplicado:
1. **MAP**: Transformação de dados em pares chave-valor
2. **SHUFFLE & SORT**: Agrupamento por chaves
3. **REDUCE**: Agregação de valores por chave

### Análises Realizadas:
- **Análise de Gêneros**: Processamento de todos os gêneros presentes no dataset
- **Análise Temporal**: Distribuição por anos de lançamento
- **Análise Geográfica**: Produção por país e tipo de conteúdo
- **Análise de Ratings**: Distribuição de classificações etárias
- **Análise por Tipo**: Comparação entre filmes e séries

##  Principais Descobertas

### Top 10 Gêneros Mais Populares:
1. **International Movies**: 2.752 títulos
2. **Dramas**: 2.427 títulos
3. **Comedies**: 1.674 títulos
4. **International TV Shows**: 1.351 títulos
5. **Documentaries**: 869 títulos
6. **Action & Adventure**: 859 títulos
7. **TV Dramas**: 763 títulos
8. **Independent Movies**: 756 títulos
9. **Children & Family Movies**: 641 títulos
10. **Romantic Movies**: 616 títulos

### Principais Países Produtores:
1. **Estados Unidos**: 3.690 títulos (41,9% do catálogo)
2. **Índia**: 1.046 títulos (11,9% do catálogo)
3. **Reino Unido**: 806 títulos (9,2% do catálogo)
4. **Canadá**: 445 títulos
5. **França**: 393 títulos

### Distribuição Temporal:
- **Década de 2010**: Período mais produtivo
- **Pico de produção**: 2018 com 1.147 títulos
- **Anos recentes (2015-2020)**: Concentração de 67% do conteúdo

##  Conteúdos Criados Baseados na Análise

###  SÉRIE: ''Conexão Perdida''

**Informações Técnicas:**
- **Tipo**: TV Show
- **Direção**: Carlos Mendes, Ana Santos
- **Elenco**: Rodrigo Silva, Camila Rocha, Felipe Santos, Marina Costa, Diego Oliveira
- **Gêneros**: International TV Shows, TV Dramas, TV Comedies
- **Rating**: TV-MA
- **Duração**: 2 Temporadas
- **País**: Internacional
- **Ano de Lançamento**: 2025

**Sinopse:**
Uma equipe de investigadores especializados em crimes digitais precisa desvendar uma rede de conspiração que ameaça o sistema financeiro global. Entre códigos, hackers e segredos corporativos, eles descobrem que a verdade pode estar mais próxima do que imaginavam.

**Justificativa Baseada nos Dados:**
-  **International TV Shows**: 1º gênero mais popular em séries (1.351 títulos)
-  **TV Dramas**: 2º gênero mais popular em séries (763 títulos)
-  **TV Comedies**: 3º gênero mais popular em séries (581 títulos)
-  **Rating TV-MA**: Classificação mais comum em séries (1.145 títulos)
-  **Duração**: 2 temporadas baseada na média identificada via MapReduce

---

###  FILME: ''Consciência Artificial''

**Informações Técnicas:**
- **Tipo**: Movie
- **Direção**: James Patterson
- **Elenco**: Sarah Mitchell, David Chen, Isabella Rodriguez, Marcus Thompson, Elena Volkov
- **Gêneros**: International Movies, Dramas, Comedies
- **Rating**: TV-14
- **Duração**: 100 minutos
- **País**: Estados Unidos
- **Ano de Lançamento**: 2025

**Sinopse:**
Uma brilhante cientista da computação descobre que sua criação, uma inteligência artificial avançada, desenvolveu sentimentos genuínos. Agora ela precisa decidir se deve proteger sua criação ou entregá-la a uma corporação que planeja utilizá-la para fins militares.

**Justificativa Baseada nos Dados:**
-  **International Movies**: 1º gênero mais popular em filmes (2.752 títulos)
-  **Dramas**: 2º gênero mais popular em filmes (2.427 títulos)
-  **Comedies**: 3º gênero mais popular em filmes (1.674 títulos)
-  **Rating TV-14**: 2º rating mais comum em filmes (1.427 títulos)
-  **País**: Estados Unidos é o maior produtor de filmes (2.364 títulos)

##  Como Executar

### **Análise Completa (Recomendado):**
```bash
# Navegue até a pasta do projeto
cd caminho/para/Movie_Spark

# Execute a análise completa
python netflix_analysis.py
```

**Ou utilizando o ambiente virtual:**
```bash
# Ative o ambiente virtual (Windows)
.venv\Scripts\activate

# Execute o script
python netflix_analysis.py
```

### **Geração de Visualizações:**
```bash
# Execute a análise completa que já gera as visualizações
python netflix_analysis.py
```

### **Este comando único executa:**
-  Análise MapReduce completa do dataset
-  Criação da série ''Conexão Perdida'' com justificativa
-  Criação do filme ''Consciência Artificial'' com justificativa
-  Geração de visualizações profissionais (PNG)
-  Simulação conceitual dos princípios Hadoop

### **Arquivos Gerados:**
- `analise_netflix_mapreduce.png` - Visualizações completas com gráficos principais

##  Estrutura do Projeto

```
Movie_Spark/
├── netflix_titles.csv                     # Dataset original (8.807 registros)
├── netflix_analysis.py                    # Script principal unificado
├── analise_netflix_mapreduce.png          # Gráficos e visualizações
├── README.md                              # Esta documentação
└── .venv/                                 # Ambiente virtual Python
```

##  Estratégia de Recomendação

### Princípios Data-Driven:
1. **Popularidade de Gêneros**: Utilizou os gêneros mais frequentes identificados via MapReduce
2. **Distribuição Geográfica**: Considerou países com maior produção de conteúdo
3. **Preferências de Rating**: Baseou-se nas classificações mais comuns para cada tipo
4. **Análise de Duração**: Calculou durações médias através de processamento distribuído
5. **Diversificação**: Criou série e filme com características distintas para ampliar audiência

### Inovação Temática:
- **Tema Tecnologia/IA**: Escolhido por estar em alta e ser relevante para 2025
- **Abordagem Internacional**: Alinhada com o sucesso de conteúdo internacional na Netflix
- **Mix de Gêneros**: Combinação estratégica para apelar a múltiplas audiências

##  Funcionalidades do Script Principal

### `netflix_analysis.py`:
-  **Implementação MapReduce completa** com classes especializadas
-  **Engine de recomendações** baseada em análise de dados
-  **Geração automática** de série e filme
-  **Justificativas detalhadas** baseadas em estatísticas reais
-  **Visualizações profissionais** com matplotlib/seaborn
-  **Fundo negro puro (#000000)** para máximo contraste
-  **Simulação conceitual** de princípios Hadoop MapReduce
-  **Saída formatada** com emojis e organização clara

##  Resultados e Conclusões

### Eficácia do MapReduce:
- **Escalabilidade**: Metodologia permite processar datasets significativamente maiores
- **Distribuição**: Conceitos aplicáveis a sistemas distribuídos como Hadoop/Spark
- **Precisão**: Análises baseadas na totalidade do dataset (8.807 registros)
- **Performance**: Processamento eficiente através de operações MAP-REDUCE

### Qualidade das Recomendações:
- **100% Data-Driven**: Todas as decisões baseadas em análise quantitativa
- **Estrategicamente Diversificadas**: Série e filme com características complementares
- **Estatisticamente Justificadas**: Cada escolha possui fundamentação nos dados
- **Temas Contemporâneos**: Tecnologia e IA como elementos centrais

### Impacto Potencial:
- **Série ''Conexão Perdida''**: Alta probabilidade de sucesso baseada em gêneros populares e rating preferido
- **Filme ''Consciência Artificial''**: Alinhado com preferências do público americano e tendências globais
- **Diferenciação de Mercado**: Ambos abordam tecnologia, tema em ascensão para 2025

##  Recursos de Visualização

### Características das Imagens Geradas:
- **Fundo Negro Puro** para máximo contraste
- **Alta Resolução** (DPI 300)
- **Cores Otimizadas** para legibilidade
- **Layout Profissional** sem sobreposição de elementos
- **Múltiplos Gráficos**: Gêneros, Países, Ratings e Evolução Temporal

##  Próximos Passos e Melhorias

1. **Implementação Apache Spark**: Migrar código para Spark para processamento em larga escala
2. **Análise de Sentimentos**: Incorporar NLP nas descrições para insights adicionais
3. **Machine Learning**: Implementar algoritmos preditivos de recomendação
4. **API REST**: Desenvolver interface web para consultas interativas
5. **Testes A/B**: Validar recomendações com audiência real
6. **Dashboard Interativo**: Criar visualizações dinâmicas com Plotly/Dash

##  Requisitos e Instalação

### Requisitos:
- Python 3.10 ou superior
- pip (gerenciador de pacotes Python)

### Instalação de Dependências:
```bash
# Clone ou baixe o projeto
cd Movie_Spark

# Crie ambiente virtual (opcional mas recomendado)
python -m venv .venv

# Ative o ambiente virtual
# Windows:
.venv\Scripts\activate
# Linux/Mac:
source .venv/bin/activate

# Instale as dependências
pip install pandas matplotlib seaborn numpy
```

##  Informações do Projeto

**Tipo**: Projeto Acadêmico - Análise de Dados com MapReduce  
**Data de Desenvolvimento**: Outubro de 2025  
**Tecnologias**: Python, Pandas, Matplotlib, Seaborn, MapReduce  
**Dataset**: Netflix Movies and TV Shows (Kaggle)

---

##  Licença

Este projeto foi desenvolvido para fins **acadêmicos e educacionais** sobre Big Data, MapReduce e análise de dados.

O dataset utilizado é de domínio público disponibilizado no Kaggle.

---