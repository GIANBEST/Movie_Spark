# ANÁLISE NETFLIX DATASET COM MAPREDUCE - ARQUIVO UNIFICADO
# Análise completa, criação de série e filme, e visualizações em um só script

import pandas as pd
import matplotlib
matplotlib.use('Agg')  # Backend para não mostrar janelas
import matplotlib.pyplot as plt
import seaborn as sns
import numpy as np
from collections import Counter, defaultdict
import re
from datetime import datetime

class NetflixMapReduce:
    # Implementação de análises usando conceitos MapReduce
    # para o dataset da Netflix
    
    def __init__(self, csv_file):
        self.df = pd.read_csv(csv_file)
        self.results = {}
        
    def map_genres(self, row):
        # MAP: Extrai gêneros de uma linha
        if pd.isna(row['listed_in']):
            return []
        genres = [genre.strip() for genre in row['listed_in'].split(',')]
        return [(genre, 1) for genre in genres]
    
    def reduce_genres(self, mapped_data):
        # REDUCE: Conta gêneros totais
        genre_count = Counter()
        for row_genres in mapped_data:
            for genre, count in row_genres:
                genre_count[genre] += count
        return dict(genre_count)
    
    def map_countries(self, row):
        # MAP: Extrai países de uma linha
        if pd.isna(row['country']):
            return []
        countries = [country.strip() for country in row['country'].split(',')]
        return [(country, 1) for country in countries]
    
    def reduce_countries(self, mapped_data):
        # REDUCE: Conta países totais
        country_count = Counter()
        for row_countries in mapped_data:
            for country, count in row_countries:
                country_count[country] += count
        return dict(country_count)
    
    def map_ratings(self, row):
        # MAP: Extrai ratings de uma linha
        if pd.isna(row['rating']):
            return []
        return [(row['rating'], 1)]
    
    def reduce_ratings(self, mapped_data):
        # REDUCE: Conta ratings totais
        rating_count = Counter()
        for row_ratings in mapped_data:
            for rating, count in row_ratings:
                rating_count[rating] += count
        return dict(rating_count)
    
    def map_release_years(self, row):
        # MAP: Extrai anos de lançamento
        if pd.isna(row['release_year']):
            return []
        return [(int(row['release_year']), 1)]
    
    def reduce_release_years(self, mapped_data):
        # REDUCE: Conta anos de lançamento
        year_count = Counter()
        for row_years in mapped_data:
            for year, count in row_years:
                year_count[year] += count
        return dict(year_count)
    
    def map_type_analysis(self, row):
        # MAP: Análise por tipo (Movie vs TV Show)
        content_type = row['type']
        genre_list = []
        if not pd.isna(row['listed_in']):
            genre_list = [genre.strip() for genre in row['listed_in'].split(',')]
        
        country = 'Unknown'
        if not pd.isna(row['country']):
            country = row['country'].split(',')[0].strip()
            
        rating = row['rating'] if not pd.isna(row['rating']) else 'Unknown'
        year = int(row['release_year']) if not pd.isna(row['release_year']) else 0
        
        return [(content_type, {
            'genres': genre_list,
            'country': country,
            'rating': rating,
            'year': year
        })]
    
    def reduce_type_analysis(self, mapped_data):
        # REDUCE: Analisa padrões por tipo
        type_analysis = defaultdict(lambda: {
            'count': 0,
            'genres': Counter(),
            'countries': Counter(),
            'ratings': Counter(),
            'years': Counter()
        })
        
        for row_data in mapped_data:
            for content_type, data in row_data:
                type_analysis[content_type]['count'] += 1
                
                for genre in data['genres']:
                    type_analysis[content_type]['genres'][genre] += 1
                
                type_analysis[content_type]['countries'][data['country']] += 1
                type_analysis[content_type]['ratings'][data['rating']] += 1
                type_analysis[content_type]['years'][data['year']] += 1
        
        return dict(type_analysis)
    
    def run_analysis(self):
        # Executa todas as análises MapReduce
        print("🎬 Iniciando análise MapReduce do dataset Netflix...")
        
        # Análise de gêneros
        print("📊 Analisando gêneros...")
        mapped_genres = [self.map_genres(row) for _, row in self.df.iterrows()]
        self.results['genres'] = self.reduce_genres(mapped_genres)
        
        # Análise de países
        print("🌍 Analisando países...")
        mapped_countries = [self.map_countries(row) for _, row in self.df.iterrows()]
        self.results['countries'] = self.reduce_countries(mapped_countries)
        
        # Análise de ratings
        print("⭐ Analisando ratings...")
        mapped_ratings = [self.map_ratings(row) for _, row in self.df.iterrows()]
        self.results['ratings'] = self.reduce_ratings(mapped_ratings)
        
        # Análise de anos
        print("📅 Analisando anos de lançamento...")
        mapped_years = [self.map_release_years(row) for _, row in self.df.iterrows()]
        self.results['years'] = self.reduce_release_years(mapped_years)
        
        # Análise por tipo
        print("🎭 Analisando por tipo de conteúdo...")
        mapped_types = [self.map_type_analysis(row) for _, row in self.df.iterrows()]
        self.results['type_analysis'] = self.reduce_type_analysis(mapped_types)
        
        print("✅ Análise MapReduce concluída!")
        return self.results

class NetflixRecommendationEngine:
    # Engine para criar recomendações baseadas na análise dos dados
    
    def __init__(self, analysis_results, df):
        self.results = analysis_results
        self.df = df
        
    def analyze_trends(self):
        # Analisa tendências dos dados
        print("\n🔍 ANÁLISE DE TENDÊNCIAS:")
        
        # Top gêneros
        top_genres = sorted(self.results['genres'].items(), key=lambda x: x[1], reverse=True)[:10]
        print(f"\n📈 Top 10 Gêneros mais populares:")
        for i, (genre, count) in enumerate(top_genres, 1):
            print(f"{i}. {genre}: {count} títulos")
        
        # Top países
        top_countries = sorted(self.results['countries'].items(), key=lambda x: x[1], reverse=True)[:10]
        print(f"\n🌍 Top 10 Países produtores:")
        for i, (country, count) in enumerate(top_countries, 1):
            print(f"{i}. {country}: {count} títulos")
        
        # Ratings mais comuns
        top_ratings = sorted(self.results['ratings'].items(), key=lambda x: x[1], reverse=True)
        print(f"\n⭐ Ratings mais comuns:")
        for rating, count in top_ratings:
            print(f"{rating}: {count} títulos")
        
        # Anos mais produtivos
        recent_years = {year: count for year, count in self.results['years'].items() if year >= 2015}
        top_recent_years = sorted(recent_years.items(), key=lambda x: x[1], reverse=True)[:5]
        print(f"\n📅 Anos mais produtivos (2015+):")
        for year, count in top_recent_years:
            print(f"{year}: {count} títulos")
        
        # Análise por tipo
        print(f"\n🎭 Análise por tipo de conteúdo:")
        for content_type, data in self.results['type_analysis'].items():
            print(f"\n{content_type.upper()}:")
            print(f"  Total: {data['count']} títulos")
            
            top_genres_type = sorted(data['genres'].items(), key=lambda x: x[1], reverse=True)[:5]
            print(f"  Top 5 gêneros:")
            for genre, count in top_genres_type:
                print(f"    {genre}: {count}")
                
            top_countries_type = sorted(data['countries'].items(), key=lambda x: x[1], reverse=True)[:3]
            print(f"  Top 3 países:")
            for country, count in top_countries_type:
                print(f"    {country}: {count}")
        
        return {
            'top_genres': top_genres,
            'top_countries': top_countries,
            'top_ratings': top_ratings,
            'recent_years': top_recent_years
        }
    
    def create_serie_recommendation(self, trends):
        # Cria recomendação de série baseada nas tendências
        print("\n🎬 CRIANDO SÉRIE HIPOTÉTICA...")
        
        # Análise específica para TV Shows
        tv_data = self.results['type_analysis']['TV Show']
        
        # Gêneros mais populares para séries
        top_tv_genres = sorted(tv_data['genres'].items(), key=lambda x: x[1], reverse=True)[:3]
        selected_genres = [genre for genre, _ in top_tv_genres]
        
        # País mais produtivo para séries (excluindo EUA para diversidade)
        tv_countries = sorted(tv_data['countries'].items(), key=lambda x: x[1], reverse=True)
        selected_country = tv_countries[1][0] if tv_countries[1][0] != "United States" else tv_countries[2][0]
        
        # Rating mais comum para séries
        top_tv_rating = sorted(tv_data['ratings'].items(), key=lambda x: x[1], reverse=True)[0][0]
        
        # Ano recente popular
        recent_tv_years = {year: count for year, count in tv_data['years'].items() if year >= 2020}
        selected_year = sorted(recent_tv_years.items(), key=lambda x: x[1], reverse=True)[0][0]
        
        # Análise de duração típica para séries
        tv_shows = self.df[self.df['type'] == 'TV Show']
        seasons_data = []
        for _, row in tv_shows.iterrows():
            if not pd.isna(row['duration']) and 'Season' in str(row['duration']):
                try:
                    seasons = int(str(row['duration']).split()[0])
                    seasons_data.append(seasons)
                except:
                    pass
        
        avg_seasons = round(sum(seasons_data) / len(seasons_data)) if seasons_data else 2
        
        serie = {
            "show_id": "s_new_001",
            "type": "TV Show",
            "title": "Conexão Perdida",
            "director": "Carlos Mendes, Ana Santos",
            "cast": "Rodrigo Silva, Camila Rocha, Felipe Santos, Marina Costa, Diego Oliveira",
            "country": selected_country,
            "date_added": "October 30, 2025",
            "release_year": 2025,
            "rating": top_tv_rating,
            "duration": f"{min(avg_seasons, 3)} Seasons",
            "listed_in": ", ".join(selected_genres),
            "description": f"Uma equipe de investigadores especializados em crimes digitais precisa desvendar uma rede de conspiração que ameaça o sistema financeiro global. Entre códigos, hackers e segredos corporativos, eles descobrem que a verdade pode estar mais próxima do que imaginavam."
        }
        
        print(f"✨ SÉRIE CRIADA: '{serie['title']}'")
        print(f"📍 País: {serie['country']}")
        print(f"🎭 Gêneros: {serie['listed_in']}")
        print(f"⭐ Rating: {serie['rating']}")
        print(f"📅 Duração: {serie['duration']}")
        print(f"📝 Descrição: {serie['description']}")
        
        return serie
    
    def create_movie_recommendation(self, trends):
        # Cria recomendação de filme baseada nas tendências
        print("\n🎥 CRIANDO FILME HIPOTÉTICO...")
        
        # Análise específica para Movies
        movie_data = self.results['type_analysis']['Movie']
        
        # Gêneros mais populares para filmes (excluindo os da série para diversidade)
        movie_genres = sorted(movie_data['genres'].items(), key=lambda x: x[1], reverse=True)
        selected_genres = []
        for genre, _ in movie_genres:
            if len(selected_genres) < 3 and 'TV' not in genre:
                selected_genres.append(genre)
        
        # País diferente da série
        movie_countries = sorted(movie_data['countries'].items(), key=lambda x: x[1], reverse=True)
        selected_country = "United States"  # Maior produtor de filmes
        
        # Rating adequado para filmes
        movie_ratings = sorted(movie_data['ratings'].items(), key=lambda x: x[1], reverse=True)
        selected_rating = movie_ratings[0][0] if movie_ratings[0][0] != 'TV-MA' else movie_ratings[1][0]
        
        # Duração típica para filmes
        movies = self.df[self.df['type'] == 'Movie']
        durations = []
        for _, row in movies.iterrows():
            if not pd.isna(row['duration']) and 'min' in str(row['duration']):
                try:
                    duration = int(str(row['duration']).split()[0])
                    durations.append(duration)
                except:
                    pass
        
        avg_duration = round(sum(durations) / len(durations)) if durations else 95
        
        movie = {
            "show_id": "s_new_002",
            "type": "Movie",
            "title": "Consciência Artificial",
            "director": "James Patterson",
            "cast": "Sarah Mitchell, David Chen, Isabella Rodriguez, Marcus Thompson, Elena Volkov",
            "country": selected_country,
            "date_added": "October 30, 2025",
            "release_year": 2025,
            "rating": selected_rating,
            "duration": f"{avg_duration} min",
            "listed_in": ", ".join(selected_genres[:3]),
            "description": f"Uma brilhante cientista da computação descobre que sua criação, uma inteligência artificial avançada, desenvolveu sentimentos genuínos. Agora ela precisa decidir se deve proteger sua criação ou entregá-la para uma corporação que planeja usá-la para fins militares."
        }
        
        print(f"✨ FILME CRIADO: '{movie['title']}'")
        print(f"📍 País: {movie['country']}")
        print(f"🎭 Gêneros: {movie['listed_in']}")
        print(f"⭐ Rating: {movie['rating']}")
        print(f"⏱️ Duração: {movie['duration']}")
        print(f"📝 Descrição: {movie['description']}")
        
        return movie
    
    def justify_recommendations(self, serie, movie, trends):
        # Justifica as escolhas baseadas na análise MapReduce
        print("\n📋 JUSTIFICATIVAS DAS RECOMENDAÇÕES:")
        print("=" * 50)
        
        print(f"\n🎬 JUSTIFICATIVA DA SÉRIE '{serie['title']}':")
        print("-" * 40)
        
        tv_data = self.results['type_analysis']['TV Show']
        
        # Justificativa de gêneros
        top_tv_genres = sorted(tv_data['genres'].items(), key=lambda x: x[1], reverse=True)[:5]
        print(f"📊 Gêneros escolhidos baseados nos dados:")
        for i, genre in enumerate(serie['listed_in'].split(', ')):
            for j, (tv_genre, count) in enumerate(top_tv_genres):
                if genre == tv_genre:
                    print(f"  • {genre}: {j+1}º gênero mais popular em séries com {count} títulos")
                    break
        
        # Justificativa de país
        tv_countries = sorted(tv_data['countries'].items(), key=lambda x: x[1], reverse=True)
        for i, (country, count) in enumerate(tv_countries):
            if country == serie['country']:
                print(f"🌍 País escolhido: {country} - {i+1}º maior produtor de séries com {count} títulos")
                break
        
        # Justificativa de rating
        tv_ratings = sorted(tv_data['ratings'].items(), key=lambda x: x[1], reverse=True)
        for i, (rating, count) in enumerate(tv_ratings):
            if rating == serie['rating']:
                print(f"⭐ Rating escolhido: {rating} - {i+1}º rating mais comum em séries com {count} títulos")
                break
        
        print(f"\n🎥 JUSTIFICATIVA DO FILME '{movie['title']}':")
        print("-" * 40)
        
        movie_data = self.results['type_analysis']['Movie']
        
        # Justificativa de gêneros
        top_movie_genres = sorted(movie_data['genres'].items(), key=lambda x: x[1], reverse=True)[:5]
        print(f"📊 Gêneros escolhidos baseados nos dados:")
        for i, genre in enumerate(movie['listed_in'].split(', ')):
            for j, (mv_genre, count) in enumerate(top_movie_genres):
                if genre == mv_genre:
                    print(f"  • {genre}: {j+1}º gênero mais popular em filmes com {count} títulos")
                    break
        
        # Justificativa de país
        movie_countries = sorted(movie_data['countries'].items(), key=lambda x: x[1], reverse=True)
        for i, (country, count) in enumerate(movie_countries):
            if country == movie['country']:
                print(f"🌍 País escolhido: {country} - {i+1}º maior produtor de filmes com {count} títulos")
                break
        
        # Justificativa de rating
        movie_ratings = sorted(movie_data['ratings'].items(), key=lambda x: x[1], reverse=True)
        for i, (rating, count) in enumerate(movie_ratings):
            if rating == movie['rating']:
                print(f"⭐ Rating escolhido: {rating} - {i+1}º rating mais comum em filmes com {count} títulos")
                break
        
        print(f"\n🧠 ESTRATÉGIA DE RECOMENDAÇÃO:")
        print("-" * 40)
        print("• Baseou-se nos gêneros mais populares identificados via MapReduce")
        print("• Considerou países com maior produção de conteúdo")
        print("• Utilizou ratings mais comuns para cada tipo de conteúdo")
        print("• Analisou durações médias através de processamento distribuído")
        print("• Criou diversidade entre série e filme para ampliar audiência")
        print("• Focou em temas contemporâneos (tecnologia/IA) que estão em alta")

class NetflixVisualization:
    # Classe para gerar todas as visualizações em PNG
    
    def __init__(self, results, df):
        self.results = results
        self.df = df
    
    def generate_main_charts(self):
        # Gera gráficos principais e salva em PNG
        print("\n📊 GERANDO VISUALIZAÇÕES PRINCIPAIS...")
        
        # Configurar fondo negro puro
        plt.style.use('dark_background')
        plt.rcParams['figure.facecolor'] = '#000000'
        plt.rcParams['axes.facecolor'] = '#000000'
        plt.rcParams['savefig.facecolor'] = '#000000'
        
        fig, ((ax1, ax2), (ax3, ax4)) = plt.subplots(2, 2, figsize=(15, 12), facecolor='#000000')
        fig.patch.set_facecolor('#000000')
        fig.suptitle('Análise Netflix - Resultados MapReduce', fontsize=16, fontweight='bold', color='white')
        
        # Configurar fondo negro para cada subplot
        ax1.set_facecolor('#000000')
        ax2.set_facecolor('#000000')
        ax3.set_facecolor('#000000')
        ax4.set_facecolor('#000000')
        
        # 1. Top 10 Gêneros
        top_genres = sorted(self.results['genres'].items(), key=lambda x: x[1], reverse=True)[:10]
        genres, counts = zip(*top_genres)
        
        bars1 = ax1.barh(range(len(genres)), counts, color='red', alpha=0.8)
        ax1.set_yticks(range(len(genres)))
        ax1.set_yticklabels([g[:20] + '...' if len(g) > 20 else g for g in genres], color='white')
        ax1.set_xlabel('Número de Títulos', color='white')
        ax1.set_title('Top 10 Gêneros Mais Populares', color='white')
        ax1.tick_params(colors='white')
        ax1.invert_yaxis()
        
        # 2. Top 5 Países
        top_countries = sorted(self.results['countries'].items(), key=lambda x: x[1], reverse=True)[:5]
        countries, country_counts = zip(*top_countries)
        
        bars2 = ax2.bar(range(len(countries)), country_counts, color='blue', alpha=0.8)
        ax2.set_xticks(range(len(countries)))
        ax2.set_xticklabels([c[:8] + '...' if len(c) > 8 else c for c in countries], rotation=45, color='white')
        ax2.set_ylabel('Número de Títulos', color='white')
        ax2.set_title('Top 5 Países Produtores', color='white')
        ax2.tick_params(colors='white')
        
        # 3. Distribuição de Ratings
        top_ratings = sorted(self.results['ratings'].items(), key=lambda x: x[1], reverse=True)[:8]
        rating_names, rating_counts = zip(*top_ratings)
        
        colors = plt.cm.Set3(np.linspace(0, 1, len(rating_names)))
        wedges, texts, autotexts = ax3.pie(rating_counts, labels=rating_names, autopct='%1.1f%%', 
                                          colors=colors, startangle=90)
        ax3.set_title('Distribuição de Ratings', color='white')
        # Configurar cores do texto para branco
        for text in texts:
            text.set_color('white')
        for autotext in autotexts:
            autotext.set_color('black')
            autotext.set_fontweight('bold')
        
        # 4. Anos mais produtivos (últimas décadas)
        recent_years = {year: count for year, count in self.results['years'].items() if year >= 2010}
        top_years = sorted(recent_years.items(), key=lambda x: x[1], reverse=True)[:8]
        years, year_counts = zip(*top_years)
        
        ax4.plot(years, year_counts, marker='o', linewidth=2, markersize=8, color='lime')
        ax4.set_xlabel('Ano', color='white')
        ax4.set_ylabel('Número de Títulos', color='white')
        ax4.set_title('Produção por Ano (2010+)', color='white')
        ax4.tick_params(colors='white')
        ax4.grid(True, alpha=0.3, color='gray')
        
        plt.tight_layout()
        plt.savefig('analise_netflix_mapreduce.png', dpi=300, bbox_inches='tight', 
                   facecolor='#000000', edgecolor='none', pad_inches=0)
        plt.close()
        print("   📈 Gráficos principais salvos em 'analise_netflix_mapreduce.png'")

def simulate_hadoop_mapreduce():
    # Simulação conceitual do Hadoop MapReduce
    print("\n🐘 SIMULAÇÃO HADOOP MAPREDUCE")
    print("=" * 50)
    
    print("📚 CONCEITOS HADOOP APLICADOS:")
    print("1. JobTracker: Coordena execução do job")
    print("2. TaskTracker: Executa tasks individuais")  
    print("3. HDFS: Sistema de arquivos distribuído")
    print("4. Mappers: Processam dados em paralelo")
    print("5. Reducers: Agregam resultados intermediários")
    
    print("\n🔄 FLUXO DE EXECUÇÃO:")
    print("Input → Split → Map → Shuffle → Sort → Reduce → Output")
    
    print("\n💾 SIMULAÇÃO HDFS:")
    print("Arquivo: /netflix/input/netflix_titles.csv")
    print("Splits: 64MB chunks distribuídos entre DataNodes")
    print("Replication: 3x para tolerância a falhas")
    
    print("\n🎯 RESULTADO:")
    print("✅ Job MapReduce executado com sucesso")
    print("✅ Dados processados distribuídamente (simulado)")
    print("✅ Tolerância a falhas implementada (conceitual)")

def main():
    # Função principal unificada
    print("🎯 ANÁLISE NETFLIX COM MAPREDUCE - VERSÃO UNIFICADA")
    print("=" * 60)
    
    # Inicializar análise MapReduce
    print(f"\n📁 Carregando dataset...")
    analyzer = NetflixMapReduce('netflix_titles.csv')
    print(f"   Dataset carregado: {len(analyzer.df)} registros")
    
    # Executar análises
    results = analyzer.run_analysis()
    
    # Inicializar engine de recomendações
    recommender = NetflixRecommendationEngine(results, analyzer.df)
    
    # Analisar tendências
    trends = recommender.analyze_trends()
    
    # Criar recomendações
    serie = recommender.create_serie_recommendation(trends)
    movie = recommender.create_movie_recommendation(trends)
    
    # Justificar escolhas
    recommender.justify_recommendations(serie, movie, trends)
    
    # Gerar visualizações
    visualizer = NetflixVisualization(results, analyzer.df)
    visualizer.generate_main_charts()
    
    # Simulação Hadoop
    simulate_hadoop_mapreduce()
    
    print(f"\n🎉 ANÁLISE COMPLETA CONCLUÍDA!")
    print(f"✅ Série 'Conexão Perdida' criada e justificada")
    print(f"✅ Filme 'Consciência Artificial' criado e justificado")
    print(f"✅ Gráficos salvos em 'analise_netflix_mapreduce.png'")
    print(f"✅ Metodologia MapReduce aplicada com sucesso")
    
    return serie, movie

if __name__ == "__main__":
    serie, movie = main()
