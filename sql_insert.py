import pandas as pd
import psycopg2
from psycopg2.extras import execute_batch
import ast
import logging

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

def connect_to_db():
    db_params = {
        'host': 'localhost',
        'database': 'Trabalho banco de dados ',
        'user': 'postgres',
        'password': 'rmap201bl1',
        'port': '5432'
    }
    return psycopg2.connect(**db_params)

def insert_games(conn, df):
    cursor = conn.cursor()
    games_query = """
        INSERT INTO games (
            app_id, name, release_date, required_age, price, 
            discount, dlc_count, about_the_game, website, 
            header_image, support_url, support_email
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (app_id) DO NOTHING;
    """
    
    games_data = [(
        row['app_id'], 
        row['name'],
        row['release_date'],
        row['required_age'],
        row['price'],
        row['discount'],
        row['dlc_count'],
        row['about_the_game'],
        row['website'],
        row['header_image'],
        row['support_url'],
        row['support_email']
    ) for _, row in df.iterrows()]
    
    execute_batch(cursor, games_query, games_data)
    conn.commit()
    logging.info("Games data inserted successfully")

def insert_game_metrics(conn, df):
    cursor = conn.cursor()
    metrics_query = """
        INSERT INTO game_metrics (
            app_id, estimated_owners, peak_ccu, reviews,
            metacritic_score, metacritic_url, user_score,
            positive, negative, score_rank, acheivements,
            recomendations, avarege_palytime_forever,
            avarege_playtime_two_weeks, media_playtime_forever,
            median_playtime_two_weeks
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (app_id) DO NOTHING;
    """
    
    metrics_data = [(
        row['app_id'],
        row['estimated_owners'],
        row['peak_ccu'],
        row['reviews'],
        row['metacritic_score'],
        row['metacritic_url'],
        row['user_score'],
        row['positive'],
        row['negative'],
        row['score_rank'],
        row['achievements'],
        row['recommendations'],
        row['average_playtime_forever'],
        row['average_playtime_two_weeks'],
        row['median_playtime_forever'],
        row['median_playtime_two_weeks']
    ) for _, row in df.iterrows()]
    
    execute_batch(cursor, metrics_query, metrics_data)
    conn.commit()
    logging.info("Game metrics inserted successfully")

def insert_platforms(conn, df):
    cursor = conn.cursor()
    platforms_query = """
        INSERT INTO plataforms (app_id, windows, mac, linux)
        VALUES (%s, %s, %s, %s)
        ON CONFLICT (app_id) DO NOTHING;
    """
    
    platforms_data = [(
        row['app_id'],
        row['windows'],
        row['mac'],
        row['linux']
    ) for _, row in df.iterrows()]
    
    execute_batch(cursor, platforms_query, platforms_data)
    conn.commit()
    logging.info("Platforms data inserted successfully")

def insert_relation_data(conn, df, column_name, table_name, relation_table_name, id_column_name):
    cursor = conn.cursor()
    
    items_query = f"""
        INSERT INTO {table_name} ({column_name})
        VALUES (%s)
        ON CONFLICT DO NOTHING
        RETURNING id, {column_name};
    """
    
    relation_query = f"""
        INSERT INTO {relation_table_name} ({id_column_name}, app_id_game)
        VALUES (%s, %s)
        ON CONFLICT DO NOTHING;
    """
    
    def safe_parse(value):
        if pd.isna(value):
            return []
        
        value_str = str(value)
        
        # If it looks like an AST node representation
        if '<ast.Name' in value_str or '<ast.' in value_str:
            cleaned = value_str.split('at')[0].strip()
            return [cleaned]
        
        try:
            # Try standard parsing
            if ',' in value_str:
                # Split by comma and clean
                items = [item.strip() for item in value_str.split(',')]
                return [item for item in items if item]
            return [value_str.strip()]
        except Exception as e:
            logging.warning(f"Parsing failed for value '{value_str}': {e}")
            return [value_str.strip()]
    
    for _, row in df.iterrows():
        if pd.notna(row[column_name]):
            try:
                cursor.execute("BEGIN;")
                items = safe_parse(row[column_name])
                
                for item in items:
                    if item:  # Skip empty strings
                        cursor.execute(items_query, (str(item),))
                        result = cursor.fetchone()
                        
                        if result:
                            item_id, _ = result
                            cursor.execute(relation_query, (item_id, row['app_id']))
                
                conn.commit()
                
            except Exception as e:
                conn.rollback()
                logging.error(f"Error processing {column_name} for app_id {row['app_id']}: {e}")
                continue
    
    logging.info(f"{table_name} relation data processed")

def main():
    try:
        logging.info("Loading CSV data...")
        df = pd.read_csv('games_modificado_final.csv')
        
        logging.info("Connecting to database...")
        conn = connect_to_db()

        # Insert main tables data
        insert_games(conn, df)
        insert_game_metrics(conn, df)
        insert_platforms(conn, df)

        # Define relations
        relations = [
            ('supported_languages', 'supported_languages', 'supported_languages_games', 'id_supported_language'),
            ('full_audio_languages', 'full_audio_languages', 'full_audio_languages_games', 'id_full_audio_language'),
            ('developers', 'developers', 'developers_games', 'id_developers'),
            ('publishers', 'publishers', 'publishers_games', 'id_publishers'),
            ('categories', 'categories', 'categories_games', 'id_categorie'),
            ('genres', 'genres', 'genres_games', 'id_genres'),
            ('tags', 'tags', 'tags_games', 'id_tag'),
            ('screenshots', 'screenshots', 'screenshots_games', 'id_screenshot'),
            ('movies', 'movies', 'movies_games', 'id_movie')
        ]

        for column, table, relation_table, id_column in relations:
            logging.info(f"Processing {table}...")
            insert_relation_data(conn, df, column, table, relation_table, id_column)

        logging.info("All data inserted successfully!")

    except Exception as e:
        logging.error(f"An error occurred: {e}")
        if 'conn' in locals():
            conn.rollback()
    finally:
        if 'conn' in locals():
            conn.close()
            logging.info("Database connection closed")

if __name__ == "__main__":
    main()
