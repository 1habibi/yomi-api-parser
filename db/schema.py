import aiomysql
from typing import List
from utils.logging_config import logger

CREATE_TABLES: List[str] = [
    """
    CREATE TABLE IF NOT EXISTS anime (
        id INT AUTO_INCREMENT PRIMARY KEY,
        kodik_id VARCHAR(100) NOT NULL UNIQUE,
        kodik_type VARCHAR(50),
        link TEXT,
        title TEXT,
        title_orig TEXT,
        other_title TEXT,
        year INT,
        last_season INT,
        last_episode INT,
        episodes_count INT,
        kinopoisk_id VARCHAR(50),
        imdb_id VARCHAR(50),
        shikimori_id VARCHAR(50),
        quality VARCHAR(100),
        camrip TINYINT(1),
        lgbt TINYINT(1),
        created_at DATETIME,
        updated_at DATETIME,
        description TEXT,
        anime_description TEXT,
        poster_url TEXT,
        anime_poster_url TEXT,
        premiere_world DATE,
        aired_at DATE,
        released_at DATE,
        rating_mpaa VARCHAR(20),
        minimal_age INT,
        episodes_total INT,
        episodes_aired INT,
        imdb_rating FLOAT,
        imdb_votes INT,
        shikimori_rating FLOAT,
        shikimori_votes FLOAT,
        next_episode_at DATETIME,
        all_status VARCHAR(50),
        anime_kind VARCHAR(50),
        duration INT,
        INDEX idx_anime_shikimori (shikimori_id),
        INDEX idx_anime_imdb (imdb_id),
        INDEX idx_anime_title_year (title_orig(100), year),
        INDEX idx_anime_updated (updated_at)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
    """,
    """
    CREATE TABLE IF NOT EXISTS anime_translations (
        id INT AUTO_INCREMENT PRIMARY KEY,
        anime_id INT,
        external_id INT,
        title TEXT,
        trans_type VARCHAR(50),
        FOREIGN KEY (anime_id) REFERENCES anime(id) ON DELETE CASCADE
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
    """,
    """
    CREATE TABLE IF NOT EXISTS genres (
        id INT AUTO_INCREMENT PRIMARY KEY,
        name VARCHAR(200) UNIQUE
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
    """,
    """
    CREATE TABLE IF NOT EXISTS anime_genres (
        anime_id INT,
        genre_id INT,
        PRIMARY KEY (anime_id, genre_id),
        FOREIGN KEY (anime_id) REFERENCES anime(id) ON DELETE CASCADE,
        FOREIGN KEY (genre_id) REFERENCES genres(id) ON DELETE CASCADE
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
    """,
    """
    CREATE TABLE IF NOT EXISTS anime_screenshots (
        id INT AUTO_INCREMENT PRIMARY KEY,
        anime_id INT,
        url TEXT,
        FOREIGN KEY (anime_id) REFERENCES anime(id) ON DELETE CASCADE
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
    """,
    """
    CREATE TABLE IF NOT EXISTS persons (
        id INT AUTO_INCREMENT PRIMARY KEY,
        name VARCHAR(255) UNIQUE
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
    """,
    """
    CREATE TABLE IF NOT EXISTS anime_persons (
        anime_id INT,
        person_id INT,
        role VARCHAR(50),
        PRIMARY KEY (anime_id, person_id, role),
        FOREIGN KEY (anime_id) REFERENCES anime(id) ON DELETE CASCADE,
        FOREIGN KEY (person_id) REFERENCES persons(id) ON DELETE CASCADE
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
    """,
    """
    CREATE TABLE IF NOT EXISTS studios (
        id INT AUTO_INCREMENT PRIMARY KEY,
        name VARCHAR(255) UNIQUE
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
    """,
    """
    CREATE TABLE IF NOT EXISTS anime_studios (
        anime_id INT,
        studio_id INT,
        PRIMARY KEY (anime_id, studio_id),
        FOREIGN KEY (anime_id) REFERENCES anime(id) ON DELETE CASCADE,
        FOREIGN KEY (studio_id) REFERENCES studios(id) ON DELETE CASCADE
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
    """,
    """
    CREATE TABLE IF NOT EXISTS blocked_countries (
        id INT AUTO_INCREMENT PRIMARY KEY,
        anime_id INT,
        country VARCHAR(100),
        FOREIGN KEY (anime_id) REFERENCES anime(id) ON DELETE CASCADE
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
    """,
    """
    CREATE TABLE IF NOT EXISTS blocked_seasons (
        id INT AUTO_INCREMENT PRIMARY KEY,
        anime_id INT,
        season VARCHAR(50),
        blocked_data JSON,
        FOREIGN KEY (anime_id) REFERENCES anime(id) ON DELETE CASCADE
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
    """
]


async def ensure_tables(conn: aiomysql.Connection) -> None:
    logger.info("Creating database tables...")

    async with conn.cursor() as cur:
        for sql in CREATE_TABLES:
            await cur.execute(sql)
    await conn.commit()

    logger.info("All tables created successfully")
