import psycopg2

from config import DATABASE_URL


def get_connection():
    """
    Создаёт и возвращает соединение с базой данных.
    """
    conn = psycopg2.connect(DATABASE_URL)
    return conn


def init_db():
    """
    Инициализация базы данных, создание таблиц, если они отсутствуют.
    """
    commands = (
        """
        CREATE TABLE students (
            student_id SERIAL PRIMARY KEY,
            name VARCHAR(255) NOT NULL,
            role VARCHAR(50) NOT NULL DEFAULT 'student'  -- возможные значения: 'student', 'leader'
        );
        """,
        """
        CREATE TABLE IF NOT EXISTS messages (
            id SERIAL PRIMARY KEY,
            user_id INTEGER NOT NULL,
            text TEXT NOT NULL,
            created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (user_id)
                REFERENCES users (id)
                ON UPDATE CASCADE ON DELETE CASCADE
        )
        """
    )

    conn = None
    try:
        conn = get_connection()
        cursor = conn.cursor()
        # Выполняем все команды по созданию таблиц
        for command in commands:
            cursor.execute(command)
        cursor.close()
        conn.commit()
    except (Exception, psycopg2.DatabaseError) as error:
        print(f"Ошибка при работе с базой данных: {error}")
    finally:
        if conn is not None:
            conn.close()


def execute_query(query, params=None):
    """
    Выполнение произвольного запроса к базе данных с параметрами
    """
    conn = None
    results = None
    try:
        conn = get_connection()
        cursor = conn.cursor()
        cursor.execute(query, params)
        if cursor.description:
            results = cursor.fetchall()
        cursor.close()
        conn.commit()
        return results
    except (Exception, psycopg2.DatabaseError) as error:
        print(f"Ошибка при выполнении запроса: {error}")
    finally:
        if conn is not None:
            conn.close()
