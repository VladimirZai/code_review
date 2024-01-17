import sqlite3
import json
from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk


# Конфигурационные параметры, лучше выносить в начало кода или в отдельный модуль
ELASTICSEARCH_HOST = '192.168.1.252'
ELASTICSEARCH_PORT = 9200


def extract():
    """
    При документировании лучше полностью описать параметры и возвращаемое значение функции
    extract data from sql-db
    :return:
    """
    connection = sqlite3.connect("db.sqlite")
    cursor = connection.cursor()

    '''
    Выполнение вложенных запросов может сделать код менее эффективным, если можно обойти
    и без них, поэтому для получения всех полей можно использовать один запрос,
    так код будет более читаемым. Также функции GROUP_CONCAT и MAX можно использовать прямо в запросе,
    а не в дополнительном запросе.
    '''

    # Получаем все поля для индекса, кроме списка актеров и сценаристов, для них только id
    cursor.execute("""
        SELECT movies.id, imdb_rating, genre, title, plot, director,
               GROUP_CONCAT(actor_id),
               MAX(writer, writers)
        FROM movies
        LEFT JOIN movie_actors ON movies.id = movie_actors.movie_id
        GROUP BY movies.id
    """)

    raw_data = cursor.fetchall()

    # Запросы, которые не используются лучше удалять из кода, чтобы не вводить в заблуждение

    # Более понятный комментарий, например: Запись данных в словари
    actors = {row[0]: row[1] for row in cursor.execute('select * from actors where name != "N/A"')}
    writers = {row[0]: row[1] for row in cursor.execute('select * from writers where name != "N/A"')}

    return actors, writers, raw_data


# Нет необходимость использовать __ перед переменными, это может ввести в заблуждение
def transform(actors, writers, raw_data):
    """
    При документировании лучше полностью описать параметры и возвращаемое значение функции
    :param actors:
    :param writers:
    :param raw_data:
    :return:
    """
    documents_list = []
    for movie_info in raw_data:
        # Нужен более осмысленный комментарий
        movie_id, imdb_rating, genre, title, description, director, raw_actors, raw_writers = movie_info


        # Можно использовать тернарный оператор для лучшей читаемости и лакончности кода
        new_writers = ','.join([writer_row['id'] for writer_row in json.loads(raw_writers)]) if raw_writers[0] == '[' else raw_writers

        writers_list = [(writer_id, writers.get(writer_id)) for writer_id in new_writers.split(',')]
        actors_list = [(actor_id, actors.get(int(actor_id))) for actor_id in raw_actors.split(',')]

        document = {
            "_index": "movies",
            "_id": movie_id,
            "id": movie_id,
            "imdb_rating": imdb_rating,
            "genre": genre.split(', '),
            "title": title,
            "description": description,
            "director": director,
            "actors": [
                {
                    "id": actor[0],
                    "name": actor[1]
                }
                for actor in set(actors_list) if actor[1]
            ],
            "writers": [
                {
                    "id": writer[0],
                    "name": writer[1]
                }
                for writer in set(writers_list) if writer[1]
            ]
        }

        # Для улучшения читаемости и компактности кода можно использовать dict comprehension
        # document = {key: None if value == 'N/A' else value for key, value in document.items()}
        for key in document.keys():
            if document[key] == 'N/A':
                # Нужно чистить код от неиспользуемых функций типа print для красивого кода
                # print('hehe')
                document[key] = None

        document['actors_names'] = ", ".join([actor["name"] for actor in document['actors'] if actor]) or None
        document['writers_names'] = ", ".join([writer["name"] for writer in document['writers'] if writer]) or None

        # Все импорты нужно указывать в начале кода
        import pprint
        # Нет необходимости в этом выводе
        pprint.pprint(document)

        documents_list.append(document)

    return documents_list

# Лучше давать информативное название переменным, чтобы оно отражало суть
def load(documents_list):
    """
    При документировании лучше полностью описать параметры и возвращаемое значение функции
    :param acts:
    :return:
    """
    es = Elasticsearch([{'host': ELASTICSEARCH_HOST, 'port': ELASTICSEARCH_PORT}])
    bulk(es, documents_list)
    return True

if __name__ == '__main__':
    load(transform(*extract()))
