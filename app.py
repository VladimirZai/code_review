from flask import Flask, abort, request, jsonify
import elasticsearch as ES
from validate import validate_args

# Конфигурационные параметры, лучше выносить в начало кода или в отдельный модуль
ELASTICSEARCH_HOST = '192.168.11.128'
ELASTICSEARCH_PORT = 9200

HOST = '0.0.0.0'
PORT = 80

app = Flask(__name__)

@app.route('/')
def index():
    return 'worked'

@app.route('/api/movies/')
def movie_list():
    validate = validate_args(request.args)

    if not validate['success']:
        return abort(422)

    '''
    Можно использовать request.args.get(param, default) сразу для получения параметров запроса,
    с указанием четкого типа данных, чтобы в случае отсутствия параметров были устновлены
    параметры по умолчанию со строгой типизацией. Это также делает код более лаконичным
    '''
    defaults = {
        'limit': int(request.args.get('limit', 50)),
        'page': int(request.args.get('page', 1)),
        'sort': request.args.get('sort', 'id'),
        'sort_order': request.args.get('sort_order', 'asc')
    }


    # Уходит в тело запроса. Если запрос не пустой - мультисерч, если пустой - выдает все фильмы
    # По заданию, должен быть поиск по полям title, description, genre, actors_names, writers_names и director
    body = {
        "query": {
            "multi_match": {
                "query": defaults['search'],
                "fields": ["title", "description", "genre", "actors_names", "writers_names", "director"]
            }
        }
    } if defaults.get('search', False) else {}

    body['_source'] = dict()
    body['_source']['include'] = ['id', 'title', 'imdb_rating']

    params = {
        # Неиспользуемый код нужно удалять
        'from': int(defaults['limit']) * (int(defaults['page']) - 1),
        'size': defaults['limit'],
        'sort': [
            {
                defaults["sort"]: defaults["sort_order"]
            }
        ]
    }

    # Лучше использовать контекстный менеджер with, чтобы гарантировать, что соединение будет закрыто
    with ES.Elasticsearch([{'host': ELASTICSEARCH_HOST, 'port': ELASTICSEARCH_PORT}]) as es_client:
        search_res = es_client.search(
            body=body,
            index='movies',
            params=params,
            filter_path=['hits.hits._source']
        )


    return jsonify([doc['_source'] for doc in search_res['hits']['hits']])


@app.route('/api/movies/<string:movie_id>')
def get_movie(movie_id):
    '''
    Аналогично предыдущим правкам, лучше использовать контекстный менеджер для гарантии закрытия
    соединения, так как в случае возникновения исключения соединение может быть не закрыто.
    '''
    with ES.Elasticsearch([{'host': ELASTICSEARCH_HOST, 'port': ELASTICSEARCH_PORT}]) as es_client:
        if not es_client.ping():
            print('Elasticsearch is not available')
            return abort(500)

    search_result = es_client.get(index='movies', id=movie_id, ignore=404)

    '''
    search_result.get('_source', {}) предпочтительнее, так как он предотвращает возможное
    возникновение исключения KeyError, которое может произойти,
    если ключ _source отсутствует в search_result. Такой подход более безопасный.
    '''
    return jsonify(search_result.get('_source', {})) if search_result['found'] else abort(404)

if __name__ == "__main__":
    app.run(host=HOST, port=PORT)
