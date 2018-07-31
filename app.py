import logging
from flask import Flask
from flask import jsonify
from flask import request
from SqlManager import MySqlManager
import pymysql
import csv


app = Flask(__name__)
logging.basicConfig(level=logging.INFO)


@app.route('/')
def hello_world():
    return "<h1 style='color:blue'>阿帕比自动问答API终端</h1>"


@app.route('/insert', methods=['POST'])
def insert_book():
    request_json = request.json
    insert_all = request_json['insert_all']
    manager = MySqlManager.MySqlManager()

    if insert_all:
        # 清空book和label表格中的数据
        conn = manager.get_connect()
        cursor = conn.cursor()
        sql1 = 'truncate table book'
        sql2 = 'truncate table label'
        try:
            cursor.execute(sql1)
            conn.commit()
            cursor.execute(sql2)
            conn.commit()
        except pymysql.Error as e:
            return str(e)
        finally:
            cursor.close()
            conn.close()

        # 一次性录入所有数据
        with open('resources/mysql_data.csv', encoding='utf-8-sig') as csvfile:
            reader = csv.DictReader(csvfile)
            for row in reader:
                metaid = row['metaid']
                label = row['label']
                year = row['year']
                dic = {'metaid': metaid, 'label': label, 'year': year}
                manager.insert_book_info(dic)

    else:
        metaid = request_json['metaid']
        label = request_json['label']
        year = request_json['year']
        dic = {'metaid': metaid, 'label': label, 'year': year}
        manager.insert_book_info(dic)

    return "Data inserted successfully", 200


@app.route('/search', methods=['POST'])
def get_ids_labels():
    request_json = request.json
    sentence = request_json["user_input"]

    # TODO:语义分析寻找
    dic = {'label': '研发&定制化&知识产权&人才&集成电路&团队', 'year': '>2010'}  # Dummy test case

    # TODO:MySQL检索
    manager = MySqlManager.MySqlManager()
    ids = manager.query_book_id(dic)
    return jsonify(label=dic['label'], id=ids)


@app.errorhandler(500)
def server_error(e):
    logging.exception('An error occurred during a request. %s', e)
    return "An internal error occured", 500


if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0')
