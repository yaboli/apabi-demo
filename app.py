import logging
from flask import Flask, jsonify, request, send_from_directory
from SqlManager import MySqlManager
import pymysql
import csv
import jieba
import re
import xlrd

app = Flask(__name__)
logging.basicConfig(level=logging.INFO)

all_labels = set()
directory = 'resources/'
filename = 'user_inputs.txt'


def get_dic(line):
    word_out = cut_words(line)
    my_dict, word_out2 = get_synonyms(word_out)

    # 设置阈值进行判断
    if len(word_out2) == 0 or ('year' not in my_dict.keys()):
        my_str = ''
        for i in word_out:
            my_str = my_str + i + '&'
            # print(i, end='&')
        my_dict['year'] = 'no_year'
        my_dict['labels'] = my_str
    elif 0 < len(word_out2) < 5:

        # 去除两个列表的重复项
        word_out_del = []
        for i in word_out:
            if i not in word_out2:
                word_out_del.append(i)

        # 按顺序合并两个列表
        word_out_final = word_out_del + word_out2
        # print(word_out_final)
        my_str = ''
        for i in word_out_final:
            my_str = my_str + i + '&'
            # print(i, end='&')
        my_dict['labels'] = my_str
        # print(my_dict)
    else:

        # 查找相同标签并排序
        count_item_list = []
        word_out3 = set(word_out2)
        for item in word_out3:
            word_out2_count = word_out2.count(item)
            if word_out2_count >= 3:
                count_item = (word_out2_count, item)
                count_item_list.append(count_item)
        word_out4 = sorted(count_item_list, reverse=True)
        # print(word_out4)

        # 取出高频标签
        word_out4_list = []
        for i, j in word_out4:
            word_out4_list.append(j)

        # 去除两个列表的重复项
        word_out_del = []
        for i in word_out:
            if i not in word_out4_list:
                word_out_del.append(i)

        # 按顺序合并两个列表
        word_out_final = word_out_del + word_out4_list
        # print(word_out_final)

        # 按格式输出
        my_str = ''
        for i in word_out_final:
            my_str = my_str + i + '&'
            # print(i, end='&')
        my_dict['labels'] = my_str
    my_dict['labels'] = my_dict['labels'].rstrip('&')

    return my_dict


def cut_words(line):
    word_seg = jieba.cut(line, cut_all=False)
    words = '|'.join(word_seg)
    word_list = words.split('|')
    word_out = []
    for word in word_list:
        if word in all_labels:
            word_out.append(word)
    return word_out  # 粗解析结果


def get_synonyms(word_out):
    replace_near_years, replace_years, replace_kw_new = replace()
    # 进一步解析 所有与问题中关键词相关的标签
    my_dict = {}
    word_out2 = []
    for x in word_out:
        if x in replace_near_years:
            if '近' in x:
                t = re.sub('\D', '', x)
                min_year = 2018 - int(t)
                min_year = '>=' + str(min_year) + '年'
                ind_x = word_out.index(x)
                word_out[ind_x] = min_year  # 将‘近x年’转变为‘>=20xx’
                word_out2.append(min_year)
                my_dict['year'] = min_year
                # print(min_year)
            elif '今年' in x:
                ind_x = word_out.index(x)
                word_out[ind_x] = '2018年'
                word_out2.append('2018年')
                my_dict['year'] = '2018年'
                # print('2018年')
        elif x in replace_years:
            min_year = x + '年'
            ind_x = word_out.index(x)
            word_out[ind_x] = min_year  # 将‘近x年’转变为‘20xx’
            word_out2.append(min_year)
            my_dict['year'] = x + '年'
            # print(x)
        else:
            for i in range(len(replace_kw_new)):
                if x in replace_kw_new[i]:
                    for kk in replace_kw_new[i]:
                        word_out2.append(kk)
    return my_dict, word_out2  # 所有与问题中关键词相关的标签


def replace():
    # 读入 元数据
    labels = load_label()
    # 读取 替换的作品
    replace_titles = labels[1]  # 出版作品
    del replace_titles[0]  # 删除表头
    # 读取 替换的出版年
    replace_years = labels[3]  # 出版年
    del replace_years[0]  # 删除表头
    # 生成‘近x年’的格式
    replace_near_years = near_years(replace_years)
    # 读取 标签
    replace_keywords = labels[4]  # 图书标签
    del replace_keywords[0]  # 删除表头
    replace_kw_new = []
    for str1 in replace_keywords:
        str_new = str1.split('&')  # 将多个标签分开
        replace_kw_new.append(str_new)
    return replace_near_years, replace_years, replace_kw_new


# 读取元数据文件 from excel
def load_label():
    labels = [[], [], [], [], []]
    dir_label = 'resources/标签图书_V2_jsl.xlsx'
    workbook = xlrd.open_workbook(dir_label)
    booksheet = workbook.sheet_by_index(0)
    for i in range(1, 5):
        labels[i] = booksheet.col_values(i)  # booktitle_label, author_label, year_label, kw_label
    return labels


# 生成‘近x年’模板
def near_years(years):
    near_year_list = []
    for year in years:
        x = str(2018 - int(year))
        if x == '0':
            y = '今年'
        else:
            y = '近' + x
        near_year_list.append(y)
    return near_year_list


def build_sql_input(dic):
    year = str(dic['year'])
    str_year = ''
    if year == 'no_year':
        str_year = ''
    elif year.endswith('年'):
        str_year = year[:len(year) - 1]
        if not str_year.startswith('>=') and not str_year.startswith('>'):
            str_year = '=' + str_year
    # labels = [v.strip() for v in str(dic['labels']).split('&') if str(v).strip() != '']
    labels = dic['labels']
    return labels, str_year


@app.before_first_request
def load_dictionary():
    dictionary_path = 'resources/book_label.txt'
    # 载入分词词典
    jieba.load_userdict(dictionary_path)
    # 保存标签以便作匹配
    with open(dictionary_path, 'r', encoding='utf-8') as f:
        labels = f.readlines()
    global all_labels
    for label in labels:
        all_labels.add(label.strip('\n'))


@app.route('/')
def hello_world():
    return "<h1 style='color:blue'>阿帕比自动问答API终端</h1>"


@app.route('/download', methods=['GET'])
def get_file():
    return send_from_directory(directory=directory, filename=filename)


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
    line = request_json["user_input"]
    path = directory + filename
    with open(path, 'a', encoding='utf-8') as f:
        f.write(line + '\n')

    # 1.提取关键词/标签词
    dic = get_dic(line)
    labels, str_year = build_sql_input(dic)
    mysql_input = {'label': labels, 'year': str_year}

    # 2.MySQL检索
    manager = MySqlManager.MySqlManager()
    ids = manager.query_book_id(mysql_input)
    return jsonify(labels=dic['labels'], ids=ids)


@app.errorhandler(500)
def server_error(e):
    logging.exception('An error occurred during a request. %s', e)
    return "An internal error occured", 500


if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0')
