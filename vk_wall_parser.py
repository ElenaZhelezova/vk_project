import csv
import json
import pandas
import requests
import argparse
import datetime
import pymongo
import matplotlib.pyplot as plt


client = pymongo.MongoClient()    # create local client proxy object
database = client['vk_walls']
post_collection = database['post_collections']


def get_unixtime(usual_date):
    return int(datetime.datetime.timestamp(datetime.datetime.strptime(usual_date+' 00-00-00', '%Y-%m-%d %H-%M-%S') +
                                           datetime.timedelta(hours=-3)))


def get_wall_posts(owner_id, offset, TOKEN):
    API_URL = 'https://api.vk.com/method/wall.get?'
    params = {
        'owner_id': owner_id,
        'count': '100',
        'offset': offset,
        'access_token': TOKEN,
        'v': '5.101'
    }

    response = requests.get(API_URL, params=params).text
    json_response = json.loads(response)

    try:
        wall_posts = json_response['response']['items']
    except:
        wall_posts = []

    return wall_posts


def get_attachments_id(lst):
    ids = []
    if lst:
        for item in lst:
            item_type = item['type']
            item_id = item[item_type]['id']
            ids.append(item_id)
    return ids


def get_date_info(unix_date):
    months = ['0', 'january', 'february', 'march', 'april', 'may', 'june', 'july', 'august', 'september', 'october',
              'november', 'december']
    weekdays = ['monday', 'tuesday', 'wednesday', 'thursday', 'friday', 'saturday', 'sunday']
    usual_date = datetime.datetime.utcfromtimestamp(int(unix_date)) + datetime.timedelta(hours=3)
    year = usual_date.year
    month = months[usual_date.month]
    weekday = weekdays[usual_date.weekday()]
    hour = usual_date.hour
    return year, month, weekday, hour


def process_posts(owner_id, post_lst, min_date):
    if post_lst:
        for post in post_lst:
            if post['date'] >= min_date:
                try:
                    attachments_ids = get_attachments_id(post['attachments'])
                except KeyError:
                    attachments_ids = []
                post_info = {
                    '_id': post['id'],
                    'owner_id': owner_id,
                    'post_id': post['id'],
                    'date': post['date'],
                    'text': post['text'],
                    'attachments': attachments_ids,
                    'attachments_count': len(attachments_ids),
                    'likes': post['likes']['count'],
                    'reposts': post['reposts']['count'],
                    'comments': post['comments']['count'],
                    'year': get_date_info(post['date'])[0],
                    'month': get_date_info(post['date'])[1],
                    'weekday': get_date_info(post['date'])[2],
                    'hour': get_date_info(post['date'])[3]
                }
                try:
                    post_collection.insert_one(post_info)
                except:
                    continue


def get_content(input_id, start_date, token):
    offset = 0
    posts = get_wall_posts(input_id, offset, token)
    process_posts(input_id, posts, start_date)
    while int(posts[-1]['date']) >= start_date:
        offset += 100
        posts = get_wall_posts(input_id, offset, token)
        process_posts(input_id, posts, start_date)


def make_csv_file(owner_id, min_date, fields):
    file_name = 'posts_data_' + str(owner_id) + '_' + str(min_date) + '.csv'
    data_to_render = post_collection.find({"owner_id": owner_id})

    with open(file_name, 'w', newline='') as p_d:
        writer = csv.DictWriter(p_d, fieldnames=fields)
        writer.writeheader()

        for data in data_to_render:
            writer.writerow({field: data[field] for field in fields})

    return file_name


def get_year_stats(owner_id, min_date):
    data_posts = {}
    data_likes = {}
    data_reposts = {}
    data_comments = {}

    years = post_collection.find({"owner_id": owner_id, "date": {"$gte": min_date}}).distinct("year")

    for year in years:
        year_posts_count = post_collection.count_documents({"owner_id": owner_id, "year": year,
                                                            "date": {"$gte": min_date}})
        year_likes = post_collection.find({"owner_id": owner_id, "date": {"$gte": min_date}, "year": year},
                                          {"likes": 1, "_id": 0})
        year_reposts = post_collection.find({"owner_id": owner_id, "date": {"$gte": min_date}, "year": year},
                                            {"reposts": 1, "_id": 0})
        year_comments = post_collection.find({"owner_id": owner_id, "date": {"$gte": min_date}, "year": year},
                                             {"comments": 1, "_id": 0})

        data_posts[year] = year_posts_count

        if year_posts_count:
            data_likes[year] = round(sum([like['likes'] for like in year_likes]) / year_posts_count, 2)
            data_reposts[year] = round(sum([repost['reposts'] for repost in year_reposts]) / year_posts_count, 2)
            data_comments[year] = round(sum([comment['comments'] for comment in year_comments]) / year_posts_count, 2)
        else:
            data_likes[year] = 0
            data_reposts[year] = 0
            data_comments[year] = 0

    return {'posts': data_posts, 'likes': data_likes, 'reposts': data_reposts, 'comments': data_comments}


def get_month_stats(owner_id, min_date):
    data = {}
    years = post_collection.find({"owner_id": owner_id, "date": {"$gte": min_date}}).distinct("year")
    months = ['january', 'february', 'march', 'april', 'may', 'june', 'july', 'august', 'september', 'october',
              'november', 'december']

    for year in years:
        data_posts = {}
        data_likes = {}
        data_reposts = {}
        data_comments = {}
        for month in months:
            month_posts_count = post_collection.count_documents({"owner_id": owner_id, "year": year, "month": month,
                                                                 "date": {"$gte": min_date}})
            month_likes = post_collection.find({"owner_id": owner_id, "date": {"$gte": min_date}, "year": year,
                                                "month": month}, {"likes": 1, "_id": 0})
            month_reposts = post_collection.find({"owner_id": owner_id, "date": {"$gte": min_date}, "year": year,
                                                  "month": month}, {"year": year, "reposts": 1, "_id": 0})
            month_comments = post_collection.find({"owner_id": owner_id, "date": {"$gte": min_date}, "year": year,
                                                   "month": month}, {"year": year, "comments": 1, "_id": 0})

            data_posts[month] = month_posts_count

            if month_posts_count:
                data_likes[month] = round(sum([like['likes'] for like in month_likes]) / month_posts_count, 2)
                data_reposts[month] = round(sum([repost['reposts'] for repost in month_reposts]) / month_posts_count, 2)
                data_comments[month] = \
                    round(sum([comment['comments'] for comment in month_comments]) / month_posts_count, 2)
            else:
                data_likes[month] = 0
                data_reposts[month] = 0
                data_comments[month] = 0
        data[year] = {'posts': data_posts, 'likes': data_likes, 'reposts': data_reposts, 'comments': data_comments}

    return data


def get_weekday_stats(owner_id, min_date):
    weekdays = ['monday', 'tuesday', 'wednesday', 'thursday', 'friday', 'saturday', 'sunday']

    data_posts = {}
    data_likes = {}
    data_reposts = {}
    data_comments = {}

    for day in weekdays:
        weekday_posts_count = post_collection.count_documents({"owner_id": owner_id, "weekday": day,
                                                               "date": {"$gte": min_date}})
        weekday_likes = post_collection.find({"owner_id": owner_id, "date": {"$gte": min_date}, "weekday": day},
                                             {"likes": 1, "_id": 0})
        weekday_reposts = post_collection.find({"owner_id": owner_id, "date": {"$gte": min_date}, "weekday": day},
                                               {"reposts": 1, "_id": 0})
        weekday_comments = post_collection.find({"owner_id": owner_id, "date": {"$gte": min_date}, "weekday": day},
                                                {"comments": 1, "_id": 0})

        data_posts[day] = weekday_posts_count

        if weekday_posts_count:
            data_likes[day] = round(sum([like['likes'] for like in weekday_likes]) / weekday_posts_count, 2)
            data_reposts[day] = round(sum([repost['reposts'] for repost in weekday_reposts]) / weekday_posts_count, 2)
            data_comments[day] = \
                round(sum([comment['comments'] for comment in weekday_comments]) / weekday_posts_count, 2)
        else:
            data_likes[day] = 0
            data_reposts[day] = 0
            data_comments[day] = 0

    return {'posts': data_posts, 'likes': data_likes, 'reposts': data_reposts, 'comments': data_comments}


def get_hour_stats(owner_id, min_date):
    data_posts = {}
    data_likes = {}
    data_reposts = {}
    data_comments = {}

    for hour in range(24):
        hour_posts_count = post_collection.count_documents({"owner_id": owner_id, "hour": hour,
                                                            "date": {"$gte": min_date}})
        hour_likes = post_collection.find({"owner_id": owner_id, "date": {"$gte": min_date}, "hour": hour},
                                          {"likes": 1, "_id": 0})
        hour_reposts = post_collection.find({"owner_id": owner_id, "date": {"$gte": min_date}, "hour": hour},
                                            {"reposts": 1, "_id": 0})
        hour_comments = post_collection.find({"owner_id": owner_id, "date": {"$gte": min_date}, "hour": hour},
                                             {"comments": 1, "_id": 0})

        data_posts[hour] = hour_posts_count

        if hour_posts_count:
            data_likes[hour] = round(sum([like['likes'] for like in hour_likes]) / hour_posts_count, 2)
            data_reposts[hour] = round(sum([repost['reposts'] for repost in hour_reposts]) / hour_posts_count, 2)
            data_comments[hour] = round(sum([comment['comments'] for comment in hour_comments]) / hour_posts_count, 2)
        else:
            data_likes[hour] = 0
            data_reposts[hour] = 0
            data_comments[hour] = 0

    return {'posts': data_posts, 'likes': data_likes, 'reposts': data_reposts, 'comments': data_comments}


def get_stat_diagram(data, xlabel, file_name):
    fig, axs = plt.subplots(2, 2, figsize=(20, 15))

    axs1, axs2, axs3, axs4 = axs.flatten()

    names1 = list(data['posts'].keys())
    values1 = list(data['posts'].values())
    axs1.bar(names1, values1)
    axs1.set_xlabel(xlabel)
    axs1.yaxis.set_label_position('left')
    axs1.set_ylabel('posts')

    names2 = list(data['likes'].keys())
    values2 = list(data['likes'].values())
    axs2.bar(names2, values2)
    axs2.set_xlabel(xlabel)
    axs2.yaxis.set_label_position('left')
    axs2.set_ylabel('avg likes count')

    names3 = list(data['reposts'].keys())
    values3 = list(data['reposts'].values())
    axs3.bar(names3, values3)
    axs3.set_xlabel(xlabel)
    axs3.yaxis.set_label_position('left')
    axs3.set_ylabel('avg reposts count')

    names4 = list(data['comments'].keys())
    values4 = list(data['comments'].values())
    axs4.bar(names4, values4)
    axs4.set_xlabel(xlabel)
    axs4.yaxis.set_label_position('left')
    axs4.set_ylabel('avg comments count')

    plt.savefig(file_name)
    return file_name


def get_month_stat_files(monthes_data, owner_id, min_date):
    filenames = []
    for item in monthes_data.items():
        filename = get_stat_diagram(item[1], item[0],
                                    'months_stat_' + str(item[0]) + '_' + str(owner_id) + '_' + str(min_date)+'.png')
        filenames.append(filename)

    return filenames


def get_statistics(owner_id, start_date):
    diagramm_files = []

    year_stats = get_year_stats(owner_id, start_date)
    month_stats = get_month_stats(owner_id, start_date)
    weekday_stats = get_weekday_stats(owner_id, start_date)
    hour_stats = get_hour_stats(owner_id, start_date)
    year_stat_file = get_stat_diagram(year_stats, 'year', 'year_stat_' + str(owner_id) + '_' + str(start_date) + '.png')
    diagramm_files.append(year_stat_file)
    month_stat_files = get_month_stat_files(month_stats, owner_id, start_date)
    diagramm_files += [file for file in month_stat_files]
    weekday_stat_file = get_stat_diagram(weekday_stats, 'weekday',
                                         'weekday_stat_' + str(owner_id) + '_' + str(start_date) + '.png')
    diagramm_files.append(weekday_stat_file)
    hour_stat_file = get_stat_diagram(hour_stats, 'year', 'hour_stat_' + str(owner_id) + '_' + str(start_date) + '.png')
    diagramm_files.append(hour_stat_file)

    return diagramm_files


def render_csv(csv_file, diagramms):
    df = pandas.read_csv(csv_file)
    html_file = csv_file.replace('.csv', '.html')
    df.to_html(html_file)
    full_html = html_file.replace('.html', '_full.html')

    with open(full_html, 'w') as h_f:
        for diagramm in diagramms:
            h_f.write('<p><a href=' + diagramm + '>' + str(diagramm.strip('.png')) + '</a></p>')
            h_f.write('\n')
        h_f.write('<p><a href=' + html_file + '> posts data </a></p>')
    return html_file


def main(input_ID, start_date, report_fields):
    token = input("enter access_token")
    get_content(input_ID, start_date, token)
    csv_file = make_csv_file(input_ID, start_date, report_fields)
    graph_stat_files = get_statistics(input_ID, start_date)
    render_csv(csv_file, graph_stat_files)


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--id')
    parser.add_argument('--date', default='1970-01-01')
    parser.add_argument('--fields', default='post_id+text+attachments+attachments_count+likes+reposts+comments')

    args = parser.parse_args()

    try:
        input_ID = int(args.id)
        start_date = get_unixtime(args.date)
        report_fields = list(args.fields.split('+'))

    except ValueError:
        raise SystemExit("correct usage: script_name, --id user or group(with '-' before) id, "
                         "--date date from which statistics is being created, --fields fields for statistics report")

    main(input_ID, start_date, report_fields)
