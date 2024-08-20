import requests
import csv
from bs4 import BeautifulSoup
from datetime import datetime
import time
from multiprocessing import Pool

# парсинг сайта в заданное количество процессов
# сбор данных с сайта, парсинг идет по страницам, внутри каждого раздела
# при данной сборке парсинг 14525-и позиций проходит за 1мин  (О_о)

domen = 'https://eldvor.ru'
filter_page = '?av=v_nalichii&av=pod_zakaz'
headers = {
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9',
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/85.0.4183.102 YaBrowser/20.9.1.110 Yowser/2.5 Safari/537.36'
            }


def get_html(url, params=None):
    r = requests.get(url, headers=headers, params=params, timeout=30)
    return r.text


def get_all_links(html):
    soup = BeautifulSoup(html, 'lxml')
    all_headers_section = soup.find_all('div', class_='index-section__header')

    all_links_category = []
    for item in all_headers_section:
        item_href = domen + item.find('a').get('href') + filter_page  # получаем ссылку
        all_links_category.append(item_href)
    return all_links_category


def get_pages_count(html):
    soup = BeautifulSoup(html, 'html.parser')
    pagination = soup.find_all('div', class_='bx-pagination-container row')
    if pagination:
        return pagination[-1].text.split('\n')[-5]
    else:
        return 1


def get_page_data(html):
    soup = BeautifulSoup(html, 'lxml')
    items = soup.find('div', class_='table_cell_top content-goods-cell').find_all('div', class_='b-goods-item')
    data = []
    for item in items:
        try:
            brend = soup.find('div', class_='b-goods-item-name').get_text(strip=True)
        except:
            brend = ''
        data.append({
                'name': item.find('div', class_='b-goods-item-link').get_text(strip=True),
                'code_goods': item.find('div', class_='info-a').get_text(strip=True).split(' ')[-1],
                'price': item.find('div', class_='b-goods-item-price clearfix').get_text(strip=True).replace('\xa0', '').split(' ')[0],
                'brend': brend,
                'link': domen + item.find('div', class_='b-goods-item-link').find('a').get('href')
            })
    return data


def write_csv(data):
    with open('eldvor_pars.csv', 'a', encoding='utf-8', newline='') as file:
        writer = csv.writer(file, delimiter=',')  # делимитер это разделитель
        #writer.writerow(['Название', 'Код', 'Цена', 'Бренд'])
        for item in data:
            writer.writerow([item['name'], item['code_goods'], item['price'], item['brend'], item['link']])



def make_all(url):
    """ Делаем все"""
    # получаем количество страниц
    html_page = get_html(url)
    pages = get_pages_count(html_page)

    for page in range(1, int(pages)+1):
        print(f'Категория: {url.split("/")[-2]}. Парсинг страницы {page} из {pages}...')
        html = get_html(url, params={'PAGEN_1': page})
        while True:
            try:
                data = get_page_data(html)
                break
            except Exception as ex:
                print(ex)
                time.sleep(30)
        write_csv(data)


def parser():
    """ Запускаем наш парсер в 10 процессов"""
    start = datetime.now()

    url = 'https://eldvor.ru/'
    # получаем все ссылки на категории товаров с
    all_category = get_all_links(get_html(url))

    with Pool(10) as p:
        p.map(make_all, all_category)

    end = datetime.now()

    total = end - start
    print(str(total))


if __name__ == '__main__':
    parser()
