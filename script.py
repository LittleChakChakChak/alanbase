import requests
import pandas as pd
import datetime
import smtplib

FILE_WAY = './payout_webdev.csv'
LOGS = './logs.txt'

# адрес api
url = "https://glorypartners.api.alanbase.com/v1/admin/statistic/conversions?"
headers = {'API-KEY' : 'secret',
           'Content-Type': 'application/json',
           'Accept': 'application/json'}
params = {'timezone': 'Europe/Moscow',
          'date_from': '2022-01-01',
          'date_to': '2022-12-31',
          'goal_keys': '["ftd"]',
          'currency_code': 'USD',
          'page': 1,
          'per_page': 1000}

data = []
corrected_data = []
page = 1


def mail(page = 0, last_page = 0, flag = False):
    # # От кого:
    fromaddr = 'адрес почты тот кто отправил (в письме)'

    # Кому:
    toaddr = 'адрес почты кому придет'

    #Тема письма:
    subj = f'Enkod - {flag} - Pages {page} from {last_page} in {str(datetime.datetime.now())}'

    #Текст сообщения:
    msg_txt = f'{flag} - loading enkod \n\n Pages worked out {page} from {last_page} in {str(datetime.datetime.now())}'

    #Создаем письмо (заголовки и текст)
    msg = "From: %s\nTo: %s\nSubject: %s\n\n%s" % (fromaddr, toaddr, subj, msg_txt)

    #Логин gmail аккаунта. Пишем только имя ящика
    username = 'адрес почты тот кто отправил'

    #Соответственно, пароль от ящика:
    password = ''

    #Инициализируем соединение с сервером gmail по протоколу smtp.
    server = smtplib.SMTP('smtp.gmail.com:587')

    #Выводим на консоль лог работы с сервером (для отладки)
    server.set_debuglevel(1)

    #Переводим соединение в защищенный режим (Transport Layer Security)
    server.starttls()

    # Проводим авторизацию:
    server.login(username, password)

    # Отправляем письмо:
    server.sendmail(fromaddr, toaddr, msg)

    # Закрываем соединение с сервером
    server.quit()


try:
    # отправляем общий запрос
    response = requests.get(url, headers=headers, params=params)
    # получаем данные
    list_data = response.json()

    # создание колонок из ключей
    columns_all = list(list_data['data'][0].keys())
    hard_colums = columns_all[39:]  # колонки содержащие словари
    columns = columns_all[:39]

    # добавление сложных колонок
    columns_all = columns + ['goal_name', 'goal_tag',
                             'advertiser_id', 'advertiser_email',
                             'product_id', 'product_name',
                             'offer_id', 'offer_name', 'offer_tags',
                             'partner_id', 'partner_email',
                             'partner_manager_id',  'partner_manager_email']

    last_page = list_data['meta']['last_page']

    while page <= last_page:
        params['page'] = page

        response = requests.get(url, headers=headers, params=params)
        list_data = response.json()

        # построчная запись данных
        for record in list_data['data']:
            #одна запись = одна запись в списке
            # запись простых колонок
            corrected_data = [record[key] for key in columns]

            # запись сложных колонок
            manager = record['partner'].get('manager', '')
            goal = record.get('goal', '')
            advertiser = record.get('advertiser', '')
            product = record.get('product', '')
            offer = record.get('offer', '')
            partner = record.get('partner', '')

            if manager != None:
                corrected_data += [manager['id'], manager['email']]
            else:
                corrected_data += ['', '']

            if goal != None:
                corrected_data += [goal['name'], goal['key']]
            else:
                corrected_data += ['', '']

            if advertiser != None:
                corrected_data += [advertiser['id'], advertiser['email']]
            else:
                corrected_data += ['', '']

            if product != None:
                corrected_data += [product['id'], product['name']]
            else:
                corrected_data += ['', '']

            if offer != None:
                corrected_data += [offer['id'], offer['name'], offer['tags']]
            else:
                corrected_data += ['', '']

            if partner != None:
                corrected_data += [partner['id'], partner['email']]
            else:
                corrected_data += ['', '']

            # собираем данные в один массив
            data.append(corrected_data)

        print(f"Страниц отработано {page} из {last_page} в {str(datetime.datetime.now())}")

        page += 1

    df = pd.DataFrame(data, columns=columns_all)

    # запись в csv файл
    df.to_csv(FILE_WAY, index=False)

    with open(LOGS, 'a+') as file:
        file.writelines(f'Файл заполнен {page-1} из {last_page} в {str(datetime.datetime.now())}\n')

    mail(page-1, last_page, True)
except:
    mail(page-1, last_page, False)

