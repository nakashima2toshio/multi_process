# 呼び出すworkerの数が少なくそれぞれの処理が長い場合は[Process]を使う。
# それぞれの処理が短い場合は[Pool]を使う。
import os
from faker import Faker
import multiprocessing as mp
import time
import logging
import pandas as pd
# import random

logging.basicConfig(
    level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


# データフレームの作成
def make_sample_csv():
    # Fakerインスタンスの初期化
    global df
    fake = Faker()

    # データフレームのサイズ
    rows = 1000
    columns = 12000

    # データフレームの作成
    if not os.path.exists('random_data.csv'):
        data = [[int(fake.numerify('###')) for _ in range(columns)] for _ in range(rows)]
        df = pd.DataFrame(data)

    # CSVファイルとして保存
    if not os.path.exists('random_data.csv'):
        df.to_csv('random_data.csv', index=False)


# サンプルデータ作成例
def faker_sample():
    logging.info('faker_sample started ===================')
    fake = Faker('ja-JP')
    ad = fake.address()
    na = fake.name()
    tx = fake.text()
    dx = fake.date_time().strftime("%Y/%m/%d %H:%M:%S")
    nu = fake.numerify()

    print(ad, na, tx, dx, nu)
    logging.info('faker_sample end ======================')


def woker1(start, end):
    begin = time.time()
    msg = 'worker-1-start:' + str(start) + ' - end:' + str(end)
    logging.info(msg)
    df1 = pd.read_csv('random_data.csv')
    for col in range(start, end - 1):
        for row in range(0, 1000 - 1):
            a = df1.iloc[row, col]
            if len(str(a)) <= 5:
                pass
            else:
                print('[row, col: data]-error-='.format(row, col, a))
    time.sleep(5)
    end = time.time()
    logging.info('worker-1 end')
    print('process_time1 = {}'.format(end - begin))


def woker2(start, end):
    begine = time.time()
    msg = 'worker-2-start:' + str(start) + ' - end:' + str(end)
    logging.info(msg)
    df2 = pd.read_csv('random_data.csv')
    for col in range(start, end - 1):
        for row in range(0, 1000 - 1):
            a = df2.iloc[row, col]
            if len(str(a)) <= 5:
                pass
            else:
                print('[row, col: data]-error-='.format(row, col, a))
    time.sleep(5)
    end = time.time()
    logging.info('worker-2 end')
    print('process_time2 = {}'.format(end - begine))


def woker3(start, end):
    begine = time.time()
    msg = 'worker-3-start:' + str(start) + ' - end:' + str(end)
    logging.info(msg)
    df3 = pd.read_csv('random_data.csv')
    for col in range(start, end - 1):
        for row in range(0, 1000 - 1):
            a = df3.iloc[row, col]
            if len(str(a)) <= 5:
                pass
            else:
                print('[row, col: data]-error-='.format(row, col, a))
    time.sleep(5)
    end = time.time()
    logging.info('worker-3 end')
    print('process_time3 = {}'.format(end - begine))


def woker4(start, end):
    begine = time.time()
    msg = 'worker-4-start:' + str(start) + ' - end:' + str(end)
    logging.info(msg)
    df4 = pd.read_csv('random_data.csv')
    for col in range(start, end - 1):
        for row in range(0, 1000 - 1):
            a = df4.iloc[row, col]
            if len(str(a)) <= 5:
                pass
            else:
                print('[row, col: data]-error-='.format(row, col, a))
    time.sleep(5)
    end = time.time()
    logging.info('worker-4 end')
    print('process_time4 = {}'.format(end - begine))


def woker5(start, end):
    begine = time.time()
    msg = 'worker-5-start:' + str(start) + ' - end:' + str(end)
    logging.info(msg)
    df5 = pd.read_csv('random_data.csv')
    for col in range(start, end - 1):
        for row in range(0, 1000 - 1):
            a = df5.iloc[row, col]
            if len(str(a)) <= 5:
                pass
            else:
                print('[row, col: data]-error-='.format(row, col, a))
    time.sleep(5)
    end = time.time()
    logging.info('worker-5 end')
    print('process_time5 = {}'.format(end - begine))


#
if __name__ == '__main__':
    make_sample_csv()

    # making processes
    t1 = mp.Process(target=woker1, args=(0, 3000,))
    t2 = mp.Process(target=woker2, args=(3001, 6000,))
    t3 = mp.Process(target=woker3, args=(6001, 9000,))
    t4 = mp.Process(target=woker3, args=(9001, 12000,))
    t5 = mp.Process(target=woker4, args=(0,12000))

    # process.start()
    t1.start()
    t2.start()
    t3.start()
    t4.start()
    print("Processes started. ----------------")

    # process.join()
    t1.join()
    t2.join()
    t3.join()
    t4.join()
    print("Processes joined. -----------------")

    t5.start()
    t5.join()
    print("ALL Processes joined. -----------------")
