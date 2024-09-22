import os
import sys
import pandas as pd
import datetime


def aggregate_user_actions(date_to_aggregate):
    today = date_to_aggregate

    # Создаем директорию output
    if not os.path.isdir("output"):
        os.mkdir("output")

    # Указываем диапазон дней
    start_date = today - datetime.timedelta(days=7)

    # Загружаем все файлы с логами
    all_logs = []
    for day in range((today - start_date).days + 1):
        day_log = f"input/{start_date + datetime.timedelta(days=day)}.csv"
        logs = pd.read_csv(day_log)
        logs.columns = ['email', 'action', 'date']
        all_logs.append(logs)

    # Объединяем логи за каждый день в один общий
    combined_logs = pd.concat(all_logs)

    # Группируем по почте пользователя и его действиям
    aggregated_data = combined_logs.groupby(['email', 'action']).size().reset_index(name='count')

    # Формируем итоговые данные при помощи сводной таблицы
    result = aggregated_data.pivot_table(index='email', columns='action', values='count').fillna(0)
    result = result.reindex(columns=['CREATE', 'READ', 'UPDATE', 'DELETE'])
    for i in range(len(result.columns)):
        result.columns.values[i] = str(result.columns[i]).lower()+'_count'
    result = result.astype(int)

    # Сохраняем результат в директорию output
    filename = f"output/{today:%Y-%m-%d}.csv"
    result.to_csv(filename, index=True)


if __name__ == "__main__":
    today = datetime.datetime.strptime(sys.argv[1], '%Y-%m-%d').date()
    print("Считаем недельный агрегат за период:", today - datetime.timedelta(days=7), "до", today)
    aggregate_user_actions(today)