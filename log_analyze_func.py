from pathlib import Path, PurePath 

from datetime import datetime

import json
from pprint import pprint, pformat
import pandas as pd
import numpy as np
import os
import seaborn as sns
import matplotlib.pyplot as plt

# Функции загрузки и выгрузки данных
def log2dataframe(filename):
    """Загрузить лог в dataframe"""
    logs_array = []
    with open(filename, 'r', encoding='utf-8') as logfile:
        for line in logfile.readlines():
            json_line = json.loads(line)
            line_dict = {
                't': datetime.fromisoformat(json_line["t"]),
                'Time': datetime.fromisoformat(json_line["t"]).
                                 strftime('%H:%M'),
                'pid': json_line['pid'],
                'l': json_line['l'],
                'lg': json_line['lg'],
                'message': '',
                'detail': '',
                'tn': json_line.get('tn', ''),
                'un': json_line.get('un', ''),
                'pid': json_line['pid'],
                'original_line': line
            }
            mt = json_line.get("mt", None)
            if mt is not None:
                line_dict['message'] = mt
            span = json_line.get("span", None)
            if span is not None:
                line_dict['message'] = f'span({span})'
            ex = json_line.get("ex", None)
            if ex is not None:
                line_dict['detail'] = ex
            if 'durationMs' in line_dict['message']:
                line_dict['duration'] = float(line_dict['message'].split(',')[0].split(':')[1])/1000.0
            logs_array.append(line_dict)
    df = pd.DataFrame(logs_array)
    return df

def load_logs(folder, date_str, logs_mask, folder_out, hosts, debug = False):
    """Найти и загрузить логи из folder за дату date_str"""
    logs = {'intervals': [], 'services': [], 
            'date': date_str, 'hosts': hosts,
            'folder_out': folder_out}
    for service in logs_mask.keys():
        logs['services'].append(service)
        for host in logs['hosts']:
            file_mask = logs_mask[service].format(host, date_str)
            if service not in logs.keys():
                logs[service] = {}
            logs[service][host] = {'original_file': file_mask}
            df = pd.DataFrame()
            for currentFile in Path(folder).glob(file_mask):  
                if len(df.index) == 0:
                    df = log2dataframe(currentFile)
                else:
                    df = df.append(log2dataframe(currentFile))
            logs[service][host]["data"] = df
            if debug:
                print(service, len(logs[service][host]["data"]))
    return logs

def add_interval(logs, start_interval_str, end_interval_str, debug = False):
    """Выделить временной интервал в логах"""
    interval_code = f'{start_interval_str}_{end_interval_str}'.replace(":","")
    if interval_code not in logs["intervals"]:
        logs["intervals"].append(interval_code)
    for service in logs["services"]:
        for host in logs["hosts"]:
            df_log = logs[service][host]["data"]
            if len(df_log.index) > 0:
                df_log_interval = df_log.loc[ (df_log["Time"]>=start_interval_str) 
                                             & (df_log["Time"]<=end_interval_str)]
                logs[service][host][interval_code] = {'data': df_log_interval}
            else:
                logs[service][host][interval_code] = {'data': pd.DataFrame()}
            if debug:
                print(service, interval_code, 
                      len(logs[service][host][interval_code]['data']))

def get_log_df(logs, service, host, interval_code=None, sublog_code=None):
    if interval_code is None:
        return logs[service][host]
    if sublog_code is None:
        return logs[service][host][interval_code]
    return logs[service][host][interval_code][sublog_code]

# Сохранение фреймов в логи
def save_dataframe_to_file(df, folder_out, filename):
    """Сохранить dataframe в виде лог-файла"""
    lines_out = []
    for line in df["original_line"]:
        lines_out.append(line)
    result_file = Path(folder_out, filename.replace("*", "").
                                            replace("..", ".").
                                            replace("_.", "."))
    with open(result_file, "w", encoding="utf-8") as logout_file:
        logout_file.write(''.join(lines_out))

def dataframef2log(logs, service, host, interval_code=None, sublog_code=None):
    df_dict = get_log_df(logs, service, host, interval_code, sublog_code)
    df = df_dict["data"]
    if len(df.index) > 0:
        original_file = logs[service][host]["original_file"]
        if sublog_code is None:
            filename = f'{interval_code}_{original_file}'
        else:
            filename = f'{interval_code}_{sublog_code}_{original_file}'
        save_dataframe_to_file(df, logs['folder_out'], filename)

def plot_log(logs, service, host, interval_code, sublog_code, kind = "line", show_count=True, show_duration=False):
    cnt = 17
    """Визуализировать состояние """
    sublog_dict = get_log_df(logs, service, host, interval_code, sublog_code)
    data = sublog_dict["data"]
    if len(data.index) == 0:
        return
    description = sublog_dict["description"]
    rows = (1 if show_count else 0) + (3 if show_duration else 0)
    fig = plt.figure(figsize=(16, 4*rows))
    fig.suptitle(f'{host}.{service} ({interval_code})')

    d = data.groupby(['Time']).agg({'t':['count']})
    if int(len(d.index)/cnt) > 0:
        xticks = d.index[::int(len(d.index)/cnt)]
    else:
        xticks = d.index

    if show_count:
        description = f'{sublog_dict["description"]}'
        sp1 = fig.add_subplot(rows, 1, 1)
        sp1.set_title('Количество (шт/мин)')
        df4plot = data.groupby(['Time']).\
                       agg({'t':['count']}).rename(columns={"count": sublog_code})  
        
        plt.grid(True, axis='x')
        plt.plot(df4plot, label=description)
        plt.xticks(xticks)
        plt.legend() #1
        
    if show_duration:
        description = f'{sublog_dict["description"]}'

        sp2 = fig.add_subplot(rows, 1, 2)
        sp1.set_title(f'Длительность, сек/мин')
        df4plot = data.groupby(['Time']).agg({'duration':['sum']}) \
                                        .rename(columns={"sum": f'duration_sum'})
        plt.plot(df4plot, label=f'sum: {sublog_dict["description"]}')
        plt.grid(True, axis='x')
        plt.xticks(xticks)
        plt.legend()

        sp3 = fig.add_subplot(rows, 1, 3)
        df4plot = data.groupby(['Time']).agg({'duration':['median']})
        plt.grid(True, axis='x')
        plt.plot(df4plot, label=f'median: {sublog_dict["description"]}')

        df4plot = data.groupby(['Time']).agg({'duration':['mean']})
        plt.plot(df4plot, label=f'mean: {sublog_dict["description"]}')

        df4plot = data.groupby(['Time']).agg({'duration':['max']})
        plt.plot(df4plot, label=f'max: {sublog_dict["description"]}')
        plt.xticks(xticks)

        plt.legend()

        sp4 = fig.add_subplot(rows, 1, 4)
        plt.yscale('log')
        plt.hist(data['duration'], color = 'blue', edgecolor = 'black', 
                 bins = 100, label=f'Распределение длительности: {sublog_dict["description"]}')   
        plt.legend() #2


    plt.show()
        
def prepare_logs(logs, func, sublog_code, description, kind="line", show_count=True, show_duration=False, visualization_func = plot_log):
    """Применить функцию func к логам"""
    for service in logs["services"]:
        for host in logs["hosts"]:
            for interval_code in logs["intervals"]:
                data = get_log_df(logs, service, host, interval_code)["data"]
                if len(data.index) > 0:
                    logs[service][host][interval_code][sublog_code] = {
                        'data': func(data),
                        'description': description
                    }
                    logs[service][host][interval_code][sublog_code]["data"]
                    dataframef2log(logs, service, host, interval_code, sublog_code)
                    if kind is not None:
                        visualization_func(logs, service, host, interval_code, sublog_code, kind, show_count, show_duration)
                    else:
                        print(f'{host}.{service}.{interval_code}.{sublog_code} Строк: {len(data.index)}')

def plot_count_log2(logs, service, host, interval_code, sublog_code, kind = "line", show_count=True, show_duration=False):
    """Визуализировать состояние """
    sublog_dict = get_log_df(logs, service, host, interval_code, sublog_code)
    data = sublog_dict["data"]
    if len(data.index) == 0:
        return
    df = data.groupby(['Time']).\
             agg({'t':['count']}).rename(columns={"count": sublog_code})    
    df.plot(kind=kind,
              figsize=(16, 4),
              title=f'{sublog_dict["description"]} ({host}{service}.{interval_code}.{sublog_code})')
