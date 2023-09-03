import numpy as np
import pandas as pd
import sqlite3
from datetime import datetime, timedelta
import math


import simpy
from datetime import timedelta


MEASUREMENT_INTERVAL = 30  # mins
SPS_LOWEST_BOUND = 0.2
SPS_HIGHET_BOUND = 1
INFINITE = 10000


def read_workloads():
    """
    :return: dataframe with all the workloads
    """
    conn = sqlite3.connect('/Users/maojingyi/Downloads/scout-master/dataset/MscProject.db')
    query = r"select * from processed_workloads order by job_id ASC"
    df = pd.read_sql_query(query, conn)
    res = df.values.tolist()
    conn.close()
    return res

def get_single_workload_watt(job_id):
    """
    :param job_id: shoud be a str
    :return: a float represent the watt of the workload
    """
    conn = sqlite3.connect('/Users/maojingyi/Downloads/scout-master/dataset/MscProject.db')
    query = r"select watt from processed_workloads where job_id = ?"
    df = pd.read_sql_query(query, conn, params=(job_id,))
    res = float(df['watt'].iloc[0])
    conn.close()
    return res


def get_intervals_consumption(job_id, real_start_time):
    """
    :param job_id:
    :param real_start_time:
    :return:kwh
    """
    consumption = []
    watt = get_single_workload_watt(str(job_id))
    for t in real_start_time:
        consumption.append(t[1]/2 * watt * 1/1000)
    return consumption


def get_single_carbon_emission(job_id, real_start_time):
    """
    :param job_id: id of the job
    :param real_start_time: [(start_time_0,elapsed_time_0), (start_time_1,elapsed_time_1), ...]
    :return: value of the emission
    """
    total_emission = 0
    consumptions = get_intervals_consumption(job_id, real_start_time)
    for i in range(len(real_start_time)):

        cur_start_time = str(real_start_time[i][0]).split(' ')
        cur_time = cur_start_time[0] + 'T' + cur_start_time[1][:5] + 'Z'
        cur_ci = get_single_interval_ci(cur_time)[0][1]
        total_emission += consumptions[i] / cur_ci
    return total_emission






def change_time(elapsed_time):
    res = []
    if elapsed_time <= 1:
        res.append(elapsed_time)
    else:
        temp = elapsed_time
        while temp > 1:
            res.append(1)
            temp -= 1
        res.append(temp)
    return res


def find_nearest_hour_or_half_hour(target_time):
    flag = 0
    if (int(target_time.split(':')[1]) == 30 and int(target_time.split(':')[2] == 0)) or (int(target_time.split(':')[1]) == 0 and int(target_time.split(':')[2]) == 0):
        target_time = datetime.strptime(target_time, "%Y-%m-%d %H:%M:%S")
        return target_time, 0, 0
    elif int(target_time.split(':')[1]) > 30:
        flag = 1

    target_time = datetime.strptime(target_time, "%Y-%m-%d %H:%M:%S")
    nearest_hour = nearest_half_hour = target_time
    if flag == 0:
        nearest_hour = (target_time.replace(minute=0, second=0, microsecond=0))
        nearest_half_hour = (target_time.replace(minute=30, second=0, microsecond=0))
    if flag == 1:
        nearest_hour = (target_time.replace(minute=0, second=0, microsecond=0) + timedelta(hours=1))
        nearest_half_hour = (target_time.replace(minute=30, second=0, microsecond=0))

    # print(nearest_hour)
    # print(nearest_half_hour)

    diff_to_hour = (target_time - nearest_hour).seconds if nearest_hour < target_time else (
                nearest_hour - target_time).seconds
    # print('diff_to_hour', diff_to_hour)
    diff_to_half_hour = (target_time - nearest_half_hour).seconds if nearest_half_hour < target_time else (
                nearest_half_hour - target_time).seconds
    # print('diff_to_half_hour', diff_to_half_hour)

    earlier = 0
    if diff_to_hour < diff_to_half_hour:
        earlier = 1 if nearest_hour < target_time else 0
        return nearest_hour, earlier, diff_to_hour
    else:
        earlier = 1 if nearest_half_hour < target_time else 0
        return nearest_half_hour, earlier, diff_to_half_hour


def get_total_carbon_emission(real_job_run):
    """
    :param real_job_run: Actual operation of the programme(dict)
    {job_id: [(start_time_0,elapsed_time_0), (start_time_1,elapsed_time_1), ...]}
    :return: the total carbon emission
    """
    print('started to calculate the total carbon emission')
    res = 0
    for id in real_job_run.keys():
        cur_emission = get_single_carbon_emission(id, real_job_run[id])
        #print(id, cur_emission)
        res += cur_emission

    return res


#done!
def baseline_experiment(ad_hoc_jobs):
    """
    :param env:
    :param ad_hoc_jobs: the raw job workloads data
    :return:
    """
    run = {}
    for job in ad_hoc_jobs:
        # ['2023-07-04 05:20:53', 'c_8', 1, 1131.335, 0.6285194444444445]
        run_list = []
        job_id = str(job[2])
        nearest, earlier, diff = find_nearest_hour_or_half_hour(job[0])
        if earlier == 1:
            fisrt_time = 1800-diff if (1800-diff) < job[3] else job[3]
            run_list.append((nearest, fisrt_time/1800))
            if (1800-diff) < job[3]:
                left_time = (job[3] - (1800 - diff)) / 1800
                separete = change_time(left_time)
                cur = nearest + timedelta(hours=0.5)
                for unit in separete:
                    run_list.append((cur, unit))
                    cur = cur + timedelta(hours=0.5)
            run[job_id] = run_list
        else:
            fisrt_time = diff if diff < job[3] else job[3]
            run_list.append((nearest-timedelta(hours=0.5), fisrt_time/1800))
            if diff < job[3]:
                left_time = (job[3] - diff) / 1800
                separete = change_time(left_time)
                cur = nearest
                for unit in separete:
                    run_list.append((cur, unit))
                    cur = cur + timedelta(hours=0.5)
            run[job_id] = run_list

    print('finish scheduling for the baseline experiment')
    total_carbon_emission = get_total_carbon_emission(run)
    return total_carbon_emission

def get_intervals_sps(time, type):
    conn = sqlite3.connect('/Users/maojingyi/Downloads/scout-master/dataset/MscProject.db')
    nearest, earlier, diff = find_nearest_hour_or_half_hour(time)
    if earlier == 1:
        #query = r"select time, pred from predicted_sps where type == "+type+" and time >= "+str(nearest)+" order by time ASC LIMIT 48"
        query = r"select time, pred from predicted_sps where type == ? and time >= ? order by time ASC LIMIT 48"
        sps_df = pd.read_sql_query(query, conn, params=(type, nearest, ))
        res = sps_df.values.tolist()
        conn.close()
        return res
    else:
        nearest = nearest - timedelta(hours=0.5)
        # query = r"select time, pred from predicted_sps where type == " + type + " and time >= " + str(
        #     nearest) + " order by time ASC LIMIT 48"
        query = r"select time, pred from predicted_sps where type == ? and time >= ? order by time ASC LIMIT 48"
        sps_df = pd.read_sql_query(query, conn, params=(type, nearest, ))
        res = sps_df.values.tolist()
        conn.close()
        return res

def get_intervals_ci(time):
    conn = sqlite3.connect('/Users/maojingyi/Downloads/scout-master/dataset/MscProject.db')
    nearest, earlier, diff = find_nearest_hour_or_half_hour(time)
    if earlier == 1:
        query = r"select time, forecast, predicted_level from ci where time >= ? order by time ASC LIMIT 48"
        ci_df = pd.read_sql_query(query, conn, params=(time,))
        res = ci_df.values.tolist()
        conn.close()
        return res
    else:
        nearest = nearest - timedelta(hours=0.5)
        query = r"select time, forecast, predicted_level from ci where time >= ? order by time ASC LIMIT 48"
        ci_df = pd.read_sql_query(query, conn, params=(time,))
        res = ci_df.values.tolist()
        conn.close()
        return res

def get_single_interval_ci(time):
    """
    :param time: it should input like "2023-07-01T00:00Z"
    :return: eg:[['2023-07-01T00:00Z', 69, 0.13574660633484162]]
    """
    conn = sqlite3.connect('/Users/maojingyi/Downloads/scout-master/dataset/MscProject.db')
    query = 'SELECT time, forecast, predicted_level FROM ci WHERE time = ?'
    #query = r"select time, forecast, predicted_level from ci where time = " + time
    #ci_df = pd.read_sql_query(query, conn)
    df = pd.read_sql_query(query, conn, params=(time,))
    res = df.values.tolist()
    conn.close()
    return res


def calculate_score(s, c, baseline, alpha=0.5, beta=0.5):
    if baseline == True:
        return s
    else:
        #print(beta*c - alpha*s)
        return beta*c - alpha*s


def interruptible_experiment(ad_hoc_jobs, baseline, alpha=0.5, beta=0.5):
    run = {}
    for job in ad_hoc_jobs:
        run_list = []
        #['2023-07-04 05:20:53', 'c_8', 1, 1131.335, 0.6285194444444445]
        sps_total = get_intervals_sps(job[0], job[1])
        ci_total = get_intervals_ci(job[0])
        score = {}
        for i in range(47):
            try:
                cur_sps = sps_total[i]
                cur_ci = ci_total[i]
            except:
                print(i)
            if cur_sps[1] < SPS_LOWEST_BOUND:
                score[cur_sps[0]] = INFINITE
            else:
                s = cur_sps[1]
                c = cur_ci[2]
                score[cur_sps[0]] = calculate_score(s, c, baseline, alpha, beta)
        sorted_score = sorted(score.items(), key=lambda x: x[1])
        #print(sorted_score)
        sep = change_time(job[4])
        count = len(sep)
        target_intervals = sorted_score[:count+2]
        interval_list = []
        for interval in target_intervals:
            interval_list.append(datetime.strptime(interval[0], "%Y-%m-%d %H:%M:%S"))
        interval_list = sorted(interval_list)
        i = 0
        while i < count:
            if i == 0 and interval_list[i] == sps_total[0][0]:
                nearest, earlier, diff = find_nearest_hour_or_half_hour(job[0])
                add_time = 1800-diff if earlier == 1 else diff
                add_time = add_time/1800 if add_time/1800 > sep[i] else sep[i]
                run_list.append((interval_list[i], add_time))
                temp = sep[-1] + 1 - add_time
                if temp > 1:
                    sep[-1] = 1
                    sep.append(temp-1)
                else:
                    sep[-1] = temp
                count = len(sep)
            else:
                run_list.append((interval_list[i], sep[i]))
            i += 1
            run[str(job[2])] = run_list
    print(run)
    total_carbon_emission = get_total_carbon_emission(run)
    return total_carbon_emission


def interruptible_experiment_baseline(ad_hoc_jobs, baseline, alpha=0.5, beta=0.5):
    run = {}
    for job in ad_hoc_jobs:
        run_list = []
        #['2023-07-04 05:20:53', 'c_8', 1, 1131.335, 0.6285194444444445]
        sps_total = get_intervals_sps(job[0], job[1])
        ci_total = get_intervals_ci(job[0])
        score = {}
        for i in range(48):
            cur_sps = sps_total[i]
            cur_ci = ci_total[i]
            if cur_sps[1] < SPS_LOWEST_BOUND:
                score[cur_sps[0]] = INFINITE
            else:
                s = cur_sps[1]
                c = cur_ci[2]
                score[cur_sps[0]] = calculate_score(s, c, baseline, alpha, beta)
        #sorted_score = sorted(score.items(), key=lambda x: x[1])
        sorted_score = list(score.items())
        #print(sorted_score)
        sep = change_time(job[4])
        count = len(sep)
        target_intervals = sorted_score[:count+2]
        interval_list = []
        for interval in target_intervals:
            interval_list.append(datetime.strptime(interval[0], "%Y-%m-%d %H:%M:%S"))
        interval_list = sorted(interval_list)
        i = 0
        while i < count:
            if i == 0 and interval_list[i] == sps_total[0][0]:
                nearest, earlier, diff = find_nearest_hour_or_half_hour(job[0])
                add_time = 1800-diff if earlier == 1 else diff
                add_time = add_time/1800 if add_time/1800 > sep[i] else sep[i]
                run_list.append((interval_list[i], add_time))
                temp = sep[-1] + 1 - add_time
                if temp > 1:
                    sep[-1] = 1
                    sep.append(temp-1)
                else:
                    sep[-1] = temp
                count = len(sep)
            else:
                run_list.append((interval_list[i], sep[i]))
            i += 1
            run[str(job[2])] = run_list
    print(run)
    total_carbon_emission = get_total_carbon_emission(run)
    return total_carbon_emission




def uninterruptible_experiment(ad_hoc_jobs, baseline, alpha=0.5, beta=0.5):
    run = {}
    for job in ad_hoc_jobs:
        # ['2023-07-04 05:20:53', 'c_8', 1, 1131.335, 0.6285194444444445]
        sps_total = get_intervals_sps(job[0], job[1])
        ci_total = get_intervals_ci(job[0])
        # first search for every possible start time
        if job[4] <= 1:
            nearest, earlier, diff = find_nearest_hour_or_half_hour(job[0])
            score = {}
            pre_time = nearest if earlier == 1 else nearest - timedelta(hours=0.5)
            for i in range(48):
                cur_sps = sps_total[i]
                cur_ci = ci_total[i]
                if cur_sps[1] < SPS_LOWEST_BOUND:
                    score[cur_sps[0]] = INFINITE
                else:
                    s = cur_sps[1]
                    c = cur_ci[2]
                    score[cur_sps[0]] = calculate_score(s, c, baseline, alpha, beta)
            sorted_score = sorted(score.items(), key=lambda x: x[1])
            #print(sorted_score)
            # first_score = next(iter(sorted_score.items()))
            # second_score = next(first_score)
            first_score = sorted_score[0]
            second_score = sorted_score[1]
            if first_score[0] == pre_time:
                cur_diff = diff if earlier == 0 else 1800-diff
                if cur_diff >= job[3]:
                    run[job[2]] = [(pre_time, job[4])]
                else:
                    if pre_time + timedelta(hours=0.5) not in score.keys():
                        run[job[2]] = [(second_score[0], job[4])]
                    else:
                        firstwo = (sps_total[0][1] + sps_total[1][1]) / 2
                        if firstwo < second_score[1]:
                            run[job[2]] = [(first_score[0], cur_diff/1800), (sps_total[1][0], (job[3]-cur_diff)/1800)]
                        else:
                            run[job[2]] = [(second_score[0], job[4])]
            else:
                run[job[2]] = [(first_score[0], job[4])]

        else:
            run_list = []
            number = int(math.ceil(job[4]))
            score_list = []
            for i in range(48):
                cur_score = calculate_score(sps_total[i][1], ci_total[i][2], baseline, alpha, beta) if sps_total[i][1] > SPS_LOWEST_BOUND else INFINITE
                score_list.append(cur_score)
            score_series = pd.Series(score_list)
            means = score_series.rolling(number).mean()[number - 1:]
            start_from = np.argmin(means)
            times = change_time(job[4])
            for j in range(number):
                run_list.append((sps_total[start_from+j][0], times[j]))
            run[job[2]] = run_list
    total_carbon_emission = get_total_carbon_emission(run)
    return total_carbon_emission




if __name__ == '__main__':


    ad_hoc_jobs = read_workloads()

    #baseline = baseline_experiment(ad_hoc_jobs)

    # evaluation_1 = (experiment_2 - experiment_1) / experiment_1
    experiment_1 = interruptible_experiment(ad_hoc_jobs, True)
    experiment_2 = uninterruptible_experiment(ad_hoc_jobs, True)
    experiment_3 = interruptible_experiment(ad_hoc_jobs, False, 0.8, 0.2)
    experiment_4 = uninterruptible_experiment(ad_hoc_jobs, False, 0.8, 0.2)
    experiment_5 = interruptible_experiment(ad_hoc_jobs, False, 0.7, 0.3)
    experiment_6 = uninterruptible_experiment(ad_hoc_jobs, False, 0.7, 0.3)
    experiment_7 = interruptible_experiment(ad_hoc_jobs, False, 0.6, 0.4)
    experiment_8 = uninterruptible_experiment(ad_hoc_jobs, False, 0.6, 0.4)

    experiment_9 = interruptible_experiment(ad_hoc_jobs, False, 0.5, 0.5)
    experiment_10 = uninterruptible_experiment(ad_hoc_jobs, False, 0.5, 0.5)
    experiment_11 = interruptible_experiment(ad_hoc_jobs, False, 0.4, 0.6)
    experiment_12 = uninterruptible_experiment(ad_hoc_jobs, False, 0.4, 0.6)
    experiment_13 = interruptible_experiment(ad_hoc_jobs, False, 0.3, 0.7)
    experiment_14 = uninterruptible_experiment(ad_hoc_jobs, False, 0.3, 0.7)
    experiment_15 = interruptible_experiment(ad_hoc_jobs, False, 0.2, 0.8)
    experiment_16 = uninterruptible_experiment(ad_hoc_jobs, False, 0.2, 0.8)
    experiment_17 = interruptible_experiment(ad_hoc_jobs, False, 0, 1)
    experiment_18 = uninterruptible_experiment(ad_hoc_jobs, False, 0, 1)

    print('experiment_1: ', experiment_1)
    print('experiment_2: ', experiment_2)
    print('experiment_3: ', experiment_3, (experiment_1-experiment_3)/experiment_1)
    print('experiment_4: ', experiment_4, (experiment_2-experiment_4)/experiment_2)
    print('experiment_5: ', experiment_5, (experiment_1-experiment_5)/experiment_1)
    print('experiment_6: ', experiment_6, (experiment_2-experiment_6)/experiment_2)
    print('experiment_7: ', experiment_7, (experiment_1-experiment_7)/experiment_1)
    print('experiment_8: ', experiment_8, (experiment_2-experiment_8)/experiment_2)
    print('experiment_9: ', experiment_9, (experiment_1-experiment_9)/experiment_1)
    print('experiment_10: ', experiment_10, (experiment_2-experiment_10)/experiment_2)
    print('experiment_11: ', experiment_11, (experiment_1-experiment_11)/experiment_1)
    print('experiment_12: ', experiment_12, (experiment_2-experiment_12)/experiment_2)
    print('experiment_13: ', experiment_13, (experiment_1-experiment_13)/experiment_1)
    print('experiment_14: ', experiment_14, (experiment_2-experiment_14)/experiment_2)
    print('experiment_15: ', experiment_15, (experiment_1-experiment_15)/experiment_1)
    print('experiment_16: ', experiment_16, (experiment_2-experiment_16)/experiment_2)
    print('experiment_17: ', experiment_17, (experiment_1-experiment_17)/experiment_1)
    print('experiment_18: ', experiment_18, (experiment_2-experiment_18)/experiment_2)






























