import random


def user_input_checker(message, correct_inputs):
    input_from_user = input(message).strip()

    while input_from_user not in correct_inputs:
        print('Please enter correct input')
        input_from_user = input(message).strip()

    return input_from_user


def request_for_metrics():
    metrics_id = int(random.randint(1, 18000))  # generate a random number from 1 to 18000 and make it string

    # input for benchmark
    benchmark_type = int(user_input_checker("1 for DVD store Or 2 for NDBench: ", ["1", "2"]))

    # input for Workload Metric
    workload_metric = int(user_input_checker("1 for CPU, 2 for Network In, 3 for Network Out Or 4 for Memory : ",
                                             ["1", "2", "3", "4"]))

    # user input for batch unit
    batch_unit = int(input("Enter Batch Unit : ").strip())

    # user input for batch id
    batch_id = int(input("Enter Batch ID : ").strip())

    # user input for batch size
    batch_size = int(input("Enter Batch Size : ").strip())

    # user input for data type
    data_type = int(user_input_checker("1 for Training Or 2 for Testing : ", ["1", "2"]))

    return metrics_id, benchmark_type, workload_metric, batch_unit, batch_id, batch_size, data_type


def get_file_name(benchmark, data):
    if benchmark == 1:
        benchmark_type = "DVD"
    else:
        benchmark_type = "NDBench"

    if data == 1:
        data_type = "testing"
    else:
        data_type = "training"

    # location of the file to be read
    return f'{benchmark_type}-{data_type}.csv'


def get_metric(workload_metric):
    workload_metrics = ['CPUUtilization_Average', 'NetworkIn_Average', 'NetworkOut_Average',
                        'MemoryUtilization_Average']
    return workload_metrics[workload_metric - 1]
