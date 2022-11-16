# https://www.mongodb.com/languages/python
# https://www.youtube.com/watch?v=VQnmcBnguPY&ab_channel=IndianPythonista
# https://medium.com/analytics-vidhya/connecting-to-mongodb-atlas-with-python-pymongo-5b25dab3ac53
# https://www.tutorialspoint.com/python_data_access/python_mongodb_create_database.htm
# https://www.geeksforgeeks.org/mongodb-and-python/
# https://www.youtube.com/watch?v=RNH0ODaZNE8&ab_channel=TotalTechnology
# https://pymongo.readthedocs.io/en/stable/examples/aggregation.html
# https://www.mongodb.com/docs/manual/reference/operator/aggregation/add/#mongodb-expression-exp.-add
# https://www.mongodb.com/docs/manual/reference/operator/aggregation-pipeline/
# https://www.geeksforgeeks.org/mongodb-ceil-operator/

from pymongo import MongoClient
import numpy as np

from Request import get_file_name, get_metric, request_for_metrics

if __name__ == '__main__':
    # connected mongodb cluster using the url
    client = MongoClient(f'mongodb+srv://test:test@cluster0.944ftit.mongodb.net/?retryWrites=true&w=majority')
    # create a database named "database"
    db = client['metric_database']
    # input from user is stored here
    metrics_id, benchmark_type, workload_metric, batch_unit, batch_id, batch_size, data_type = request_for_metrics()
    # file name is stored in the mongodb as file_name
    file_name = get_file_name(benchmark_type, data_type)
    print(file_name)
    # Initialize the collection by the name of the file
    collection_db = db[file_name]
    # record = collection_db.find()
    # for x in record:
    #     print(x)
    # which column to use
    requested_metric = get_metric(workload_metric)
    # Start and End of record
    first_data = batch_id * batch_unit
    print(first_data)
    last_data = first_data + batch_size * batch_unit - 1
    print(last_data)
    stage_limit_5 = {"$limit": 5}
    pipeline = [
        # Stage 1
        {
            '$match': {
                'select_row': {
                    '$gte': first_data,
                    '$lte': last_data
                }
            }
        }, {  # stage 2

            '$group': {
                '_id': {
                    '$ceil': {  # Returns the smallest integer greater than or equal to the specified number,
                        # incase if it is in decimal it will return a proper integer
                        '$divide': [  # Divides one number by another and returns the result
                            {
                                # Add numbers together
                                '$add': ['$select_row', 1, -first_data]
                            },
                            batch_unit
                        ]
                    }
                },
                'requested_metric': {  # returns an array of all values
                    '$push': f'${requested_metric}'
                }
            }

        }, {  # sort in ascending order , Stage 3
            '$sort': {
                f'{requested_metric}': 1
            }
        }, {  # Stage 4
            '$project': {
                '_id': 0,  # do not display _id
                'batch_id': '$_id',
                'minimum': {
                    '$min': f'${requested_metric}'
                },
                'maximum': {
                    '$max': f'${requested_metric}'
                },
                # 'median': {
                #
                # },
                'Standard_deviation': {
                    '$stdDevSamp': f'${requested_metric}'
                }
            }
        }, {  # stage 5
            '$sort': {'batch_id': 1}
        },

    ]

    request_metric = {"metrics_id": metrics_id, "benchmark_type": benchmark_type, "workload_metric": workload_metric,
                      "batch_unit": batch_unit, "batch_id": batch_id, "batch_size": batch_size, "data_type": data_type}
    print("Requested Item : ")
    print(request_metric)
    print("Response for Item : ")
    # Aggregate requests returns cursor from server
    cursor = collection_db.aggregate(pipeline)
    for index in cursor:
        print(index)




