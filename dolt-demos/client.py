from metaflow import Flow, get_metadata
from metaflow.datatools.dolt import DoltDT
from doltpy.core import Dolt

def print_data_map(data_map):
    for run_step in data_map.keys():
        for table in data_map[run_step]:
            print('{}, {}'.format(run_step, table))
            #print(data_map[run_step][table])


print("Current metadata provider: %s" % get_metadata())
doltdb_path = './imdb-reviews'
flow = Flow('IMDBSentimentsFlow')
run = flow.latest_successful_run
print("Using run: %s" % str(run))

'''
Ex 1: Get all the inputs used by a specific run of a flow
'''
# doltdt = DoltDT(run, doltdb_path, 'master')
# data_map_for_run = doltdt.get_reads(steps=['start'])
# print_data_map(data_map_for_run)

'''
Ex 2: Get all the inputs used by a specific step of a run of a flow
'''
# doltdt = DoltDT(run, doltdb_path, 'vinai/add-rotten-data')
# data_map_for_run = doltdt.get_reads(steps=['start'])
# print_data_map(data_map_for_run)

'''
Ex 3 Outputs are handled identically
'''
doltdt = DoltDT(run, doltdb_path, 'vinai/add-rotten-data')
data_map_flow_outputs = doltdt.get_writes(steps=['stats'])
print_data_map(data_map_flow_outputs)

d = Dolt('imdb-reviews')
