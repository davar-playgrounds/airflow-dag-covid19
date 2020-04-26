from dags_packages.covid19_master_vault.data_handler import DataHandler
from dags_packages.covid19_master_vault.credentials import get_bqclient, get_bigstorageclient

def data_extraction():
    bqclient = get_bqclient()
    bqstorageclient = get_bigstorageclient()

    #Extracting raw data section
    for source in DataHandler.data_source_list:
        data_handler = DataHandler(source)
        if data_handler.source_type == 'Big Query':
            data_handler.extract_data(bqclient, bqstorageclient)
        elif data_handler.source_type == 'CSV':
            data_handler.extract_data()

if __name__ == '__main__':
    data_extraction()
