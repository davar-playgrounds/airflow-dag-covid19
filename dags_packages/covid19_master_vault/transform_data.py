from dags_packages.covid19_master_vault.data_handler import DataHandler

def data_transformation():
    #Transform data section
    for source in DataHandler.data_source_list:
        data_handler = DataHandler(source)
        data_handler.transform_data()

if __name__ == '__main__':
    data_transformation()
