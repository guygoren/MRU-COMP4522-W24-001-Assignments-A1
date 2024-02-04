# Adv DB Winter 2024 - 1
import pandas as pd
import numbers
import os.path

import random

data_base = []  # Global binding for the Database contents
'''
transactions = [['id1',' attribute2', 'value1'], ['id2',' attribute2', 'value2'],
                ['id3', 'attribute3', 'value3']]
'''
transactions = [['1', 'Department', 'Music'], ['5', 'Civil_status', 'Divorced'],
                ['15', 'Salary', '200000']]

DB_Log = [] # <-- You WILL populate this as you go

def recovery_script(log:list, failing_transaction_index, database):  #<--- Your CODE
    '''
    Restore the database to stable and sound condition, by processing the DB log.
    '''
    print("Calling your recovery script with DB_Log as an argument.")
    print("Recovery in process ...\n")

    #Setting up variables to assist in tracking down the appropriate variables to search rows and columns with
    revertID = (failing_transaction_index - 1)
    transitionRevert = transactions[(revertID)]
    transactionID = (int((transitionRevert[0])) - 1)
    
    #Reversion of transaction
    database.loc[(transactionID), transitionRevert[1]] = log[(revertID)].loc[(0), 'Prev']
    print(database)
    pass

def transaction_processing(index, database): #<-- Your CODE
    '''
    1. Process transaction in the transaction queue.
    2. Updates DB_Log accordingly
    3. This function does NOT commit the updates, just execute them
    '''
    # save record before alteration of it

    # getting the transition currently being used via index
    transitionInPlay = transactions[index]

    # changes the tranition id to an int and finds the corrosponding row
    transactionid = int(transitionInPlay[0])
    rowID = (transactionid - 1)
    transition = database['Unique_ID'] == transactionid
    row_index = database.index[transition]

    #updates the row and adds message to DB_Log (not sure if we are doing dblog right)

    #DBMessage = database.loc[row_index, transitionInPlay[1]] + " " + transitionInPlay[2]
    changelog = pd.DataFrame([[rowID, database.loc[rowID, transitionInPlay[1]], transitionInPlay[2]]], columns=['id', 'Prev', 'Change'])
    database.loc[row_index, transitionInPlay[1]] = transitionInPlay[2]
    DB_Log.append(changelog)
    pass
    
def create_csv(data_base):
    #Reading requirements, thought to create an alternative CSV generator
    #Dunno if this is the right thing to do 
    exists = os.path.exists('CodeAndData\Employees_DB_ADV_2.csv')
    if not exists: 
        writer = data_base.to_csv('CodeAndData\Employees_DB_ADV_2.csv', index=False)
        exists = os.path.exists('CodeAndData\Employees_DB_ADV_2.csv')
        print(f"File does not exist, code to create alternative CS \n Should now exist as {exists}")
    else: 
       print("If this prints alt DB (where transactions will affect) already exists")

def read_file(file_name:str)->list:
    '''
    Read the contents of a CSV file line-by-line and return a list of lists
    '''
    data = []
    #
    # one line at-a-time reading file
    #
    with open(file_name, 'r') as reader:
    # Read and print the entire file line by line
        line = reader.readline()
        while line != '':  # The EOF char is an empty string
            line = line.strip().split(',')
            data.append(line)
             # get the next line
            line = reader.readline()

    size = len(data)
    print('The data entries BEFORE updates are presented below:')
    for item in data:
        print(item)
    print(f"\nThere are {size} records in the database, including one header.\n")
    return data

def is_there_a_failure()->bool:
    '''
    Simulates randomly a failure, returning True or False, accordingly
    '''
    value = random.randint(0,1)
    if value == 1:
        result = True
    else:
        result = False
    return result

def main():
    number_of_transactions = len(transactions)
    must_recover = False
    data_base = pd.read_csv('CodeAndData\Employees_DB_ADV.csv')
    #data_base = [list(row) for row in data_base.values]
    #print(data_base)
    failure = is_there_a_failure()
    failing_transaction_index = None
    #while not failure:
        # Process transaction
    for index in range(number_of_transactions):
            print(f"\nProcessing transaction No. {index+1}.")  
            transaction_processing(index, data_base)  #<--- Your CODE (Call function transaction_processing)
            print("UPDATES have not been committed yet...\n")
            print("database")
            print(data_base)
            failure = is_there_a_failure()
            if failure:
                must_recover = True
                failing_transaction_index = index + 1
                print(f'There was a failure whilst processing transaction No. {failing_transaction_index}.')
                break
            else:
                print(f'Transaction No. {index+1} has been commited! Changes are permanent.')
                
    if must_recover:
        #Call your recovery script
        recovery_script(DB_Log, failing_transaction_index, data_base) ### Call the recovery function to restore DB to sound state
    else:
        # All transactiones ended up well
        print("All transaction ended up well.")
        print("Updates to the database were committed!\n")

    print('\n The data entries AFTER updates -and RECOVERY, if necessary- are presented below: \n')
    for index in range (len(DB_Log)): 
        print(f"{data_base.loc[DB_Log[(index - 1)].loc[0, 'id']]} \n")
    
    #for item in data_base:
        #print(f'{item}')
    
main()


