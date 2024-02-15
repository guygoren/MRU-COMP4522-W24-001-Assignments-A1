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

def recovery_script(log:list, failing_transaction_index, database): 
    '''
    Restore the database to stable and sound condition, by processing the DB log.
    '''
    print("Calling your recovery script with DB_Log as an argument.")
    print("Recovery in process ...\n")

    #Setting up variables to assist in tracking down the appropriate variables to search rows and columns with
    revertID = (failing_transaction_index - 1)
    transitionRevert = transactions[(revertID)]
    transactionID = (int((transitionRevert[0])) - 1)
<<<<<<< Updated upstream
    
    #Reversion of transaction
    database.loc[(transactionID), transitionRevert[1]] = log[(revertID)].loc[(0), 'Prev']
    print(database)
    pass

def transaction_processing(index, database):
=======

    
    #Reversion of transaction
    database.loc[(transactionID), transitionRevert[1]] = log[revertID]["Prev"]
   
    print(database)
    pass

def transaction_processing(index, database, failure, failing_transaction_index, isexecuted):  # Pass failure variable to the transaction_processing function
>>>>>>> Stashed changes
    '''
    1. Process transaction in the transaction queue.
    2. Updates DB_Log accordingly
    3. This function does NOT commit the updates, just execute them
    '''
    # save record before alteration of it

<<<<<<< Updated upstream
    # getting the transition currently being used via index
    transitionInPlay = transactions[index]

    # changes the tranition id to an int and finds the corrosponding row
=======
        # getting the transition currently being used via index
    transitionInPlay = transactions[index]

    # changes the transition id to an int and finds the corresponding row
>>>>>>> Stashed changes
    transactionid = int(transitionInPlay[0])
    rowID = (transactionid - 1)
    transition = database['Unique_ID'] == transactionid
    row_index = database.index[transition]
<<<<<<< Updated upstream

    #updates the row and adds message to DB_Log (not sure if we are doing dblog right)

    #DBMessage = database.loc[row_index, transitionInPlay[1]] + " " + transitionInPlay[2]
    changelog = pd.DataFrame([[rowID, database.loc[rowID, transitionInPlay[1]], transitionInPlay[2]]], columns=['id', 'Prev', 'Change'])
    database.loc[row_index, transitionInPlay[1]] = transitionInPlay[2]
    DB_Log.append(changelog)
    pass
    
=======
    #we use this to set non executed transactions to have the non executed status
    if (isexecuted) :
        # updates the row and adds message to DB_Log
        status = 'committed'  # Default status
        # Check if the transaction should be rolled-back or not-executed
        if failure and (failing_transaction_index is not None) and (index + 1) > failing_transaction_index:
            status = 'rolled-back'
    else:
        status = 'not-executed'

    # Create the changelog dictionary with the appropriate status
    changelog = {'id': rowID, 'Prev': database.loc[rowID, transitionInPlay[1]], 
                 'Change': transitionInPlay[2], 'Status': status}
    
    # Locating the row that needs to be changed and making the change via information in transactions array
    database.loc[row_index, transitionInPlay[1]] = transitionInPlay[2]
    DB_Log.append(changelog)
    
>>>>>>> Stashed changes
def create_csv(data_base):
    #Reading requirements, thought to create an alternative CSV generator
    #Not sure if this is the right thing to do 
    exists = os.path.exists('CodeAndData\Employees_DB_ADV_2.csv')
    if not exists: 
        writer = data_base.to_csv('CodeAndData\Employees_DB_ADV_2.csv', index=False)
        exists = os.path.exists('CodeAndData\Employees_DB_ADV_2.csv')
        print(f"File does not exist, code to create alternative CS \n Should now exist as {exists}")
    else: 
       print("If this prints alt DB (where transactions will affect) already exists")

<<<<<<< Updated upstream
def read_file(file_name:str)->list:
    '''
    Read the contents of a CSV file line-by-line and return a list of lists
    '''
    data = []

    # one line at-a-time reading file
    
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
=======
>>>>>>> Stashed changes

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

<<<<<<< Updated upstream
=======

>>>>>>> Stashed changes
def main():
    #get number of total transactions, which is always 3
    number_of_transactions = len(transactions)
    must_recover = False
    data_base = pd.read_csv('CodeAndData\Employees_DB_ADV.csv')
<<<<<<< Updated upstream
    #data_base = [list(row) for row in data_base.values]
    #print(data_base)
    failure = is_there_a_failure()
    failing_transaction_index = None
    #while not failure:
        # Process transaction
    for index in range(number_of_transactions):
            print(f"\nProcessing transaction No. {index+1}.")  
            transaction_processing(index, data_base)  #Call function transaction_processing
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
        # Call your recovery script
        recovery_script(DB_Log, failing_transaction_index, data_base) # Call the recovery function to restore DB to sound state
=======
    failing_transaction_index = 0

    
    for index in range(number_of_transactions):
        # call the failure function to check if failure
        failure = is_there_a_failure()
        print(f"\nProcessing transaction No. {index+1}.")  
        #we call transaction_processing, and always set the last as true as these transactions are being executed
        transaction_processing(index, data_base, failure, failing_transaction_index, True)  # Pass failure variable to the transaction_processing function
        print("UPDATES have not been committed yet...\n")
        print("database")
        print(data_base)
        
        if failure:
            must_recover = True
            failing_transaction_index = index + 1
            print(f'There was a failure whilst processing transaction No. {failing_transaction_index}.')
            break
        else:
            print(f'Transaction No. {index+1} has been commited! Changes are permanent.')
            
                
    if must_recover:
        recovery_script(DB_Log, failing_transaction_index, data_base)
        
>>>>>>> Stashed changes
    else:
        # All transactiones ended up well
        print("All transaction ended up well.")
        print("Updates to the database were committed!\n")

<<<<<<< Updated upstream
    print('\n The data entries AFTER updates -and RECOVERY, if necessary- are presented below: \n')
    for index in range (len(DB_Log)): 
        print(f"{data_base.loc[DB_Log[(index - 1)].loc[0, 'id']]} \n")
    
    #for item in data_base:
        #print(f'{item}')
    
=======
    print('\n The data entries AFTER updates and potential recovery are presented below: \n')
    create_csv(data_base)
    #If transactions do not get completed, we call the processing function and set the last parameter as false to indicate its non exectured and add to DB_Log
    transactionindex = index + 1
    while (transactionindex > 0 and transactionindex < 3):
         transaction_processing(transactionindex, data_base, False, None, False)
         transactionindex = (transactionindex + 1)
    # if (transactionindex == 1):
    #     print("The following transactions were not executed")
    #     transaction_processing(1, data_base, False, None, False)
    #     transaction_processing(2, data_base, False, None, False)
    # elif (transactionindex == 2):
    #     print("The following transaction was not executed")
    #     transaction_processing(2, data_base, False, None, False)
    for transaction in DB_Log:
        print(f"Transaction ID: {transaction['id']}, Attribute: {transaction['Prev']} -> {transaction['Change']}, Status: {transaction['Status']}")
    

>>>>>>> Stashed changes
main()

