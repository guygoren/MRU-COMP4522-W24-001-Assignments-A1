import pandas as pd
import random

data_base = []  # Placeholder for the Database contents

# Sample transactions data
transactions = [['1', 'Department', 'Music'], ['5', 'Civil_status', 'Divorced'], ['15', 'Salary', '200000']]

journal = []  # Placeholder for transaction journal

def recovery_script(journal:list, failing_transaction_index, database): 
    '''
    Restore the database to a stable condition by processing the journal.
    '''
    revertID = (failing_transaction_index - 1)
    transitionRevert = transactions[revertID]
    transactionID = int(transitionRevert[0]) - 1
    
    # Revert the transaction
    database.loc[transactionID, transitionRevert[1]] = journal[revertID]['Prev']

def transaction_processing(index, database):
    '''
    Process transaction in the transaction queue and journal the changes.
    '''
    transitionInPlay = transactions[index]
    transactionid = int(transitionInPlay[0])
    rowID = transactionid - 1
    transition = database['Unique_ID'] == transactionid
    row_index = database.index[transition]

    # Update row and add message to journal
    changelog = {'id': rowID, 'Prev': database.loc[rowID, transitionInPlay[1]], 'Change': transitionInPlay[2]}
    database.loc[row_index, transitionInPlay[1]] = transitionInPlay[2]
    journal.append(changelog)

def is_there_a_failure() -> bool:
    '''
    Simulate randomly a failure, returning True or False.
    '''
    value = random.randint(0,1)
    return value == 1

def main():
    number_of_transactions = len(transactions)
    must_recover = False
    data_base = pd.read_csv('CodeAndData\Employees_DB_ADV.csv')
    failure = is_there_a_failure()
    failing_transaction_index = None
    
    for index in range(number_of_transactions):
        print(f"\nProcessing transaction No. {index+1}.")  
        transaction_processing(index, data_base)
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
        recovery_script(journal, failing_transaction_index, data_base)
    else:
        print("All transactions ended up well.")
        print("Updates to the database were committed!\n")

    print('\n The data entries AFTER updates and potential recovery are presented below: \n')
    for index in range(len(journal)): 
        print(f"{data_base.loc[journal[index]['id']]} \n")
    
main()
