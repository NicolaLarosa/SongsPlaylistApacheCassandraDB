import cassandra
from cassandra.cluster import Cluster

def cassandra_connection():
    '''
    Connect to Cassandra DB and create a keyspace.
    
    Make a connection to a Cassandra instance to local machine (127.0.0.1),\
    and the connection has been established, it creates a keyspace.
    
    Parameters:
    No parameters.
    
    Returns:
    session (object): connection to cassandra
    cluster (object): instance to local machine (127.0.0.1)
    '''

    try: 
        cluster = Cluster(['127.0.0.1']) # establish connection
        session = cluster.connect() # initialize session
    except Exception as e:
        print(e)

    try:
        # Keyspace creation
        session.execute("""
        CREATE KEYSPACE IF NOT EXISTS udacity 
        WITH REPLICATION = 
        { 'class' : 'SimpleStrategy', 'replication_factor' : 1 }"""
    )

    except Exception as e:
        print(e)

    try:
        session.set_keyspace('udacity') # set the keyspace
    except Exception as e:
        print(e)

    return [session, cluster]

def shutdown_connection():
    '''
    Shutdown the connection to Cassandra.
    
    This function call cassandra_connection() function to get\
    the session and cluster parameter, then it shut them down.
    
    Parameters:
    No parameters.
    
    Returns:
    No returned parameters.
    '''
    
    l = cassandra_connection() #get session and cluster from cassandra_connection
    l[0].shutdown() # shutdown the session
    l[1].shutdown() # shutdown the cluster
    print("\nShutdown connection\n")