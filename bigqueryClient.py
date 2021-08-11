from google.cloud import bigquery
import os

os.environ[
    "GOOGLE_APPLICATION_CREDENTIALS"
] = "/Users/Jack/Documents/Projects/BigQueryPingPong/input/pingpong-322517-aab02d6e732c.json"

def rankings(client):
    '''
    Returns player rankings with games >= 10
    :param client: A Client object that specifies the connection to the dataset
    :return: player rankings
    '''
    
    my_query = """
               SELECT *
               FROM `pingpong-322517.PingPong.current_rank`
               WHERE games >= 10
               ORDER BY rating DESC
               """
    
    # Set up the query 
    safe_config = bigquery.QueryJobConfig(maximum_bytes_billed=10**10)      
    my_query_job = client.query(my_query, job_config=safe_config)

    # API request - run the query, and return a pandas DataFrame
    results = my_query_job.to_dataframe()

    return results

def head2head(client, player1, player2):
    """
    Returns a head-to-head of the two specified players
    :param client: A Client object that specifies the connection to the dataset
    :param player1/player2: String, names of players to compare
    :return: Head-to-head of two players
    """

    players = [player1, player2]
    players = sorted(players)
    player1, player2 = players[0], players[1]

    my_query = """
               WITH filter AS 
               (
               SELECT *
               FROM `pingpong-322517.PingPong.history`
               WHERE player1 = '{}' AND player2 = '{}'
               ORDER BY id
               )
               SELECT winner, COUNT(winner) AS wins
               FROM filter
               GROUP BY winner
               """.format(
        player1, player2
    )

    # Set up the query
    safe_config = bigquery.QueryJobConfig(maximum_bytes_billed=10 ** 10)
    my_query_job = client.query(my_query, job_config=safe_config)

    # API request - run the query, and return a pandas DataFrame
    results = my_query_job.to_dataframe()

    return results


if __name__ == "__main__":
    client = bigquery.Client()

    # rankings
    ranking = rankings(client)
    print(ranking)

    # head-to-head
    head = head2head(client, "Logan", "Jack")
    print(head)

