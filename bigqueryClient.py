from google.cloud import bigquery
import os

from rankingSystem import recordMatch

os.environ[
    "GOOGLE_APPLICATION_CREDENTIALS"
] = "/Users/Jack/Documents/Projects/BigQueryPingPong/input/pingpong-322517-aab02d6e732c.json"


def rankings(client, display=False):
    """
    Returns player rankings with games >= 10
    :param client: A Client object that specifies the connection to the dataset
    :return: player rankings
    """
    if display:
        my_query = """
                SELECT *
                FROM `pingpong-322517.PingPong.current_rank`
                WHERE games >= 10
                ORDER BY rating DESC
                """
    else:
        my_query = """
                SELECT *
                FROM `pingpong-322517.PingPong.current_rank`
                ORDER BY rating DESC
                """
    # Set up the query
    safe_config = bigquery.QueryJobConfig(maximum_bytes_billed=10 ** 10)
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


def history(client):
    """
    Returns player ranking history
    :param client: A Client object that specifies the connection to the dataset
    :return: player ranking history
    """

    my_query = """
               SELECT *
               FROM `pingpong-322517.PingPong.ranking_history`
               ORDER BY id DESC
               """

    # Set up the query
    safe_config = bigquery.QueryJobConfig(maximum_bytes_billed=10 ** 10)
    my_query_job = client.query(my_query, job_config=safe_config)

    # API request - run the query, and return a pandas DataFrame
    results = my_query_job.to_dataframe()

    return results


def update_rankings(client, person1, person2, winner):

    df = rankings(client)
    new_ranking_1, new_ranking_2 = recordMatch(df, person1, person2, winner)

    my_query1 = """
                UPDATE `pingpong-322517.PingPong.current_rank`
                SET rating = {}, games = games + 1
                WHERE name = '{}'
                """.format(
        new_ranking_1, person1
    )
    my_query2 = """
                UPDATE `pingpong-322517.PingPong.current_rank`
                SET rating = {}, games = games + 1
                WHERE name = '{}'
                """.format(
        new_ranking_2, person2
    )

    safe_config = bigquery.QueryJobConfig(maximum_bytes_billed=10 ** 10)

    my_query_job1 = client.query(my_query1, job_config=safe_config)
    my_query_job1.result()
    my_query_job2 = client.query(my_query2, job_config=safe_config)
    my_query_job2.result()

    print(f"Rankings updated for game between {person1} and {person2}")


def update_history(client, ranks, hist):

    names = ranks.name.tolist()
    names.append("id")
    columns_query = ", ".join(names)

    rank_vals = ranks.rating.tolist()
    rank_vals.append(hist.id.max() + 1)
    string_rank = [str(num) for num in rank_vals]
    rank_query = ", ".join(string_rank)

    my_query = f"""
                INSERT INTO `pingpong-322517.PingPong.ranking_history` ({columns_query})
                VALUES ({rank_query})
                """
    hist_update = client.query(my_query)
    hist_update.result()

    print(f"Rankings updated for row index {rank_vals[-1]}")


if __name__ == "__main__":
    client = bigquery.Client()

    # update rank
    # update_rankings(client, "Henry", "Luis", "Henry")
    # print(rankings(client, display=False))

    # update history
    hist = history(client)
    ranks = rankings(client)
    update_history(client, ranks, hist)
    print(history(client))

    # # rankings
    # ranking = rankings(client)
    # print(ranking)

    # # head-to-head
    # head = head2head(client, "Logan", "Jack")
    # print(head)

    # # history
    # hist = history(client)
    # print(hist)
