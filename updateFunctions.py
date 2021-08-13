from google.cloud import bigquery
import os

from bigqueryClient import *

os.environ[
    "GOOGLE_APPLICATION_CREDENTIALS"
] = "/Users/Jack/Documents/Projects/BigQueryPingPong/input/pingpong-322517-aab02d6e732c.json"


def update_all(client, hist, person1, person2, winner):
    update_rankings(client, person1, person2, winner)
    ranks = rankings(client)
    update_history(client, ranks, hist)
    update_game_history(client, person1, person2, winner)
    print("UPDATED ALL")


def go_back_all(client):
    revert_back_rankings(client)
    delete_previous_game_history(client)
    delete_previous_history(client)


if __name__ == "__main__":
    client = bigquery.Client()
    go_back_all(client)
    # hist = history(client)
    # update_all(client, hist, "Jack", "Logan", "Jack")

    ranks = rankings(client)
    hist = history(client)
    game_hist = game_history(client)

    print(ranks)
    print(hist)
    print(game_hist)
