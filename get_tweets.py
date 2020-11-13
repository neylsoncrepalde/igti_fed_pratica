import json
from tweepy import OAuthHandler, Stream, StreamListener
from datetime import datetime

# Cadastrar as chaves de acesso
consumer_key = "ok3VXgkDzFeb3wtN4lbMiEEys"
consumer_secret = "jQyqApzYXQQXLMNbMNgHuYW66RothmD5qECR9f1UqHyWyAI6Yw"

access_token = "1327025605502709762-VM7eyKOBVZyG1VQ5xCremqdBBF8GNI"
access_token_secret = "qErwhBudDDCUhzl6UTC4NH1QSAbn3DVCrc3wxDpn0bx9z"

# Definir um arquivo de saída para armazenar os tweets coletados
data_hoje = datetime.now().strftime("%Y-%m-%d-%H-%M-%S")
out = open(f"collected_tweets_{data_hoje}.txt", "w")

# Implementar uma classe para conexão com o Twitter

class MyListener(StreamListener):

    def on_data(self, data):
        itemString = json.dumps(data)
        out.write(itemString + "\n")
        return True

    def on_error(self, status):
        print(status)


# Implementar nossa função MAIN
if __name__ == "__main__":
    l = MyListener()
    auth = OAuthHandler(consumer_key, consumer_secret)
    auth.set_access_token(access_token, access_token_secret)

    stream = Stream(auth, l)
    stream.filter(track=["Trump"])