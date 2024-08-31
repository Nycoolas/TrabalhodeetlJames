import json
import requests

res = requests.get('https://jsonplaceholder.typicode.com/users') # Extract
obj = json.loads(res.text)

arquivo = open('./resultado1.json','w+')
arquivo.write(json.dumps(obj))
arquivo.close()

arquivo2 = open('./resulta2.csv', 'w+')

#tranformar em csv
for item in obj:
    linha = f"{item['id']}, {item['name']}, {item['username']} \n" #Trasformation
    arquivo2.write(linha) #load
arquivo2.close()


res_posts = requests.get('https://jsonplaceholder.typicode.com/posts')

arquivo3 = open('./resulta3.csv', 'w+')

#tranformar em csv
for item in json.loads(res_posts.text):
    linha = f"{item['userId']}, {item['id']}, {item['title']} \n" #Trasformation
    arquivo3.write(linha)  #load
arquivo3.close()