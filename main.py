import mysql.connector
import socket
import json
import re
import requests
from http.server import BaseHTTPRequestHandler
from io import BytesIO
import urllib.parse

mydb = mysql.connector.connect(
  host="localhost",
  user="root",
  password="Ahmad2891=",
  database = "mysql"

)

mycursor = mydb.cursor()
TABLE_NAME = 'Geolocation2'
# mycursor.execute(f"CREATE TABLE {TABLE_NAME} (location1 VARCHAR(255),location2 VARCHAR(255) ,distance INTEGER ,freq INTEGER )")

class HTTPRequest(BaseHTTPRequestHandler):
    def __init__(self, request_text):
        self.rfile = BytesIO(request_text)
        self.raw_requestline = self.rfile.readline()
        self.error_code = self.error_message = None
        self.parse_request()

    def send_error(self, code, message):
        self.error_code = code
        self.error_message = message
class GeolocationServer:
    def __init__(self,host='127.0.0.1',port=8080):
        self.HOST = host
        self.PORT = port

    def run(self):
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.bind((self.HOST,self.PORT))
            s.listen()
            print("waiting for connection ...")
            conn, addr = s.accept()
            with conn:
                print('Connected by', addr)
                while True:
                    data = conn.recv(1024).decode("utf-8")
                    if not data:
                        break
                    request = HTTPRequest(bytes(data, "utf-8"))
                    msg_command = request.command
                    if msg_command == "GET":
                        flag= request.path[3::]

                        if flag == "hello":
                            data = self.__prepare_result({},200)
                            print(data)
                            conn.sendall(data.encode())  # send data to the client
                        elif flag=="popularsearch":
                            data = self.__get_max_freq()
                            data = self.__prepare_result(data,200)

                            conn.sendall(data.encode())  # send data to the client
                        elif flag=="health":
                            if mydb:
                                #healthy
                                data = self.__prepare_result({}, 200)
                                conn.sendall(data.encode())  # send data to the client

                            else:
                                #not
                                data = self.__prepare_result({"Connection with the DB was unsuccessful":0}, 500)
                                conn.sendall(data.encode())

                        else:
                            src, dest = self.__parse_distance_msg(request.path)
                            if not src and not dest:
                                # this means that the format wasn't valid -> do something
                                data = self.__prepare_result({"invalid input location ":0}, 500)
                                conn.sendall(data.encode())
                            print(src)
                            print(dest)
                            distInKM = self.__get_distance(src, dest)
                            if distInKM == 0 and src != dest :
                                data = self.__prepare_result({"invalid input location":0}, 500)
                                conn.sendall(data.encode())
                                continue
                            data = self.__prepare_result({"distance": distInKM}, 200)
                            conn.sendall(data.encode())

                    else:
                        print("post request")
                        print(data)

                        # print(request.request)
                        length = int(request.headers['content-length'])
                        field_data = request.rfile.read(length)
                        fields = urllib.parse.urlparse(field_data)
                        json_obj = json.loads(fields.path)
                        print(json_obj)
                        if not self.__find_in_db(json_obj['source'],json_obj['destination']):
                            self.__add_to_table(json_obj['source'],json_obj['destination'],json_obj['distance'],1)
                        else:
                            print("om here")
                        print("source and dist : ",json_obj['source'],json_obj['destination'])
                        myresult = self.__find_in_db(json_obj['source'],json_obj['destination'])[0]
                      #  line_number = 0
                        src_arg = 0
                        dest_arg = 1
                       # distance_arg = 2
                        freq_arg = 3

                        data = self.__prepare_result( {"source": myresult[src_arg],
                                                       "destination": myresult[dest_arg],
                                                       "hits": myresult[freq_arg]}, 201)
                        conn.sendall(data.encode())  # send data to the client

                    break
                conn.close()



    def __get_max_freq(self):
        mycursor=mydb.cursor()
        sql =f"SELECT location1, location2 , distance, freq" \
             f" FROM {TABLE_NAME} WHERE " \
             f"freq = (SELECT MAX(freq)   FROM  {TABLE_NAME})" \
             "LIMIT 1"

        mycursor.execute(sql)
        myresult = mycursor.fetchall()
        mydb.commit()
        line_number = 0
        src_arg = 0
        dest_arg = 1
        freq_arg = 3

        data = {"source": myresult[line_number][src_arg], "destination": myresult[line_number][dest_arg], "hits": myresult[line_number][freq_arg]}

        return data

    def __get_distance(self,src ,dest):
        result = self.__find_in_db(src,dest)
        if not result:
            loc1 = src.replace(" " ,"+")
            loc2 = dest.replace(" " ,"+")
            result= requests.get('https://www.distance24.org/route.json?stops='+loc1+'|'+loc2)
            distInKM=  result.json()['distance']
            self.__add_to_table(src, dest, distInKM, 1)
        else:
            line_number = 0
            src_arg=0
            dest_arg=1
            distance_arg = 2
            freq_arg=3

            distInKM = result[line_number][distance_arg]
            print("freq: ",result[line_number][freq_arg])
            #edit freq

            self.__edit_frequency(result[line_number][src_arg],result[line_number][dest_arg])
        return distInKM

    def __edit_frequency(self,src,dest):
        mycursor=mydb.cursor()
        sql = f"UPDATE {TABLE_NAME} SET freq = freq+1 WHERE location1 = '{src}' and location2 = '{dest}'"
        mycursor.execute(sql)

        mydb.commit()


    def __find_in_db(self,loc1,loc2 ):
        mycursor = mydb.cursor()
        sql = f"SELECT * FROM {TABLE_NAME} WHERE  location1 = '{loc1}' and location2 = '{loc2}' or  location1 = '{loc2}' and location2 = '{loc1}' "
        mycursor.execute(sql)
        myresult = mycursor.fetchall()

        return myresult

    def __add_to_table(self,src,dest,distance,freq):
        val = (src, dest, distance, freq)
        print(val)
        sql = f"INSERT INTO {TABLE_NAME}  (location1, location2 , distance, freq) VALUES (%s, %s, %s, %s)"
        mycursor.execute(sql, val)
        mydb.commit()

    def __prepare_result(self,res,response_code):
        """

        :param res: json object
        :return:
        """
        print("preparing : ",res)
        json_dump = json.dumps(res)
        data = f"HTTP/1.1 {response_code} OK\r\n"
        data += "Content-Type:application/json; charset=utf-8\r\n"
        data += "\r\n"

        data += f"{json_dump}\r\n"
        return data


    def __parse_distance_msg(self,distance_data):
        """

        :param distance_data:
        :return: a tuple representing (src,dest)
        """
        # if dist
        pattern = re.compile(r'/\?=distance\?source=(.*)%26destination=(.*)')
        match = pattern.search(distance_data)
        if match:
            src, dest = match.group(1).replace("%20"," "), match.group(2).replace("%20"," ")
            print(src,dest)
            return src,dest
        else:
            print("invalid format")
            return None,None

if __name__ == '__main__':

  myserver = GeolocationServer()
  myserver.run()


