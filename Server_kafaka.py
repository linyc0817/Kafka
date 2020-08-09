import socket
import argparse
import threading
import json
import sqlite3
from sqlite3 import IntegrityError
from datetime import date
import datetime
from kafka import KafkaProducer
from kafka.admin import KafkaAdminClient, NewTopic
from collections import defaultdict
import time


# db
# where data store in user.db file
dbConn = sqlite3.connect('user.db')
# cursor
cursorObj = dbConn.cursor()


admin_client = KafkaAdminClient(
    bootstrap_servers="localhost:9092"
)
# kafka
producer = KafkaProducer(bootstrap_servers='localhost:9092')


def send_byte_json(msg, conn):
    # 將dict型別的資料轉成str
    response = json.dumps(msg.get("message"))
    # 刪除前後 " "
    response = response[1:-1]

    # print(response)
    if response == "":
        # 為了換行 for telnet
        try:
            conn.send("% ".encode('utf-8'))
        except (BrokenPipeError, IOError):
            print('BrokenPipeError caught')
    else:
        # for client 在那換行
        conn.send(response.encode('utf-8'))


def handle(data, conn, login_d, tmp_topic_list, keyword_dict):
    # print(dic[username])
    # inputs = data.split()
    inputs = data.decode('ascii').split()
    
    # print(len(inputs))
    # thread name
    thread_name = threading.current_thread().name
    # print(thread_name)
    # print(inputs)

    if len(inputs) == 0:
        response = {"message": ""}
        send_byte_json(response, conn)
    else:
        # GET Client command from decode
        command_type = inputs[0]
        # print(command_type)

        # Exit command
        if command_type == "bye":
            print("Connection End")
            msg = "bye2"
            conn.send(msg.encode('utf-8'))
        
        # elif command_type =="kafka-new":
        #     topic_list = []
        #     topic_list.append(NewTopic(name="example_topic", num_partitions=1, replication_factor=1))
        #     admin_client.create_topics(new_topics=topic_list, validate_only=False)

        #     response = {"message": "Kafka successfully."}
        #     send_byte_json(response, conn)

        # elif command_type =="kafka-delete":
        #     topic_list=[]
        #     topic_list.append("example_topic")
        #     admin_client.delete_topics(topic_list)

        #     response = {"message": "Kafka delete successfully."}
        #     send_byte_json(response, conn)

        # elif command_type == "kafka-add-msg":
        #     msg = inputs[1]
        #     producer.send('sample2', msg.encode('utf-8'))

        #     response = {"message": "Kafka msg successfully."}
        #     send_byte_json(response, conn)

        # Register command
        elif command_type == "register":
            # format error
            if len(inputs) != 4:
                response = {
                    "message": "Usage: register <username> <email> <password>"
                }
                send_byte_json(response, conn)
                return

            # register
            username = inputs[1]
            email = inputs[2]
            password = inputs[3]

            # db
            dbConn_thread = sqlite3.connect('user.db')
            cursorObj_thread = dbConn_thread.cursor()
            try:
                cursorObj_thread.execute(
                    "INSERT INTO users (username, email, password) VALUES (?, ?, ?)", (username, email, password))

                # add username kafka topic
                topic_list = []
                topic_list.append(NewTopic(name=username, num_partitions=1, replication_factor=1))
                admin_client.create_topics(new_topics=topic_list, validate_only=False)

                dbConn_thread.commit()
                dbConn_thread.close()
    #            print("Data in")
            except IntegrityError:
                response = {
                    "message": "Username is already used."
                }
                send_byte_json(response, conn)
                return

            response = {"message": "Register successfully."}
            send_byte_json(response, conn)

        # Login command
        elif command_type == "login":
            # format error
            if len(inputs) != 3:
                response = {
                    "message": "Usage: login <username> <password>"
                }
                send_byte_json(response, conn)
                return

            # login
            username = inputs[1]
            password = inputs[2]
            # db
            dbConn_thread = sqlite3.connect('user.db')
            cursorObj_thread = dbConn_thread.cursor()

            find_user = ("SELECT * FROM users WHERE username = ? AND password = ?")
            cursorObj_thread.execute(find_user, [(username), (password)])
            results = cursorObj_thread.fetchall()

            if results:
                # if not already login
                if not thread_name in login_d:
                    login_d[thread_name] = username
    #                print(d)
                    response = {"message": "Welcome, {}.".format(username)}
                    send_byte_json(response, conn)
                else:
                    #                print(d)
                    response = {"message": "Please logout first."}
                    send_byte_json(response, conn)
            else:
                response = {"message": "Login failed."}
                send_byte_json(response, conn)

        # Logout command
        elif command_type == "logout":
            if thread_name in login_d:
                username = login_d[thread_name]
                del login_d[thread_name]
                tmp_topic_list = []
                keyword_dict = defaultdict(list)
    #            print(d)
                response = {"message": "Bye, {}.".format(username)}
                send_byte_json(response, conn)
            else:
                response = {"message": "Please login first."}
                send_byte_json(response, conn)
        
        # Whoami command
        elif command_type == "whoami":
            if thread_name in login_d:
                username = login_d[thread_name]
                response = {"message": "{}".format(username)}
                send_byte_json(response, conn)
            else:
                response = {"message": "Please login first."}
                send_byte_json(response, conn)
        
        # ================================= board cmd
        # create board
        elif command_type == "create-board":

            if len(inputs) != 2:
                response = {
                    "message": "Usage: create-board <name>"
                }
                send_byte_json(response, conn)
                return

            #db
            dbConn_thread = sqlite3.connect('user.db')
            cursorObj_thread = dbConn_thread.cursor()

            boardname = inputs[1]

            if thread_name in login_d:

                # get moderator
                username = login_d[thread_name]

                try:
                    cursorObj_thread.execute(
                        "INSERT INTO boards (boardname, moderator) VALUES (?, ?)", (boardname, username))

                    # add board kafka topic
                    topic_list = []
                    topic_list.append(NewTopic(name=boardname, num_partitions=1, replication_factor=1))
                    admin_client.create_topics(new_topics=topic_list, validate_only=False)

                    dbConn_thread.commit()
                    dbConn_thread.close()

                except IntegrityError:
                    response = {
                        "message": "Board already exist."
                    }
                    send_byte_json(response, conn)
                    return

                response = {"message": "Create board successfully."}
                send_byte_json(response, conn)

            else:
                response = {"message": "Please login first."}
                send_byte_json(response, conn)
        # list-board
        elif command_type == "list-board":
            msg_final = ""
            #db
            dbConn_thread = sqlite3.connect('user.db')
            cursorObj_thread = dbConn_thread.cursor()

            # list first row
            msg = "  Index\tName\tModerator\n"

            # keyword list
            if len(inputs) == 2:
                target = inputs[1].lstrip('#')

                results = cursorObj_thread.execute("SELECT * FROM boards")
                id = 0
                for item in results:
                    # print(item[0])
                    if target in item[0]:
                        id = id + 1
                        msg2 = "  {}\t{}\t{}\n".format(id, item[0], item[1])
                        msg_final = msg_final + msg2

            # normal list
            else:
                # list boards
                results = cursorObj_thread.execute("SELECT * FROM boards")
                id = 0
                for item in results:
                    id = id + 1
                    msg2 = "  {}\t{}\t{}\n".format(id, item[0], item[1])
                    msg_final = msg_final + msg2

            msg_send = msg + msg_final
            conn.send(msg_send.encode('utf-8'))

            dbConn_thread.commit()
            dbConn_thread.close()

        # create-post
        elif command_type == "create-post":

            #db
            dbConn_thread = sqlite3.connect('user.db')
            cursorObj_thread = dbConn_thread.cursor()

            if thread_name in login_d:

                # get Author
                username = login_d[thread_name]
                # get date
                today = date.today()

                boardname = inputs[1]
                titlename = ""
                contentname = ""
                if inputs[2] == "--title":
                    j = 3
                    while inputs[j] != "--content":
                        titlename = titlename + ' ' + inputs[j]
                        j = j + 1
    #                print(titlename)
                    titlename = titlename.lstrip(' ')
                    j = j + 1
                    while j < len(inputs):
                        contentname = contentname + ' ' + inputs[j]
                        j = j + 1
    #                print(contentname)
                    contentname = contentname.lstrip(' ')
                    contentname = contentname.replace('<br>', '\n ')

                day = today.strftime("%m/%d")
                day2 = today.strftime("%Y-%m-%d")

                # check if boardname exist
                cursorObj_thread.execute(
                    "SELECT COUNT(*) FROM boards WHERE boardname = ? ", (boardname, ))
                # c.f. fetchone() -> 返回一個數組，fetchall的第一組
                result = cursorObj_thread.fetchall()
                check = result[0][0]

                if check:
                    cursorObj_thread.execute(
                        "INSERT INTO posts (boardname, title, author, content, date, date2) VALUES (?, ?, ?, ?, ?, ?)", (boardname, titlename, username, contentname, day, day2))
                    
                    msg = titlename

                    producer.send(boardname, msg.encode('utf-8'))
                    # time.sleep(0.5)
                    producer.send(username, msg.encode('utf-8'))
                    # time.sleep(0.5)


                    dbConn_thread.commit()
                    dbConn_thread.close()

                    response = {"message": "Create post successfully."}
                    send_byte_json(response, conn)

                else:

                    response = {"message": "Board does not exist."}
                    send_byte_json(response, conn)

            else:
                response = {"message": "Please login first."}
                send_byte_json(response, conn)

        # list-post
        elif command_type == "list-post":

            if not (len(inputs) == 2 or len(inputs) == 3):
                response = {
                    "message": "Usage: list-post <board-name> ##<key>"
                }
                send_byte_json(response, conn)
                return

            boardname = inputs[1]
            #db
            dbConn_thread = sqlite3.connect('user.db')
            cursorObj_thread = dbConn_thread.cursor()

            # check if boardname exist
            cursorObj_thread.execute(
                "SELECT COUNT(*) FROM boards WHERE boardname = ?", (boardname, ))
            result = cursorObj_thread.fetchall()
            check = result[0][0]

            if check:
                msg_final = ""
                # list first row
                msg = "  ID\tTitle\tAuthor\tDate\n"
                # conn.send(msg.encode('utf-8'))

                # keyword list
                if len(inputs) == 3:
                    target = inputs[2].lstrip('#')
                    # not using title LIKE ? -> can seperate big and little letter
                    results = cursorObj_thread.execute(
                        "SELECT * FROM posts WHERE boardname = ?", (boardname, ))
                    for item in results:
                        if target in item[2]:
                            msg2 = "  {}\t{}\t{}\t{}\n".format(
                                item[0], item[2], item[3], item[5])
                            # conn.send(msg2.encode('utf-8'))
                            msg_final = msg_final + msg2

                else:
                    results = cursorObj_thread.execute(
                        "SELECT * FROM posts WHERE boardname = ?", (boardname, ))
                    for item in results:
                        msg2 = "  {}\t{}\t{}\t{}\n".format(
                            item[0], item[2], item[3], item[5])
                        # conn.send(msg2.encode('utf-8'))
                        msg_final = msg_final + msg2

                msg_send = msg + msg_final
                conn.send(msg_send.encode('utf-8'))

            else:
                response = {"message": "Board does not exist."}
                send_byte_json(response, conn)

            dbConn_thread.commit()
            dbConn_thread.close()

        # read
        elif command_type == "read":
            if len(inputs) != 2:
                response = {
                    "message": "Usage: read <post-id>"
                }
                send_byte_json(response, conn)
                return

            #db
            dbConn_thread = sqlite3.connect('user.db')
            cursorObj_thread = dbConn_thread.cursor()

            postId = inputs[1]

            # check if post-id exist
            cursorObj_thread.execute(
                "SELECT COUNT(*) FROM posts WHERE id = ?", (postId, ))
            result = cursorObj_thread.fetchall()
            check = result[0][0]

            if check:
                msg2 = ""
                msg3 = ""
                #            print('id exists')

                results = cursorObj_thread.execute(
                    "SELECT * FROM posts WHERE id = ?", (postId, ))
                for item in results:
                    msg2 = " Author  :{}\n Title   :{}\n Date    :{}\n\n --\n\n {}\n\n --\n\n".format(
                        item[3], item[2], item[6], item[4])
                    # conn.send(msg2.encode('utf-8'))

                comments = cursorObj_thread.execute(
                    "SELECT * FROM comments WHERE id = ?", (postId, ))
                for item2 in comments:
                    msg3 = " {}: {}\n".format(item2[1], item2[2])
                    # conn.send(msg3.encode('utf-8'))

                msg = msg2 + msg3
                conn.send(msg.encode('utf-8'))
            else:
                response = {"message": "Post does not exist."}
                send_byte_json(response, conn)

            dbConn_thread.commit()
            dbConn_thread.close()

        # comment
        elif command_type == "comment":

            #db
            dbConn_thread = sqlite3.connect('user.db')
            cursorObj_thread = dbConn_thread.cursor()

            # check login
            if thread_name in login_d:

                # get post-id
                postId = inputs[1]

                # check post exist
                cursorObj_thread.execute(
                    "SELECT COUNT(*) FROM posts WHERE id = ?", (postId, ))
                result = cursorObj_thread.fetchall()
                check = result[0][0]

                if check:
                    # get Commenter
                    username = login_d[thread_name]

                    # get content
                    content = ""
                    j = 2
                    while(j < len(inputs)):
                        content = content + ' ' + inputs[j]
                        j = j + 1
                    content = content.lstrip(' ')

                    # get date
                    today = date.today()
                    day = today.strftime("%Y-%m-%d")

                    cursorObj_thread.execute(
                        "INSERT INTO comments (id, commenter,content, date) VALUES (?, ?, ?, ?)", (postId, username, content, day))
                    dbConn_thread.commit()
                    dbConn_thread.close()

                    response = {"message": "Comment successfully."}
                    send_byte_json(response, conn)
                else:
                    response = {"message": "Post does not exist."}
                    send_byte_json(response, conn)

            else:
                response = {"message": "Please login first."}
                send_byte_json(response, conn)

        # update-post
        elif command_type == "update-post":

            #db
            dbConn_thread = sqlite3.connect('user.db')
            cursorObj_thread = dbConn_thread.cursor()

            # check login
            if thread_name in login_d:

                access = 0
                # get post-id
                postId = inputs[1]

                # check post exist
                cursorObj_thread.execute(
                    "SELECT COUNT(*) FROM posts WHERE id = ?", (postId, ))
                result = cursorObj_thread.fetchall()
                check = result[0][0]

                if check:

                    # get user now
                    username = login_d[thread_name]

                    posts = cursorObj_thread.execute(
                        "SELECT * FROM posts WHERE id = ?", (postId, ))
                    for post in posts:
                        if post[3] == username:
                            access = 1
                        else:
                            response = {"message": "Not the post owner."}
                            send_byte_json(response, conn)

                    # able to update
                    if access == 1:

                        purpose = inputs[2]
                        # change = inputs[3]
                        titleChange = ""
                        contentChange = ""
                        j = 3
                        if purpose == "--title":

                            while(j < len(inputs)):
                                titleChange = titleChange + ' ' + inputs[j]
                                j = j + 1
    #                            print(titleChange)
                            titleChange = titleChange.lstrip(' ')

                            cursorObj_thread.execute(
                                "UPDATE posts SET title = ? WHERE id = ?", (titleChange, postId))
                            dbConn_thread.commit()
                            dbConn_thread.close()
                        elif purpose == "--content":

                            while(j < len(inputs)):
                                contentChange = contentChange + ' ' + inputs[j]
                                j = j + 1
    #                            print(contentChange)
                            contentChange = contentChange.lstrip(' ')
                            contentChange = contentChange.replace('<br>', '\n ')

                            cursorObj_thread.execute(
                                "UPDATE posts SET content = ? WHERE id = ?", (contentChange, postId))
                            dbConn_thread.commit()
                            dbConn_thread.close()

                        response = {"message": "Update successfully."}
                        send_byte_json(response, conn)

                else:
                    response = {"message": "Post does not exist."}
                    send_byte_json(response, conn)
            else:
                response = {"message": "Please login first."}
                send_byte_json(response, conn)

        # delete
        elif command_type == "delete-post":
            if len(inputs) != 2:
                response = {
                    "message": "Usage: delete-post <post-id>"
                }
                send_byte_json(response, conn)
                return

            #db
            dbConn_thread = sqlite3.connect('user.db')
            cursorObj_thread = dbConn_thread.cursor()

            # check login
            if thread_name in login_d:

                access = 0
                # get post-id
                postId = inputs[1]

                # check post exist
                cursorObj_thread.execute(
                    "SELECT COUNT(*) FROM posts WHERE id = ?", (postId, ))
                result = cursorObj_thread.fetchall()
                check = result[0][0]

                if check:
                    # get user now
                    username = login_d[thread_name]

                    posts = cursorObj_thread.execute(
                        "SELECT * FROM posts WHERE id = ?", (postId, ))
                    for post in posts:
                        if post[3] == username:
                            access = 1
                        else:
                            response = {"message": "Not the post owner."}
                            send_byte_json(response, conn)

                    # able to delete
                    if access == 1:
                        cursorObj_thread.execute(
                            "DELETE FROM posts WHERE id= ?", (postId))
                        cursorObj_thread.execute(
                            "DELETE FROM comments WHERE id= ?", (postId))
                        dbConn_thread.commit()
                        dbConn_thread.close()

                        response = {"message": "Delete successfully."}
                        send_byte_json(response, conn)
                else:
                    response = {"message": "Post does not exist."}
                    send_byte_json(response, conn)
            else:
                response = {"message": "Please login first."}
                send_byte_json(response, conn)

        # ======== subscribe cmds
        elif command_type == "subscribe":
            if len(inputs) < 3 or (inputs[1] != "--board" and inputs[1] != "--author") or "--keyword" not in inputs:
                response = {
                    "message": "Usage: subscribe --board <board-name> / --author <author-name> --keyword <keyword>"
                }
                send_byte_json(response, conn)
                return
            
            topicname = ""
            keyword = ""

            # check login
            if thread_name in login_d:
                if inputs[1] == "--author" or inputs[1] == "--board":
                    j = 2
                    while inputs[j] != "--keyword":
                        topicname = topicname + ' ' + inputs[j]
                        j = j + 1
    #                print(topicname)
                    topicname = topicname.lstrip(' ')
                    j = j + 1
                    while j < len(inputs):
                        keyword = keyword + ' ' + inputs[j]
                        j = j + 1
    #                print(keyword)
                    keyword = keyword.lstrip(' ')
                    # keyword = keyword.replace('<br>', '\n ')
                # print(topicname)
                # print(keyword)

                if (topicname not in tmp_topic_list) or (keyword not in keyword_dict[topicname]):

                    tmp_topic_list.append(topicname)
                    # print("tmp", tmp_topic_list)

                    keyword_dict[topicname].append(keyword)

                    # msg = "subscribe_msg " + topicname + " "+ keyword 
                    # conn.send(msg.encode('utf-8'))

                    response = {"message": "Subscribe successfully {} {}".format(topicname, keyword)}
                    send_byte_json(response, conn)
                    # response = {"message": "subscribe_msg2 {} {}".format(topicname,keyword)}
                    # send_byte_json(response, conn)
                else:
                    response = {"message": "Already subscribed"}
                    send_byte_json(response, conn)
            else:
                response = {"message": "Please login first."}
                send_byte_json(response, conn)

        elif command_type == "get_sub_msg":
            posttitle = ""
            # print("get_sub_msg", inputs)
            topicname = inputs[1]
            # get posttitle
            j = 2
            while(j < len(inputs)):
                posttitle = posttitle + ' ' + inputs[j]
                j = j + 1
            posttitle = posttitle.lstrip(' ')
            # print(posttitle)
            # posttitle = inputs[2]
            time.sleep(0.5)
            #db
            dbConn_thread = sqlite3.connect('user.db')
            cursorObj_thread = dbConn_thread.cursor()

            # check post exist with subscribe author
            cursorObj_thread.execute(
                "SELECT COUNT(*) FROM posts WHERE (author, title) = (?, ?)", (topicname, posttitle))
            result = cursorObj_thread.fetchall()
            check = result[0][0]
            # print("check1", check)
            # check post exist with subscribe boardname
            cursorObj_thread.execute(
                "SELECT COUNT(*) FROM posts WHERE (boardname, title) = (?, ?)", (topicname, posttitle))
            result = cursorObj_thread.fetchall()
            check2 = result[0][0]
            # print("check2", check2)

            if check:
                posts = cursorObj_thread.execute("SELECT * FROM posts WHERE (author, title) = (?, ?)", (topicname, posttitle))
                for post in posts:
                    msg = "*[" + post[1] + "] " + post[2] + " - by " + post[3] + "*"
                    conn.send(msg.encode('utf-8'))
            elif check2:
                posts = cursorObj_thread.execute("SELECT * FROM posts WHERE (boardname, title) = (?, ?)", (topicname, posttitle))
                for post in posts:
                    msg = "*[" + post[1] + "] " + post[2] + " - by " + post[3] + "*"
                    conn.send(msg.encode('utf-8'))
            else:
                msg = "Cannot find subscribe article"
                conn.send(msg.encode('utf-8'))

        elif command_type == "unsubscribe":
            target = inputs[2]
            target_kind = inputs[1]
            # check login
            if thread_name in login_d:
                if target_kind == "--board":
                    if target in tmp_topic_list and target in tmp_topic_list:
                        tmp_topic_list.remove(target)
                        del keyword_dict[target]
                        # print(keyword_dict)
                        response = {"message": "Unsubscribe successfully {}".format(target)}
                        send_byte_json(response, conn)
                    else:
                        response = {"message": "You haven't subscribed {}".format(target)}
                        send_byte_json(response, conn)

                elif target_kind == "--author":
                    if target in tmp_topic_list and target in tmp_topic_list:
                        tmp_topic_list.remove(target)
                        del keyword_dict[target]
                        # print(keyword_dict)
                        response = {"message": "Unsubscribe successfully {}".format(target)}
                        send_byte_json(response, conn)
                    else:
                        response = {"message": "You haven't subscribed {}".format(target)}
                        send_byte_json(response, conn)
                else:
                    response = {"message": "unsubscribe --board < board-name > / --author <author-name>"}
                    send_byte_json(response, conn)
            else:
                response = {"message": "Please login first."}
                send_byte_json(response, conn)
        
        elif command_type == "list-sub":
            if thread_name in login_d:
                msg = "list_ok"
                conn.send(msg.encode('utf-8'))
            else:
                response = {"message": "Please login first."}
                send_byte_json(response, conn)
        # unknown command
        else:
            response = {
                "message": "Unknown command {}".format(command_type)
            }
            send_byte_json(response, conn)


def handle_client(conn, addr):
    print("New Connection from ", addr[0], ":", addr[1])


    # login dictionary
    login_d = {}
    # tmp_topic_list
    tmp_topic_list = []
    # keyword dic
    keyword_dict = defaultdict(list)

    while (True):
        try:
            data = conn.recv(1024)
            if not data:
                print(addr[0], ":", addr[1], " disconnected")
                break
            # r_data = data.decode("utf-8")
            # Client input command
            handle(data, conn, login_d, tmp_topic_list, keyword_dict)
        except socket.timeout:
            print("bye bye ~~")
            conn.settimeout(None)
            break


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("PORT")
    args = parser.parse_args()
    global IP
    global PORT
    IP = socket.gethostname()
    PORT = int(args.PORT)
    print(IP)

    # === DB tables

    # Delete exist TABLE which already named 'users'
    cursorObj.execute('DROP TABLE IF EXISTS users')
    # TABLE name 'users'
    cursorObj.execute('CREATE TABLE IF NOT EXISTS users('
                      'username TEXT NOT NULL UNIQUE, '
                      'email TEXT NOT NULL, '
                      'password TEXT NOT NULL)')

    cursorObj.execute('Drop TABLE IF EXISTS boards')

    cursorObj.execute('CREATE TABLE IF NOT EXISTS boards('
                      'boardname TEXT NOT NULL UNIQUE, '
                      'moderator TEXT NOT NULL)')

    cursorObj.execute('Drop TABLE IF EXISTS posts')

    cursorObj.execute('CREATE TABLE IF NOT EXISTS posts('
                      'id integer PRIMARY KEY AUTOINCREMENT, '
                      'boardname TEXT NOT NULL, '
                      'title TEXT NOT NULL, '
                      'author TEXT NOT NULL, '
                      'content TEXT NOT NULL, '
                      'date TEXT NOT NULL, '
                      'date2 TEXT NOT NULL)')

    cursorObj.execute('Drop TABLE IF EXISTS comments')

    cursorObj.execute('CREATE TABLE IF NOT EXISTS comments('
                      'id integer, '
                      'commenter TEXT NOT NULL, '
                      'content TEXT NOT NULL, '
                      'date TEXT NOT NULL)')


    try:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
            # 保證端口地址可重用
            sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            sock.bind((IP, PORT))
            # 10 = backlog, the maximum num to connect
            sock.listen(10)

            
            while (True):
                # conn -> new socket to transfer datas
                (conn, addr) = sock.accept()

                # check connected
#                print('Connected by', addr)
                # msg send need to be bytes-like
                msg = '********************************\n** Welcome to the BBS server. **\n********************************\n% '
                conn.send(msg.encode('utf-8'))
                # 設置conn socket超時時間 -> 超時bye~bye~
                # conn.settimeout(10)


                # Threading
                t = threading.Thread(target=handle_client, args=(conn, addr))

                t.start()
#                print("Thread Start")

    except KeyboardInterrupt:
        print("keyboard interrupt")
        sock.close()

    #db
    dbConn.commit()
    dbConn.close()

if __name__ == '__main__':
    main()
