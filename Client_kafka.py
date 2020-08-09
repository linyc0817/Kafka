# pylint: disable=E1101
import socket
import argparse
import threading
import json
import threading
import os
from kafka import KafkaConsumer
from kafka.structs import TopicPartition
import time
from collections import defaultdict

IP = None
PORT = None
login_ip = None
kafka_flag = 0
keyword_dict = defaultdict(list)
sub_topic = []
# topic = ""
# value = ""
# kafka target
# consumer = KafkaConsumer(
#     bootstrap_servers=['127.0.0.1:9092'], consumer_timeout_ms=500)
# consumer_timeout_ms，默认一直循环等待接收，若指定，则超时返回，不再等待
# max_poll_interval_ms = 1000 -> broker就认为这个消费者挂了，就会重新把它从组内删除，并且重新平衡

def send_as_bytes(msg, s):
    b_msg = bytes(msg, encoding="utf-8")
    s.sendall(b_msg)


def handle_send(sock):
  global kafka_flag
  # global topic
  # global value
  while True:
    content = input("").split()
    # print("Input:",content)
    if (len(content) == 0):
        print("%", end='')

    elif (content[0] == "exit"):
        kafka_flag = 1
        # print("set flag", kafka_flag)
        msg = "bye"
        send_as_bytes(msg, sock)
        break

    msg = ' '.join(content)
    send_as_bytes(msg, sock)


def handle_kafka(sock):
  global kafka_flag
  global keyword_dict
  # global topic
  # global value
  # ===
  # print(consumer.topics(), "\n% ", end='')
  # ===
  # print(consumer.position(TopicPartition(topic='test', partition=0)), "\n% ", end='')  # 獲取當前主題的最新偏移量offset
  # print("kafka_flag",kafka_flag)
  # tp = TopicPartition('test', 0)
  # lastOffset = consumer.position(tp)
  consumer = KafkaConsumer(bootstrap_servers=['127.0.0.1:9092'], consumer_timeout_ms=30000, max_poll_interval_ms=500, metadata_max_age_ms = 500)
  # print("consumer reset")

  while True:
    if len(sub_topic) == 0:
      # print("Unsubscribe all")
      consumer.unsubscribe()
    if len(sub_topic) != 0:
      # print("Update subscribe")
      # print("new topci list", sub_topic)
      consumer.subscribe(topics=sub_topic)
    # print(kafka_flag)
    # for message in consumer:
    #     print("new sub message", message.value)
    #     value = message.value.decode("utf-8")
    #     topic = message.topic
    #     print("%s:%d:%d: key=%s value=%s" % (message.topic,message.partition, message.offset, message.key, message.value), "\n% ", end='')
    #     # filter here
    #     if keyword_dict[topic] != []:
    #       for i in range(len(keyword_dict[topic])):
    #         if keyword_dict[topic][i] in value:
    #           # print(value, "\n% ", end='')
    #           # print(topic)
    #           # print(value)
    #           msg = "get_sub_msg " + topic + " " + value
    #           send_as_bytes(msg, sock)
    #           break
    msg_pack = consumer.poll(timeout_ms=1000)
    for tp, messages in msg_pack.items():
      # print(sub_topic)
      # print("new message",tp)
      for message in messages:
          value = message.value.decode("utf-8")
          topic = tp.topic
          # message value and key are raw bytes -- decode if necessary!
          # e.g., for unicode: `message.value.decode('utf-8')`
          # print("%s:%d:%d: key=%s value=%s" % (tp.topic, tp.partition,
                                              # message.offset, message.key,
                                              # message.value))
          if keyword_dict[topic] != []:
            for i in range(len(keyword_dict[topic])):
              if keyword_dict[topic][i] in value:
                # print(value, "\n% ", end='')
                # print(topic)
                # print(value)
                msg = "get_sub_msg " + topic + " " + value
                send_as_bytes(msg, sock)
                break

              
    if kafka_flag == 1:
      break

  consumer.close()

      # 離不開 -> 是要去check每一個topic是否都到最後偏移量然後break?
      # if message.offset == lastOffset - 1:
      #     break


def handle_receive(sock):
  global keyword_dict
  global sub_topic
  # sub_topic = []
  # sub_topic.append("test")
  # sub_topic.append("sample2")
  # print("ini_list ",sub_topic)
  topicname = ""
  keyword = ""
  response2 = ""
  while True:
    # if len(sub_topic) == 0:
    #   # print("Unsubscribe all")
    #   consumer.unsubscribe()
    # if len(sub_topic) != 0:
    #   # print("Update subscribe")
    #   print("new topci list", sub_topic)
    #   consumer.subscribe(topics=sub_topic)
    # kafka subscribe
    # sub_topic.append("sample2")
    # consumer.subscribe(topics=sub_topic)  # 訂閱要消費的主題
    response = sock.recv(4096).decode("utf-8")
    if response == "bye2":
      break
    response2 = response.split()
    # print(response2)
    # print(response2[0])
    if response2[0] == "Subscribe":
      topicname = response2[2]
      keyword = response2[3]
      keyword_dict[topicname].append(keyword)
      # ========
      # print("dictionary: ",keyword_dict)
      # sub_topic.append(topicname)
      if topicname not in sub_topic:
        sub_topic.append(topicname)
      # print(sub_topic, "\n% ", end='')
      print(response2[0] + " " + response2[1], "\n% ", end='')
      # consumer.subscribe(topics=sub_topic)  # 訂閱要消費的主題

    elif (response2[0] == "Unsubscribe"):
        target = response2[2]
        del keyword_dict[target]
        sub_topic.remove(target)
        # print("remove", sub_topic)
        # print("re_key", keyword_dict)

        print(response2[0] + " " + response2[1], "\n% ", end='')
        # if len(sub_topic) != 0:
        #   time.sleep(0.5)
        #   consumer.subscribe(topics=sub_topic)

    elif response2[0] == "list_ok":
      if len(sub_topic) == 0:
        print("Empty Subscribe", "\n% ", end='')
      else:
        for i in range(len(sub_topic)):
          if i == len(sub_topic)-1:
            print(sub_topic[i], ":", keyword_dict[sub_topic[i]], "\n% ", end='')
          else:
            print(sub_topic[i], ":", keyword_dict[sub_topic[i]])

    elif response2[0] == "Bye,":
      sub_topic = []
      keyword_dict = defaultdict(list)
      # print(sub_topic)
      # print(keyword_dict)
      print(response, "\n% ", end='')

    else:
      print(response, "\n% ", end='')
  

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("IP")
    parser.add_argument("PORT")
    args = parser.parse_args()
    global IP, login_ip, PORT
    # global kafka_flag
    # kafka_flag = 0
    login_ip = IP = args.IP

    PORT = int(args.PORT)
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.connect((IP, PORT))
    # get welcome message
    print(sock.recv(1024).decode("utf-8"), end='')

    
    
    send_handler = threading.Thread(
        target=handle_send, args=(sock, ))
    send_handler.start()
    receive_handler = threading.Thread(
        target=handle_receive, args=(sock, ))
    receive_handler.start()
    kafka_handler = threading.Thread(
        target=handle_kafka, args=(sock, ))
    kafka_handler.start()
    

    # print("sender join")
    send_handler.join()
    # print("recieve join")
    receive_handler.join()
    # print("join over")
    # kafka_handler.join()

    sock.shutdown(socket.SHUT_RDWR)
    sock.close()
    


if __name__ == '__main__':
    main()
