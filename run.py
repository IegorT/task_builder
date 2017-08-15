#!/usr/bin/env python

import json
import os, sys
#import subprocess

import pika                 # RabbitMQ
import rethinkdb as r       # RethinkDB
import paramiko             # SSHClient

#read data
task_id = int(sys.argv[1])
data_dir =  "task_{0}".format(task_id)
data_file = data_dir + "/data.json"

try:
    with open(data_file) as read_data:    
        data = json.load(read_data)
    print "\nRead Settings --- OK\n"
except:
    print "Error open file"
    exit()

def data2json(task_id, msg):
    return {"task_id": task_id, "status": msg}
    
def changestatus(task_id, status, rt_key="change_status"):
    """
    Send a message in json-format {task_id: task_id, status: status} 
    to RabbitMQ server 
    """
    message = json.dumps(data2json(task_id, status))
    channel.basic_publish(exchange='', routing_key=rt_key, body=message)
    print message+"\n"

def run_command(command):
    stdin, stdout, stderr = ssh.exec_command(command)
    return stdout.read() + stderr.read()
    

def send2db(task_id, command):
    """
    Insert data(task_id, text in console, time now) to RethinkDB server
    """
    message = data2json(task_id, command)
    message['timestamp'] = r.now()
    r.table(data.get('rethinkdb_table')).insert([message]).run()
    

def console2db(task_id, command):
    """
    Send console log to RethinkDB
    """
    send2db(task_id, command)
    print ">> " + command
    task = run_command(command)
    print "console:" + task
    send2db(task_id, task)

    
if data.get('git_url'):
    git_clone = 'git clone {0} ~/workdir'.format(data.get('git_url'))
else:
    git_clone = 'mkdir ~/workdir'
    
command = 'cd workdir; {0}'.format(data.get('run'))
server_name = data.get('server_name', 'localhost')
ssh_port = data.get('server_port')
server_user = data.get('server_user')

#connect to the RabbitMQ server and RethinkDB
"""
#if not localhost server
url_parameters = 'amqp://{0}:{1}@{2}:{3}/%2F'.format(
                    data.get('login_rabbitmq'), 
                    data.get('password_rabbitmq'), 
                    data.get('host_name'), 
                    data.get('port_rabbitmq'))
parameters = pika.connection.URLParameters(url_parameters)
connection = pika.BlockingConnection(parameters)
"""
connection = pika.BlockingConnection(
                pika.ConnectionParameters(data.get('host_name')))
channel = connection.channel()
print "RabbitMQ connection --- OK\n"

r.connect(data.get('host_name'), 
                    data.get('port_rethinkdb'),
                    db=data.get('rethinkdb_db'), 
                    auth_key=data.get('auth_key_rethinkdb')).repl()
print "RethinkDB connection --- OK\n"

#SSH connect
ssh = paramiko.SSHClient()
ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
ssh.connect(server_name, username=server_user, key_filename='.ssh/id_rsa')
print "SSH connection --- OK\n"

#git clone to work dir
changestatus(task_id, "6")
try:
    console2db(task_id, git_clone)

except:
    changestatus(task_id, "7")
    "Error git clone or make workdir"
    exit()
print "\nPrepering for computation --- OK\n"
    
#start task
changestatus(task_id, "8")

#from console to RethinkDB server
print "Start Computation --- OK\n"
try:
    console2db(task_id, command)
except:
    changestatus(task_id, "9")
    print "Computation Error"
    exit()
print "\nYou're God damn right!\n"

#end task
ssh.close()
changestatus(task_id, '10')
connection.close()
os.remove(data_file)
os.removedirs(data_dir)
print "Task dir was removed --- OK\n"

print "The End\n"

