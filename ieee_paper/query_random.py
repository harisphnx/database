###################### importing libraries #########################
import sys
import json
from random import randint 
import matplotlib.pyplot as plt
import numpy as np
from scipy import stats
from socket import *                # Imports socket module

############################## declarations, initialization and user input #################################

table_number = 0
temp_array =  []

tables = [[] for i in range(10)]        #### stores the tables taking part in the query ####

mode = int(raw_input("Local query(1) Or Remote query(2)\n"))
query = ""
if(mode == 1):
	query = raw_input("enter the query\n")  #### reading the query ####
elif(mode == 2):
	print ""
	### server code for listening and receiving query from remote system ###

	host="10.100.53.154"                     # Set the server address to host (this machine) ip address
	port = 4446                              # Sets the variable port to 4446

	s=socket(AF_INET, SOCK_STREAM)

	s.bind((host,port))                      # Binds the socket. Note that the input to the bind function is a tuple
	s.listen(1)                              # Sets socket to listening state with a  queue of 1 connection
	print "Listening for connections.. "
	q,addr=s.accept()                        # Accepts incoming request from client and returns socket and address to variables q and addr
                                                 # user
	query = q.recv(1024)                     # receiving query remotely
	print "received query: ", query
	s.close()
else:
	print "wrong choice punk"
	sys.exit(0)
check_from1 = 'from '
check_from2 = 'From '

x = 0
temp_from = []
flag = 0
i = 0

temp_sched = ""
sched_num = 0
begin = 0
end = 0
######### function to                             ###########
######### 1: generate schedules and               ###########
######### 2: resolve schedules into schedule[]    ###########
######### 3: calculate the cost of schedules      ###########

def create_schedule(temp_root, caller, temp_table_matrix, edge_start, edge_end, manipulator): ### manipulator is used for mutation ###
	for i in range(n):
		if(temp_table_matrix[temp_root][i] != "0" and i != caller):
			temp_rand = randint(0,100) % 2
			if( manipulator == 1 or temp_rand == 0):
				temp_table_matrix[i][temp_root] = str(edge_end)  ## single edged
				edge_end = edge_end - 1
				temp_table_matrix[temp_root][i] = "0"
			elif(manipulator == 2 or temp_rand == 1):
				temp_table_matrix[temp_root][i] = str(edge_start)  ## double edged
				edge_start = edge_start + 1
				temp_table_matrix[i][temp_root] = str(edge_end)
				edge_end = edge_end - 1
			create_schedule(i, temp_root, temp_table_matrix, edge_start, edge_end, 0)

def resolve_schedule(temp_table_matrix, temp_sched, temp_root, last_semijoin):
	for i in range(1, (2*(n-1)+1)):
		for k in range(n):
			for l in range(n):
				if(temp_table_matrix[k][l] == str(i) ):
					temp_sched = temp_sched + str(k) + "," + str(l) + ": " 
	if(last_semijoin != 0):
		temp_sched = temp_sched + str(last_semijoin) + "," + str(temp_root) + "."
	return temp_sched

def calc_cost(temp_sched, table_size):
	cost = 0.0
        temp_relation_reduce2 = 0.0
        for i in range(4):
                temp_table_size[i] = table_size[i]
        i = 0
        j = 0
        while( i < len(temp_sched) ):
                while( j < len(temp_sched) and temp_sched[j] != ":" and temp_sched[j] != "." ):   ### extracts joins from scedule ###
                        j = j + 1
                temp_join = temp_sched[i:j]
                #print temp_join
                j = j + 2
                i = j
		if(i < len(temp_sched) and (temp_sched[i] == "," or temp_sched[i] == ".")):
			i = i - 1
			j = i
                table_to = int(temp_join[0])
                table_from = int(temp_join[2])
                cost = cost + ( float(temp_table_size[table_to]) * float(trans_cost[temp_join]) )   ### calculates cost ###
                temp_table_size[table_from] = str(float(relation_reduce[temp_join]) * float(temp_table_size[table_from]))  ### updates
															   ### size ###
                                                                                                                      	   ### of table
	cost = cost + int(full_reduce_cost[str(table_from)])
	return cost

################################ parsing to the place where the table names are ##############################
i = 0
while(i <= (len(query)-5) ):
	temp_from = query[i:i+5]
	if( (temp_from == check_from1 or temp_from ==  check_from2) ):
		flag = 1
		break
	i = i + 1
if(flag == 0):
	print "no from"
	sys.exit(0)
i = i + 5


################################# reading the table names into tables[] ######################################
x = 0
flag = 1;
while(flag == 1 and i < len(query)):
	x = i
	while( x < len(query) and query[x] != ',' and query[x] != ' '):
		x = x + 1
	temp_array = query[i:x]
	x = x + 1
	if(x < len(query) and query[x] == ' '):
		x = x + 1
	if(temp_array == 'where' or temp_array == 'order' or temp_array == 'group'):
		break
	tables[table_number] = temp_array
	table_number = table_number + 1
	i = x
print "tables taking part in query = ",tables

n = 0;
for y in tables:
	if(len(y) != 0):
		n = n + 1
print "number of tables taking part in query = ",n

################################# initializing variables according to number of tables ##########################

table_matrix = [ [ [] for i in range(n) ] for j in range(n) ]        #### for storing the graph that shows how ####
bak_table_matrix = [ [ [] for i in range(n) ] for j in range(n) ]    #### relations are connected by attributes ####
for i in range(n):                                                  
        for j in range(n):
                table_matrix[i][j] = "0"
		bak_table_matrix[i][j] = "0"


schedule = [ [] for j in range(40 + n) ]  #### for storing the schedule that shows how (PopSize = 40 + NumberOfTables) ####
for i in range(40 + n):                                                   #### sequence of semi-joins ####
        schedule[i] = "0"

schedule_root = [ [] for j in range(40 + n) ]
for i in range(40 + n):                                                   #### root of each schedule ####
        schedule_root[i] = "0"



################################## reading data dictionary to find out how relations are connected ##################
with open('relation.json', 'r+') as f:
    relation = json.load(f)

################################## setting up the table_matrix for the connection graph #############################
i = 0
while( i < n):
	temp_array = relation[tables[i]]
	for x in temp_array:
		k = 0
		j = 0
		while( x[j] != ',' ):
			j = j + 1
		temp_rel = x[k:j]
		j = j + 1
		k = j
		while( j < len(x) ):
			j = j + 1
		temp_att = x[k:j]
		for l in range(n):
			if(tables[l] == temp_rel):
				table_matrix[i][l] = temp_att
				break
	i = i + 1
print "table matrix = ",table_matrix

for i in range(n):
        for j in range(n):
                bak_table_matrix[i][j] = table_matrix[i][j]


################################### starting the genetic algorithm phase ###########################################


### reading DD ###
with open('trans_cost.json', 'r+') as f:    ### contains network cost for transporting tuples ###
    trans_cost = json.load(f)
with open('relation_reduce.json', 'r+') as f:    ### contais the reduction fraction of tables when semi-joins is applied ###
    relation_reduce = json.load(f)
with open('full_reduce_cost.json', 'r+') as f:   ### contains cost to fully reduce table once root table is reduced ###
    full_reduce_cost = json.load(f)

table_size = ['400','200','100','50']            ### number of tuples in every relation (table_size[0] = emp) ###
temp_table_size = ['0','0','0','0']

temp_sched = ""

schedule_cost = [ [] for j in range(40 + n) ]  #### for storing the cost of each schedule generated ####
for i in range(40 + n):                                                   
        schedule_cost[i] = "999999.0"
############################ Random generator ######################

counter = 0
while(counter < 5):
	sched_num = 0
	for x in range(40 + n):
		for i in range(n):
			for j in range(n):
				table_matrix[i][j] = bak_table_matrix[i][j]
		root =  randint(0, (n-1))
		#root_table = tables[root]
		#print "\n########### schedule ",x,"################\n"
		#print "selected root table = ", root_table
		#schedule_root[sched_num] = root

		#### selecting last semijoin (taking into account the constraint "vc2") ####
		last_semijoin = 0
		count = 0
		for i in range(n):
			count = 0
			if(table_matrix[root][i] != "0"):
				for j in range(n):
					if(table_matrix[j][i] != "0"):
						count = count + 1
				if(count == 1):
					break
		#print table_matrix
		if(count == 1):
			last_semijoin = i
			#print "selected last semi-join table to root = ",tables[last_semijoin]
			for i in range(n):
				table_matrix[last_semijoin][i] = "0"
				table_matrix[i][last_semijoin] = "0"
		
		#### now, resolving other joins in the schedule ####
		edge_start = 1
		edge_end = 2*(n-1)
		temp_table_matrix = table_matrix
		create_schedule(root, -1, temp_table_matrix, edge_start, edge_end, 0) #### creates the schedule for semi-joins ####
		#print temp_table_matrix
				
		temp_sched = ""
		temp_sched = resolve_schedule(temp_table_matrix, temp_sched, root, last_semijoin) #### resolves the scedules ####
													   #### in proper format      ####
													   #### and puts them schedule[][] ###
		cost =  calc_cost(temp_sched, table_size)   	    ### calculates cost           ###
		if(cost < float(schedule_cost[x])):
			schedule_root[x] = root
			schedule[x] = temp_sched
			schedule_cost[x] = str(cost)
		#print schedule[sched_num]
	counter += 1
	for i in range( 40 + n ):               #### sorts schedules according to the cost, to pick up best solution for elitist strategy  ####
		for j in range( 40 + n ):
			if( float(schedule_cost[i]) < float(schedule_cost[j])):   
				temp = float(schedule_cost[i])
				schedule_cost[i] = schedule_cost[j]
				schedule_cost[j] = str(temp)
				temp_sched = schedule[i]
				schedule[i] = schedule[j]
				schedule[j] = temp_sched
	print "iteration ",counter,"\n"
	print "ALL Schedules\n", schedule
	print "\nCOST OF EACH SCHEDULE ", schedule_cost,"\n\n"



print "\n\n############################## LOOP OVER, FINAL SCHEDULES #################################"
print "\n",schedule
print "\n",schedule_cost



