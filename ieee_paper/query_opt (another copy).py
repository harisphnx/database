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

############################ initialization phase ######################

print "\n########################## INITIALIZATION PHASE ##########################\n"

sched_num = 0
for x in range(40 + n):
	for i in range(n):
        	for j in range(n):
	                table_matrix[i][j] = bak_table_matrix[i][j]
	root =  randint(0, (n-1))
	root_table = tables[root]
	print "\n########### schedule ",x,"################\n"
	print "selected root table = ", root_table
	schedule_root[sched_num] = root

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
	print table_matrix
	if(count == 1):
		last_semijoin = i
		print "selected last semi-join table to root = ",tables[last_semijoin]
		for i in range(n):
			table_matrix[last_semijoin][i] = "0"
			table_matrix[i][last_semijoin] = "0"
	
	#### now, resolving other joins in the schedule ####
	edge_start = 1
	edge_end = 2*(n-1)
	temp_table_matrix = table_matrix
	create_schedule(root, -1, temp_table_matrix, edge_start, edge_end, 0) #### creates the schedule for semi-joins ####
	print temp_table_matrix
			
	schedule[sched_num] = resolve_schedule(temp_table_matrix, temp_sched, root, last_semijoin) #### resolves the scedules ####
												   #### in proper format      ####
										                   #### and puts them schedule[][] ###
	print schedule[sched_num]
	sched_num = sched_num + 1
print "ALL SCHEDULES GENERATED IN THE INITIALIZATION PHASE\n", schedule

########################## selection phase #############################

print "\n############################### SELECTION PHASE ##########################\n"

#### calculating cost of each schedule #### 

schedule_cost = [ [] for j in range(40 + n) ]  #### for storing the cost of each schedule generated ####
for i in range(40 + n):                                                   
        schedule_cost[i] = "0"

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

for x in range(40 + n):
        temp_sched = schedule[x]                    ### takes schedule one by one ###
	cost =  calc_cost(temp_sched, table_size)   ### calculates cost           ###
	schedule_cost[x] = str(cost)
print "\nCOST OF EACH SCHEDULE ", schedule_cost

for i in range( 40 + n ):                #### sorts schedules according to the cost, to pick up best solution for elitist strategy  ####
	for j in range( 40 + n ):
		if( float(schedule_cost[i]) < float(schedule_cost[j])):   
			temp = float(schedule_cost[i])
			schedule_cost[i] = schedule_cost[j]
			schedule_cost[j] = str(temp)
			temp_sched = schedule[i]
			schedule[i] = schedule[j]
			schedule[j] = temp_sched

print "After sorting the best schedules\n"
print schedule
print "\n"
print schedule_cost

### taking NumberOfBestSolutions (= PopSize * 0.125) for next generation (following the elitist strategy) ###
### NumberOfBestSolutions = 43 * 0.125 => 5 (approx) ###

NumberOfBestSolutions = int( (40 + n) * 0.125 )
best_schedule = [ [] for i in range(NumberOfBestSolutions) ]
best_schedule_cost = [ [] for j in range(NumberOfBestSolutions) ]

for i in range(NumberOfBestSolutions):
	best_schedule[i] = schedule[i]
	best_schedule_cost[i] = schedule_cost[i]

print "\nbest solutions\n"
print best_schedule
print "\n"
print best_schedule_cost

############################################## REPRODUCTION PHASE ######################################################

######## not sure how many generations are to be produced, taking 5 ########
print "\n\n############################ AFTER REPRODUCTION ##############################"

temp2_sched = ""
temp2_join = ""
flag = 0
for i in range(5):
	for x in range(40 + n):
		temp_sched = schedule[x]
		flag = 0
		y = x + 1
		while(y < (40 + n)):
			j = 3
			k = 3
			while( j < len(temp_sched) ):
				temp2_sched = schedule[y]
				while( k < len(temp_sched) and temp_sched[k] != ":" and temp_sched[k] != "." ):### extracts joins from sched ##
		                        k = k + 1
	        	        temp_join = temp_sched[j:k]
				temp2_join = temp2_sched[j:k]
				k = k + 2
				j = k
				if(temp_join == temp2_join and len(temp_join) != 0 and len(temp2_join) != 0):
					#### finding compatible schedule and exchanging sub-tress ####
					flag = 1
				#	while(temp_sched[k] != ":" and temp_sched[k] != "." and k < len(temp_sched) ):
				#		k = k + 1
					char1 = temp_sched[j:len(temp_sched)]
                                	char2 = temp2_sched[j:len(temp2_sched)]
					temp_sched = temp_sched[0:j] + char2
					temp2_sched =  temp2_sched[0:j] + char1
					j = len(temp_sched)
				#### checkin if the newly formed schedule is better than its parent schedule and replacing accordingly ####
					cost1 = calc_cost(temp_sched, table_size)
					if(cost1 < float(schedule_cost[x])):
						count = 0
						for b in range(n):
						        if(str(b) in temp_sched):
						                count = count + 1
						if(count == n):
							schedule[x] = temp_sched
							schedule_cost[x] = str(cost1)
					cost2 = calc_cost(temp2_sched, table_size)
					if(cost2 < float(schedule_cost[y])):
						count = 0
                                                for b in range(n):
	                                                if(str(b) in temp2_sched):
        	                                                count = count + 1
                                                if(count == n):
							schedule[y] = temp2_sched
	                                        	schedule_cost[y] = str(cost2)
			if(flag == 1):
				break
			y = y + 1


for i in range( 40 + n ):                #### sorts schedules according to the cost  ####
	for j in range( 40 + n ):
		if( float(schedule_cost[i]) < float(schedule_cost[j])):
        		temp = float(schedule_cost[i])
	                schedule_cost[i] = schedule_cost[j]
        	        schedule_cost[j] = str(temp)
        		temp_sched = schedule[i]
	                schedule[i] = schedule[j]
        	        schedule[j] = temp_sched

print "\n\n",schedule
print "\n\n",schedule_cost

############################################ MUTATION PHASE ################################################

xk = np.arange(5)                                     #### number of random numbers (mutation types) ####

for x in range(40 + n):
	temp_sched = schedule[x]
        k = 0
	count = 0
        while( k < len(temp_sched) ):
                while( k < len(temp_sched) and temp_sched[k] != ":" and temp_sched[k] != "." ):  ### extracts joins from sched ###
        	        k = k + 1
		count = count + 1								 ### choice checks number of children ###
		k += 1
	d = 0.02
	e = 1/(10*count)
	a = (1 - (e + d))/3
	b = a
	c = a
	pk = (a, b, c, d, e)                                      #### creating custom probability distribution ####
	custm = stats.rv_discrete(name='custm', values=(xk, pk))
	h = plt.plot(xk, custm.pmf(xk))
	choice = custm.rvs(size=1)
	temp_sched = ""
	if(choice == 1):
		### single edge --> double edge ###
		'''j = 0
		k = 0
	        while( j < len(temp_sched) ):
                	while(temp_sched[k] != ":" and temp_sched[k] != "." and k < len(temp_sched) ):### extracts joins from sched ###
	                        k = k + 1
			temp_join = temp_sched[j:k]
			temp_join = temp_join[::-1]
			if(temp_join not in temp_sched):'''
		#### selecting last semijoin (taking into account the constraint "vc2") ####
		for i in range(n):
	                for j in range(n):
	                        table_matrix[i][j] = bak_table_matrix[i][j]
		last_semijoin = 0
		count = 0
		for i in range(n):
			count = 0
			if(table_matrix[schedule_root[x]][i] != "0"):
				for j in range(n):
					if(table_matrix[j][i] != "0"):
						count = count + 1
				if(count == 1):
					break
		if(count == 1):
			last_semijoin = i
			for i in range(n):
				table_matrix[last_semijoin][i] = "0"
				table_matrix[i][last_semijoin] = "0"
		edge_start = 1
	        edge_end = 2*(n-1)
	        temp_table_matrix = table_matrix
	        create_schedule(schedule_root[x], -1, temp_table_matrix, edge_start, edge_end, 2) #### creates the schedule for semi-joins ### 
												  #### "2" in the last parameter says  ####
												  #### to create double edged joins   ####
	        temp2_sched = resolve_schedule(temp_table_matrix, temp_sched, schedule_root[x], last_semijoin)
		cost =  calc_cost(temp2_sched, table_size)
		if(cost < float(schedule_cost[x])):     #### checing if newly found schedule has lower cost then existing schedule ####
			schedule[x] = temp2_sched
                        schedule_cost[x] = cost
	elif(choice == 2):
		### double edge --> single edge ###
		for i in range(n):
	                for j in range(n):
	                        table_matrix[i][j] = bak_table_matrix[i][j]
		last_semijoin = 0
		count = 0
		for i in range(n):
			count = 0
			if(table_matrix[schedule_root[x]][i] != "0"):
				for j in range(n):
					if(table_matrix[j][i] != "0"):
						count = count + 1
				if(count == 1):
					break
		if(count == 1):
			last_semijoin = i
			for i in range(n):
				table_matrix[last_semijoin][i] = "0"
				table_matrix[i][last_semijoin] = "0"
		edge_start = 1
                edge_end = 2*(n-1)
                temp_table_matrix = table_matrix
                create_schedule(schedule_root[x], -1, temp_table_matrix, edge_start, edge_end, 1) #### creates the schedule for semi-joins ### 
                                                                                                  #### "1" in the last parameter says  ####
                                                                                                  #### to create single edged joins   ####
                temp2_sched = resolve_schedule(temp_table_matrix, temp_sched, schedule_root[x], last_semijoin)
                cost =  calc_cost(temp2_sched, table_size)
                if(cost < float(schedule_cost[x])):	#### checing if newly found schedule has lower cost then existing schedule ####
                        schedule[x] = temp2_sched
                        schedule_cost[x] = cost

	elif(choice == 3):
		### change order of double edges ###
		print ""
	elif(choice == 4):
		### set table as new root        ###
		for i in range(n):
	                for j in range(n):
	                        table_matrix[i][j] = bak_table_matrix[i][j]
		edge_start = 1
                edge_end = 2*(n-1)
                temp_table_matrix = table_matrix
		for b in range(n):                               #### selecting new root, and resolving schedule as the normal root ####
			if(b != schedule_root[x]):
				break
		last_semijoin = 0
		count = 0
		for i in range(n):
			count = 0
			if(table_matrix[b][i] != "0"):
				for j in range(n):
					if(table_matrix[j][i] != "0"):
						count = count + 1
				if(count == 1):
					break
		if(count == 1):
			last_semijoin = i
			for i in range(n):
				table_matrix[last_semijoin][i] = "0"
				table_matrix[i][last_semijoin] = "0"
                create_schedule(i, -1, temp_table_matrix, edge_start, edge_end, 0) #### creates the schedule for semi-joins ####
                temp2_sched = resolve_schedule(temp_table_matrix, temp_sched, i, last_semijoin)
                cost =  calc_cost(temp2_sched, table_size)
                if(cost < float(schedule_cost[x])):	#### checing if newly found schedule has lower cost then existing schedule ####
			print "change", choice
			schedule_root[x] = i
                        schedule[x] = temp2_sched
                        schedule_cost[x] = cost

	elif(choice == 5):
		print ""
		### change last edge             ###


for i in range( 40 + n ):                #### sorts schedules according to the cost  ####
	for j in range( 40 + n ):
		if( float(schedule_cost[i]) < float(schedule_cost[j])):
        		temp = float(schedule_cost[i])
	                schedule_cost[i] = schedule_cost[j]
        	        schedule_cost[j] = str(temp)
        		temp_sched = schedule[i]
	                schedule[i] = schedule[j]
        	        schedule[j] = temp_sched


print "############################## AFTER MUTATION ##############################"

print "\n",schedule
print "\n",schedule_cost

