import datetime
import logging 
import os

# Set new log file 
def set_log_file(file_name,loc='data/nuete/chk.py'):
    now = datetime.datetime.now()
    date = now.strftime("%x/%X").replace('/','_')
    file_name = "./data/"+loc.split('/')[-2]+"/logs/" + file_name + "_" + date + ".log"
    logger = logging.getLogger()
    logger.handlers = []
    fhandler = logging.FileHandler(filename=file_name, mode='a')
    formatter = logging.Formatter('%(asctime)s - %(message)s')
    fhandler.setFormatter(formatter)
    logger.addHandler(fhandler)
    logger.setLevel(logging.INFO)
    return logger

# Write nodes to log file
# param logger: the log file variable
# param lbl: which node label you want to write the log on him
# params graphDB: query on the graph
# param note: if there is special notes toy want to add to log, default no note
def write_nodes_to_log(logger,lbl,graphDB,note=''):
    nodes_count = graphDB.get_data_from_graph("match (n:"+lbl+") return count(n) as num")[0]["num"]
    if note == '':
        logger.info(lbl+': ' + str(nodes_count) + " nodes created")
    else:
        logger.info(lbl+': ' + str(nodes_count) + " nodes created, "+note)
    print(lbl + ': ' + str(nodes_count) + " nodes created")
    return nodes_count

# Write relations to log file
# param self: the log file variable
# param f_lbl: which from node label you want to write the log on him
# param t_lbl: which to node label you want to write the log on him
# params graphDB: query on the graph
# param note: if there is special notes toy want to add to log, default no note
def write_relations_to_log(logger,f_lbl,t_lbl,graphDB,note=''):
    rel_count = graphDB.get_data_from_graph("match (n:"+f_lbl+")-[r]-(:"+t_lbl+") return count(r) as num")[0]["num"]
    if note == '':
        logger.info(f_lbl+': ' + str(rel_count) + " "+f_lbl+" -> "+t_lbl+" relations created")
    else:
        logger.info(f_lbl+': ' + str(rel_count) + " "+f_lbl+" -> "+t_lbl+" relations created, "+note)
    print(f_lbl+': ' + str(rel_count) + " "+f_lbl+" -> "+t_lbl+" relations created")
    return rel_count

# Write relations to log file
# param self: the log file variable
# param f_lbl: which first node label you want to write the log on him
# param f_acc: which first node prop value has merge
# param f_prop: which first node prop has merge
# param l_lbl: which last node label you want to write the log on him
# param l_acc: which last node prop value has merge
# param l_prop: which last node prop has merge
def write_merge_nodes_to_log(logger,f_lbl,f_acc,f_prop,l_lbl,l_prop,l_acc):
    logger.info(f_lbl + ' '+f_prop+': ' + f_acc + ' merged with: ' + l_lbl + ' '+l_prop+': ' + l_acc)
