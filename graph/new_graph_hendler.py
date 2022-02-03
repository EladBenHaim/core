from py2neo import Graph, Node, Relationship, NodeMatcher
import typing
from neo4j import connect_to_neo4j

# Create a graph variable
# param neo4j_name: dbms name (dev, prod, neo4j etc)
# param loc: where you want to connect (local, database, docker) 'C' - cloud (database), 'L' - local (neo4j desktop), 'D' - docker (neo4j docker)
# params user, password: pass user and password variables to connect to docker or localhost
# param secret_name: get user and password feom aws secret, default neo4j secret
# param ip_address: ip_address, default false
def connect_to_graph(neo4j_name,loc='C',user='',password='',secret_name='neo4j',ip_address=False):
    if loc.upper() == 'C':
        # dev grpah in cloud
        graph = connect_to_neo4j.get_graph(neo4j_name,secret_name,ip_address)
    elif loc.upper() == 'L':
        # local graph (change url if needed)
        graph = Graph("bolt://localhost:7687", auth=(user, password))
    elif loc.upper() == 'D':
        # connect to local from docker container
        graph = Graph("bolt://host.docker.internal:7687", auth=(user, password))
    return graph

graph = connect_to_graph("dev")


def add_new_node(node_type,id_dict, node_dict):
    '''
    Create a new node and update its meta data
    :param node_type: node type (label)
    :param id_dict: a dictionary of uniqe property name and id
    :param node_dict: a dictionary of all node's meta data
    usage example:
    add_new_node('Patient', {'patient_ID', 1234}, 
    {'patient_Name':'David', 'patient_Phone_Number':'000000'}
    '''
    create_node(node_type, id_dict)
    update_node(node_type, id_dict, node_dict)


def add_new_relation(from_node_type, from_condition, to_node_type, to_condition, rel_kind, properties_dict):
    '''
    Create a new relationship between two nodes and update properties on it
    :param from_node_type: start node type (label)
    :param from_condition: start node dictionary of uniqe property name and id
    :param to_node_type: end node type (label)
    :param to_condition: end node dictionary of uniqe property name and id
    :param rel_kind: a relationship name
    :param properties_dict: a dictionary of the relationships properties
    usage example:
    add_new_relation('Patient',{'patient_ID': 1234},
                    'Image', {'image_path': 'tmp/img.jpg'},
                    'Has_a', {'created_on': '2/7/2020','Source': 'Clalit'}
                )
    '''
    create_relation(from_node_type,from_condition, to_node_type, to_condition, rel_kind)
    update_relation(from_node_type,from_condition, to_node_type, to_condition, rel_kind,properties_dict)


def value_to_str(val_dict):
    '''
    Handle the different fields' types.
    '''
    field_value = val_dict[list(val_dict.keys())[0]]
    if type(field_value) is not str:
        return str(field_value)
    else:
        field_value = field_value.replace("'","\\'")
        return "'%s'" % (field_value)


def does_node_exist(node_type, id_dict):
    res = find_node(node_type, id_dict)
    if res==None:
        return False
    return True


def find_node(node_type, id_dict):
    '''
    This function finds a specific node from the graph.
    '''
    matcher = NodeMatcher(graph)
    id_key = list(id_dict.keys())[0]
    # Finds the node by it's ID
    node_to_return = matcher.match(node_type).where(
              "_." + id_key + " = " + value_to_str(id_dict)).first()  
    if node_to_return:
        return node_to_return
    else:
        return None


# '''
# This function finds a specific relation between 2 nodes.
# '''
# def find_relation(from_nide_type, from_id, to_node_type, to_id, rel_kind):
#     # u1_key =  list(from_id.keys())[0]
#     # u1 = Node(from_nide_type,u1_key=from_id[u1_key])
#     # u2_key =  list(to_id.keys())[0]
#     # u2 = Node(to_node_type, u2_key =to_id[u2_key])
#     # Relationship(u1, rel_kind, u2)
#     pass
  

'''
This function adds a node to the graph
usage example:
create_node('Patient', {'patient_ID':1234})
'''
def create_node(node_type, id_dict):
    try:
        finished_node = Node.cast([node_type, id_dict])
        check_duplications = find_node(node_type, id_dict)  # Prevents duplicate nodes (2 nodes with the same unique ID)
        if check_duplications:
            raise Exception(node_type+" did not created because Node with the same ID already exists!")
        
        graph.create(finished_node)
        node_added = find_node(node_type, id_dict)  # Finds the new node in the graph and presents it
        if node_added:
            str_to_return = "Node Added:\n" + str(node_added)
            return str_to_return
        else:
            raise Exception("Unable to add node")
    except Exception as e:
        print(e)
        return str(e)


'''
This function updates an existing node - Adds new fields and update the existing ones.
usage example:
update_node('Patient', {'patient_ID', 1234},
            {'Sex':'male', 'Age':26} )
'''
def update_node(node_type, id_dict, fields_to_add):
    try:
        # First finds the node to update
        node_to_update = find_node(node_type, id_dict) 
        if node_to_update:
            node_to_update.update(fields_to_add)
            graph.push(node_to_update)
        else:
            raise Exception("Node not found")
        node_updated = find_node(node_type, id_dict)  # Finds the updated node in the graph and presents it
        str_to_return = 'Node updated:\n' + str(node_updated)
        return str_to_return
    except Exception as e:
        return str(e)


'''
This function creates a relation between 2 nodes.
usage example:
create_relation('Patient', {'patient_ID', 1234},
                'Image', {'key':'path', 'value':'image_path'},
                'HAS_A')
'''
def create_relation(from_node, from_condition, to_node, to_condition, rel_kind, create_new_rel = False):
    try:
        from_node2 = find_node(from_node, from_condition)  # Finds the 'from' node for the relation
        if from_node2==None:
            raise Exception("from node not found")
        to_node2 = find_node(to_node, to_condition)  # Finds the 'to' node for the relation
        if to_node2==None:
            raise Exception("to node not found")
        rel = Relationship(from_node2, (rel_kind), to_node2)
        if create_new_rel:
            delete_relation(from_node, from_condition, to_node, to_condition, rel_kind)
        graph.merge(rel)  # 'merge' creates the relation only if it doesn't already exist.
        return "Relation created"
    except Exception as e:
        return str(e)


'''
This function deletes a node from the graph.
usage example:
delete_node ('Patient', {'key':'patient_ID', 'value':'1234'})
'''
def delete_node(node_type, id_dict):
    try:
        node_to_delete = find_node(node_type, id_dict)  # Finds the node to delete
        if node_to_delete:
            graph.delete(node_to_delete)
        else:
            raise Exception("Node not found")
    except Exception as e:
        return str(e)


'''
This function deletes a relation between 2 nodes.
usage example:
delete_relation('Patient',{'patient_ID': 1234},
                'Image', {'image_path': 'tmp/img.jpg'},
                'HAS_A')
'''
def delete_relation(from_node_type, from_condition, to_node_type, to_condition, rel_kind):
    try:
        query = "MATCH (a:" + from_node_type + ") -[rel:" + rel_kind + "]->(m:" + to_node_type + ") WHERE a." + list(from_condition.keys())[0] + " = " + value_to_str(from_condition) +" AND m." + list(to_condition.keys())[0] + " = " + value_to_str(to_condition) + " DELETE rel"  # Finds the relation to delete
        graph.run(query)
        return "Relation deleted"
    except Exception as e:
        return str(e)


'''
This function add/update property to a relation.
usage example:
update_relation('Patient',{'patient_ID': 1234},
                'Image', {'image_path': 'tmp/img.jpg'},
                'Has_a', {'created_on': '2/7/2020','Source': 'Clalit'}
               )
'''
def update_relation(from_node_type, from_condition, to_node_type, to_condition, rel_kind, properties_dict):
    try:
        from_node2 = find_node(from_node_type, from_condition)  # Finds the 'from' node for the relation
        if from_node2==None:
            raise Exception("from node not found")
        to_node2 = find_node(to_node_type, to_condition)  # Finds the 'to' node for the relation
        if to_node2==None:
            raise Exception("to node not found")
        rel = graph.match((from_node2, to_node2), rel_kind).first() # Gets relation
        for prop_name, prop_val in properties_dict.items():
            if isinstance(prop_val, list) and prop_name in rel and isinstance(rel[prop_name], list):
                rel[prop_name] = list(set(rel[prop_name]) | set(prop_val)) # Adds items to list
            else:
                rel[prop_name] =  prop_val
        graph.push(rel)           
        return "Relation updated"
    except Exception as e:
        print(str(e))
        return str(e)


'''
This function gets a specific node from the graph.
usage example:
get_node('Patient', {'key':'patient_ID', 'value':'1234'})
'''
def get_node(node_type, id_dict):
    try:
        # Finds the node by it's ID
        node_to_return = find_node(node_type, id_dict)  
        if node_to_return:
            return node_to_return
        else:
            raise Exception("Node not found")
    except Exception as e:
        return str(e)


'''
This function gets the nodes that meets the conditions specified in a list.
usage example:
get_nodes_with_conditions('Patient', ["Age >= 20", "Sex = 'male'"])
NOTE: The 'conditions_list' parameter should be a list even if there is only 1 condition.
'''
def get_nodes_with_conditions(node_type, conditions_list):
    try:
        # Starts with the first condition
        nodes_to_return_query = "MATCH (a:" + node_type + ") WHERE a." + conditions_list.pop(0) 
        
         # Adds the other conditions
        for condition_str in conditions_list: 
            nodes_to_return_query += " AND a." + condition_str
        nodes_to_return_query = nodes_to_return_query +" RETURN a"
        nodes_to_return = graph.run(nodes_to_return_query)
        if nodes_to_return:
            return nodes_to_return
        else:
            raise Exception("Nodes not found")
    except Exception as e:
        return str(e)


def get_gene_protein_name(gene):
        query = "MATCH p=(g:Gene)-[r:TRANSLATES_TO]->(c:Protein) WHERE g.Symbol='" + gene + "' RETURN c.Name"
        res = graph.run(query)
        if not str(res)=="(No data)":
            return str(res).replace("\r","").replace(" ","").split("\n")[-2]
        else:
            return None


def get_protein_by_variant(accession):
    query = "MATCH p=(c:Protein)-[r:Has_Variant]->(v:Variant) WHERE v.Accession='" + accession + "' RETURN c.Name"
    res = graph.run(query)
    if not str(res)=="(No data)":
        return str(res).replace("\r","").replace(" ","").split("\n")[-2]
    else:
        return None


'''
This function fatches all the nodes labels
usage example:
get_all_node_labels('Patient', {'patient_ID':1234})
'''
def get_all_node_labels(node_label, id_dict):
    id_key = list(id_dict.keys())[0]
    query = (
        "MATCH (n:" + node_label + " {" + id_key + ":" + value_to_str(id_dict) + "}) "
        "RETURN labels(n) as labels"
    )
    data = get_data_from_graph(query)
    return data[0]['labels'] if data else []


'''
This function changes property name 
The function gets the node's label and the old and new property name
usage example:
rename_prop('Patient', 'city', 'location')
'''
def rename_prop(node_label, old_prop_name, new_prop_name):
    query = (
        "MATCH (n:" + node_label + ") "
        "WITH collect(n) AS nodes "
        "CALL apoc.refactor.rename.nodeProperty('" + old_prop_name + "', '" + new_prop_name + "', nodes) "
        "YIELD committedOperations "
        "RETURN committedOperations "
    )
    get_data_from_graph(query)


'''
This function changes properties name
The function gets list of dict with keys:'label','old_prop','new_prop'
'''
def rename_props(props_label_list):
    q = ("unwind $items as item "
    "match (n) where labels(n) = item.label "
    "WITH collect(n) AS nodes, item "
    "CALL apoc.refactor.rename.nodeProperty(item.old_prop, item.new_prop, nodes) "
    "YIELD committedOperations "
    "RETURN committedOperations")
    get_data_from_graph(q,items=props_label_list)


'''
This function changes label name 
The function gets the node's old and new label 
usage example:
rename_label('Person', 'Patient')
'''
def rename_label(node_label, new_node_label):
    query = (
        "MATCH (n:" + node_label + ") "
        "WITH collect(n) AS nodes "
        "CALL apoc.refactor.rename.label('" + node_label + "', '" + new_node_label + "', nodes) "
        "YIELD committedOperations "
        "RETURN committedOperations "
    )
    get_data_from_graph(query)


'''
This function changes relationship name 
The function gets the old relationships name and new name of this relationship
NOTE: this function get the quotes like `` '' ""   
usage example:
rename_relationship('PART_OF', 'EVENT_IN')
'''   
def rename_relationship(old_name_relationship, new_name_relationship,quote="'"):
    query = (
        "MATCH ()-[r:"+quote+"+"+old_name_relationship+"+"+quote+"]-() "
        "WITH collect(r) AS rels "
        "CALL apoc.refactor.rename.type('"+old_name_relationship+"','"+new_name_relationship+"', rels) "
        "YIELD committedOperations "   
        "RETURN committedOperations "
    ) 
    return get_data_from_graph(query)


'''
This function removes label/s from nodes
The function gets the node's main label and list of the to delete labels 
usage example:
remove_label('Person', ['Patient'])
'''
def remove_label(node_label, labels):
    query = (
        "MATCH (n:" + node_label + ") "
        'REMOVE n:' + ':'.join(labels)
    )
    get_data_from_graph(query)


'''
This function add label/s from nodes
The function gets the node's main label and list of the to add labels 
usage example:
rename_prop('Person', ['Patient'])
'''
def add_label(node_label, labels):
    query = (
        "MATCH (n:" + node_label + ") "
        'SET n:' + ':'.join(labels)
    )
    get_data_from_graph(query)


'''
This function merges two nodes onto the first one.
All relationships are merged onto that node too.
NOTE: If props variable is not defined : 
The first nodes' property will remain if already set, 
otherwise the last nodes' property will be written.
You can define another behavior:
for example: {name:'discard', age:'overwrite', kids:'combine', `addr.*`, 'overwrite',`.*`: 'discard'}
https://neo4j-contrib.github.io/neo4j-apoc-procedures/3.5/graph-refactoring/merge-nodes/
NOTE: If labels variable (list) is defined the function will remove all the 
labels in the list from the node definition.
usage example: 
merge_nodes('Anatomy', 'UBERON:0000006', 'EFO', 'UBERON:0000006')
'''


def merge_nodes(first_node, first_node_id, last_node, last_node_id, first_node_id_name = None, last_node_id_name = None, props = "'discard'", label_to_remove = None):
    if not first_node_id_name:
        first_node_id_name = first_node + "_Id"
    if not last_node_id_name:
        last_node_id_name = last_node + "_Id"
    remove_label = 'REMOVE node:' + label_to_remove if label_to_remove else ''
    query = (
                "MATCH (f:" + first_node + " {" + first_node_id_name + ":'" + first_node_id + "'}) , "
                "(l:" + last_node + " {" + last_node_id_name + ":'" + last_node_id + "'}) "
                "WHERE ID(f) <> ID(l) "
                "AND size(apoc.coll.intersection(labels(f),labels(l)))=0 "
                "WITH head(collect([f,l])) as nodes "
                "CALL apoc.refactor.mergeNodes(nodes,{properties: " + props + ", mergeRels:true}) "
                "yield node "
                + remove_label +
                " RETURN node"
            )
    get_data_from_graph(query)


'''
This function done the same action as merge_nodes 
but for multiple nodes couples
NOTE: first id and last id must be string!
usage example:
merge_multiple_nodes([{first:'A',first_prop:'id',first_id:123, last:'B', last_prop:'id', last_id:456}])
'''
def merge_multiple_nodes(merging_list, props = "'discard'", batch_size = 10000):
    query = (
        'UNWIND $items as i '
        'CALL apoc.cypher.run("MATCH (f:" + i.first + "{" + i.first_prop + ": \'" + i.first_id + "\'}), '
        '(l:" + i.last + "{" + i.last_prop + ": \'" + i.last_id + "\'}) '
        'WHERE ID(f) <> ID(l) '
        'AND size(apoc.coll.intersection(labels(f),labels(l)))=0 '
        'WITH head(collect([f,l])) as nodes '
        'RETURN nodes ", {}) '
        'YIELD value '
        'CALL apoc.refactor.mergeNodes(value.nodes,{properties: ' + props + ', mergeRels:true}) '
        'yield node '
        'RETURN count(node) as count'
    )
    rel_count = 0
    final_list = [merging_list[i * batch_size:(i + 1) * batch_size] for i in range((len(merging_list) + batch_size - 1) // batch_size )] 
    for list in final_list:
        count = graph.run(query, items = list).data()
        rel_count = rel_count + int(count[0]['count'])
    print(rel_count)
    return rel_count


'''
This function gets query and returns data results 
optional: the function can get parameter/s
NOTE: in query variable name must by '$items'
usage example:
get_data_from_graph("MATCH (p:Patient) WHERE p.ID = $items.ID RETURN p.name", {'ID':123})
'''
def get_data_from_graph(query, prams = []):
    res = graph.run(query, items = prams).data()
    if res == []:
        return None
    return res


'''
This function gets query and returns data results 
optional: the function can get parameter/s
NOTE: in query variable name must by '$items'
usage example:
get_data_frame_from_graph("MATCH (p:Patient) WHERE p.ID = $items.ID RETURN p.name", {'ID':123})
'''
def get_data_frame_from_graph(query, prams = []):
    res = graph.run(query, items = prams).to_data_frame()
    return res


'''
This function creates index to improve searce performence
usage example:
create_index('Patient', 'patient_ID')
'''
def create_index(node_label, prop):
    index_name = node_label + '_' + prop + '_index' 
    query = "CREATE INDEX " + index_name + " IF NOT EXISTS FOR (n:" + node_label + ") ON ( n." + prop + ")"
    graph.run(query)


'''
This function drop index
usage example:
drop_index('Patient', 'patient_ID')
'''
def drop_index(node_label, prop):
    index_name = node_label + '_' + prop + '_index' 
    query = "DROP INDEX " + index_name + " IF EXISTS"
    graph.run(query)


'''
This function deletes index
usage example:
delete_index('Patient', 'patient_ID')
'''
def delete_index(node_label, prop):
    index_name = node_label + '_' + prop + '_index' 
    query = "DROP INDEX " + index_name + " IF EXISTS"
    graph.run(query)


'''
This function creates constraint to make property uniqe
usage example:
create_index('Patient_constraint', 'Patient', 'patient_ID')
'''
def create_unique_constraint(constraint_name, node_type, unique_id):
    query = (
        "CREATE CONSTRAINT " + constraint_name + " IF NOT EXISTS ON (n: " + node_type + ") "
        "ASSERT n." + unique_id + " IS UNIQUE"
    )
    graph.run(query)


'''
This function adds at once up to 10000 node to the graph
This function prevents duplicate nodes (2 nodes with the same unique ID) by using MERGE
NOTE: in case of exist node the function will overwrite the last properties
usage example:
NOTE: in case you not want to add constraint send to the function 0 value the default is create constraint (flag = 1)
create_multiple_nodes([{'patient_ID':1234}, {'patient_ID':1235}], 'Patient', 'patient_ID', ':Person', 0)
'''
def create_multiple_nodes(nodes_list, node_type, unique_id, additional_label = '', batch_size = 10000,flag = 1): 
    # Add uniqe constraint:
    if flag == 1:
        constraint_name = node_type + '_constraint'
        create_unique_constraint(constraint_name, node_type, unique_id)
    if isinstance(additional_label,typing.List):
        additional_label = ':' + ':'.join(additional_label)
    # Create nodes:
    query = (
            "UNWIND $nodes AS node "
            "MERGE (n: " + node_type + additional_label + " {" + unique_id + ":" + "node." + unique_id + "}) "
            "SET n = node" 
            )
    final_list = [nodes_list[i * batch_size:(i + 1) * batch_size] for i in range((len(nodes_list) + batch_size - 1) // batch_size )] 
    for list in final_list:
        graph.run(query, nodes = list)
    # Return nodes count 
    query = 'MATCH (n: ' + node_type + ') RETURN count(n) as count'
    data = get_data_from_graph(query)
    nodes_count = 0 if not data else int(data[0]['count'])
    return nodes_count


'''
This function has 2 stages:
1. updates props on existing relatonships
2. create new relationships
NOTE: The function create_multiple_relatioships will update props only when "on create"
while update_multiple_relatioships function will update only when "on match"
'''
def add_multiple_relatioships(from_node_type, from_node_id, to_node_type, to_node_id, relatioships_list, batch_size = 10000):
    # Update existing relationships properties
    rel_count = update_multiple_relatioships(from_node_type, from_node_id, to_node_type, to_node_id, relatioships_list, batch_size)
    print(str(rel_count) + ' relatioships updated.')
    # Create new relationships
    rel_count = create_multiple_relatioships(from_node_type, from_node_id, to_node_type, to_node_id, relatioships_list, batch_size)
    print(str(rel_count) + ' relatioships created.')


'''
This function updates props on exist relatonships
The function gets from node type and id, to node type and id and list of relationships details
NOTE: The function create_multiple_relatioships will update props only when "on create"
while update_multiple_relatioships function will update only when "on match"
usage example:
update_multiple_relatioships('Doctor', 'Doctor_ID, 'Patient', 'patient_ID',
     [{'from':123,'rel_type':'Treat','to':125, props: {since:1996}}, {'from':123,'rel_type':'Treat','to':126, props: {since:2009}}])
'''
def update_multiple_relatioships(from_node_type, from_node_id, to_node_type, to_node_id, relatioships_list, batch_size = 10000): 
    # Update relatioships:
    query = (
            "UNWIND $relatioships AS relationship "
            "MATCH (f:" + from_node_type + " {" + from_node_id + ": relationship.from})"
            "-[rels]->"
            "(t:" + to_node_type + " {" + to_node_id + ": relationship.to}) "
            "WHERE type(rels) = toUpper(relationship.rel_type) "
            "UNWIND rels AS r "
            "WITH r, relationship.props as map "
            "WITH r, map, "
            "[k in keys(map) WHERE k in keys(r) "
            "AND apoc.meta.cypher.type(r[k]) STARTS WITH 'LIST OF' "
            "AND apoc.meta.cypher.type(map[k]) STARTS WITH 'LIST OF'| k] AS list_prop "
            "WITH r, map, "
            "[k in keys(map) | "
            "CASE WHEN k in list_prop THEN apoc.coll.toSet(apoc.coll.flatten([r[k],map[k]])) "
            "ELSE map[k] end] as values "
            "CALL apoc.create.setRelProperties(r, keys(map), values) YIELD rel "
            "RETURN size(collect(rel)) as count "
            )
    rel_count = 0
    final_list = [relatioships_list[i * batch_size:(i + 1) * batch_size] for i in range((len(relatioships_list) + batch_size - 1) // batch_size )] 
    for list in final_list:
        count = graph.run(query, relatioships = list).data()
        rel_count = rel_count + int(count[0]['count'])
    return rel_count


'''
This function adds at once up to 10000 relationships to the graph
The function gets from node type and id, to node type and id and list of relationships details
NOTE: The function create_multiple_relatioships will update props only when "on create"
while update_multiple_relatioships function will update only when "on match"
usage example:
create_multiple_relatioships('Doctor', 'Doctor_ID, 'Patient', 'patient_ID',
     [{'from':123,'rel_type':'Treat','to':125}, {'from':123,'rel_type':'Treat','to':126, props: {'score':33}}])
'''


def create_multiple_relatioships(from_node_type, from_node_id, to_node_type, to_node_id, relatioships_list, batch_size = 10000): 
    # Create relatioships:
    query = (
            "UNWIND $relatioships AS relationship "
            "MATCH (f:" + from_node_type + " {" + from_node_id + ": relationship.from}) "
            "MATCH (t:" + to_node_type + " {" + to_node_id + ": relationship.to}) "
            "CALL apoc.merge.relationship(f, toUpper(relationship.rel_type), null, relationship.props, t, null) YIELD rel "
            "RETURN size(collect(rel)) as count "
            )
    rel_count = 0
    final_list = [relatioships_list[i * batch_size:(i + 1) * batch_size] for i in range((len(relatioships_list) + batch_size - 1) // batch_size )] 
    for list in final_list:
        count = graph.run(query, relatioships = list).data()
        rel_count = rel_count + int(count[0]['count'])
    return rel_count


'''
This function gets query and list of dict and commit run with lists of n size 
NOTE: in query variable name must by 'items'
usage example:
run_by_batches("UNWIND $items AS item MERGE (p:Patient {patient_ID: item.ID})", [{'ID':1235},{'ID':1236}])
'''
def run_query_by_batches(query, dict_list, batch_size = 100): 
    final_list = [dict_list[i * batch_size:(i + 1) * batch_size] for i in range((len(dict_list) + batch_size - 1) // batch_size )] 
    for list in final_list:
        graph.run(query, items = list)

'''
This function gets label, property and value type and convert the property value from the type to list 
usage example:
fix_mixed_properties_to_list("DomainAndFamily","xref","String")
'''
def fix_mixed_properties_to_list(label,prop,prop_type):
    graph.run("match (n:"+label+") where apoc.meta.isType(n."+prop+",'"+prop_type+"') set n."+prop+" = apoc.convert.toList(n."+prop+")")


if __name__ == ('__main__'):
    create_node("A",{"name": "aaa"})
