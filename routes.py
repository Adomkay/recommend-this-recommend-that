import pandas as pd
import numpy as np
import matplotlib as plt
from py2neo import Graph
from authentication import *

graph = Graph('meetup_db/installation-3.4.6/import', username = 'neo4j', password = 'Evangeline123')

# Create relationship between events and group
def events_group_rel():
    statement = """USING PERIODIC commit
    LOAD CSV WITH HEADERS FROM 'file:///groups.csv' as group
    MATCH (e: Events), (g: Groups)
    WHERE e.event_id = event.event_id and g.group_id = event.group_id
    MERGE (e)-[r:CREATED_BY]->(g)"""
    results = graph.run(statement).to_table()
    display(results)
    return results

#Get Member info
def get_member_info(member_id):
    statement = '''Match (m:Members)-[:HAS_GROUP]->(g:Groups),
    (m:Members)-[:LIKES_TOPIC]-(t:Topics)
    WHERE m.member_id = {A}
    RETURN m.member_id AS Member_id, g.name AS Group_name'''
    results = graph.run(statement, {"A":member_id}).to_table()
    display(results)
    return results

#Get Group info
def get_group_info(group_id):
    statement = '''MATCH (g:Groups)-[:HAS_CATEGORY]->(cat:Categories)
    WHERE g.group_id = {A}
    RETURN g.name AS Group_name, cat.name AS Category'''
    results = graph.run(statement, {"A": group_id}).to_table()
    display(results)
    return results

#Get the top 20 Topics liked by members
def top_10_topics_by_members():
    statement = '''MATCH (t:Topics)<-[r:LIKES_TOPIC]-(m:Members)
    RETURN t.topic_id, t.name, count(r) as  num_of_members
    ORDER BY num_of_members DESC LIMIT 10'''
    results = graph.run(statement).to_table()
    display(results)
    return results

#Get the top 20 Topics liked by groups
def top_10_topics_by_groups():
    statement = '''MATCH (t:Topics)<-[r:HAS_TOPIC]-(g:Groups)
    RETURN t.topic_id, t.name, count(r) as  num_of_groups
    ORDER BY num_of_groups DESC LIMIT 10'''
    results = graph.run(statement).to_table()
    display(results)
    return results

#Get top 20 Categories
def top_10_categories():
    statement = '''
    MATCH (cat:Categories)<-[r:HAS_CATEGORY]-(g:Groups)
    RETURN cat.category_id, cat.name, count(r) as  num_of_groups
    ORDER BY num_of_groups DESC LIMIT 10
    '''
    results = graph.run(statement).to_table()
    display(results)
    return results

#Recommend new meetup groups to members
def member_group_recommendation(member_id):
    statement = '''MATCH (g1:Groups)<-[:HAS_GROUP]-(m:Members)-[:HAS_GROUP]->(g2:Groups)
    MATCH (g2)<-[:HAS_GROUP]-(m2:Members)-[r:HAS_GROUP]->(g3:Groups)
    WHERE m.member_id = {A}
    AND NOT (m)-[:HAS_GROUP]->(g3)
    RETURN g3.group_id, g3.name, count(r) as  num_of_members
    ORDER BY num_of_members DESC LIMIT 10'''
    results = graph.run(statement, {"A": str(member_id)}).to_table()
    display(results)
    return results

#Path taken to arrive at group recommendations
def member_group_recommendation_path(member_id):
    statement = '''MATCH (g1:Groups)<-[:HAS_GROUP]-(m:Members)-[:HAS_GROUP]->(g2:Groups),
    (g2)<-[:HAS_GROUP]-(m2:Members)-[r:HAS_GROUP]->(g3:Groups)
    WHERE m.member_id = {A} and g3.group_id = {B}
    AND NOT (m)-[:HAS_GROUP]->(g3)
    RETURN m.member_id, g2.name, m2.member_id, g3.name'''
    results = graph.run(statement, {"A": str(member_id)}, {"B": str(chosen_group_id)}).to_table()
    display(results)
    return results

#Group Collaboration Recommendations
def group_collaboration(group_id):
    statement = """MATCH (g1:Groups)-[r1:HAS_CATEGORY]->(c1:Categories)<-[r2:HAS_CATEGORY]-(g2:Groups)
    MATCH (m:Members)-[r3:HAS_GROUP]->(g2:Groups)
    WHERE g1.group_id = {A}
    RETURN g2.group_id, g2.name, count(r3) as  num_of_members
    ORDER BY num_of_members DESC LIMIT 100"""
    results = graph.run(statement, {"A": str(group_id)}).to_table()
    display(results)
    return results

#Get member IDs
def get_member_ids():
    statement = '''MATCH (m:Members)
    RETURN m.member_id as ID'''
    results = graph.run(statement).data()
    df = pd.DataFrame(results)
    return df

df = get_member_ids()

#Get random_member_id
def random_member_id():
    random_member_id = df['ID'].sample(1).iloc[0]
    return random_member_id

#Get group get_ids
def get_group_ids():
    statement = '''MATCH (g:Groups)
    RETURN g.group_id as ID'''
    results = graph.run(statement).data()
    df1 = pd.DataFrame(results)
    return df1

df1 = get_group_ids()

#Get random_member_id
def random_group_id():
    random_group_id = df1['ID'].sample(1).iloc[0]
    return random_group_id

def get_categories():
    statement2 = '''MATCH MATCH (g: Groups)-[r:HAS_CATEGORY]->(cat:Categories)                                                         \
    RETURN cat.name AS category, count(r) AS groups
    ORDER BY groups DESC
    LIMIT 10'''
    results = graph.run(statement2).data()
    df5 = pd.DataFrame(results)
    return df5
