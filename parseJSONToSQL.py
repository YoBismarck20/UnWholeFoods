import sqlite3 as sl
import pandas as pd
from sqlite3 import Error
import sqlite3
import json

connection = sqlite3.connect("C:\\Users\\kennc\\Downloads\\test.db")

cursor = connection.cursor()

cmd1 = """CREATE TABLE IF NOT EXISTS
stores(store_id INTEGER PRIMARY KEY, location TEXT, delivery_opt TEXT)"""

cursor.execute(cmd1)

cmd2 = """CREATE TABLE IF NOT EXISTS
inventory(inv_id INTEGER PRIMARY KEY, store_id INTEGER, 
FOREIGN KEY(store_id) REFERENCES stores(store_id))"""

cursor.execute(cmd2)

cmd3 = """CREATE TABLE IF NOT EXISTS
item(inv_id INTEGER, name TEXT, weight INTEGER, freshness INTEGER, price REAL, 
discount_rate REAL, num_in_stock INTEGER, 
FOREIGN KEY(inv_id) REFERENCES inventory(inv_id))"""
cursor.execute(cmd3)

def insert_store(conn, task):
    """
    Create a new task
    :param conn:
    :param task:
    :return:
    """

    sql = ''' INSERT INTO stores(store_id,location,delivery_opt)
              VALUES(?,?,?) '''
    cur = conn.cursor()
    cur.execute(sql, task)
    conn.commit()
    return cur.lastrowid

def insert_inventory(conn, task):
    """
    Create a new task
    :param conn:
    :param task:
    :return:
    """

    sql = ''' INSERT INTO inventory(inv_id, store_id)
              VALUES(?,?) '''
    cur = conn.cursor()
    cur.execute(sql, task)
    conn.commit()
    return cur.lastrowid
        
def insert_item(conn, task):
    """
    Create a new task
    :param conn:
    :param task:
    :return:
    """

    sql = ''' INSERT INTO item(inv_id,name,weight,freshness,price,discount_rate,num_in_stock)
              VALUES(?,?,?,?,?,?,?) '''
    cur = conn.cursor()
    cur.execute(sql, task)
    conn.commit()
    return cur.lastrowid

theDict={"GIANT Heirloom":1,
        "ACME":2}
productDict={"tomato":1,
            "banana":2}


def addValFromJsonToSQL(theJSON,conn):
    theCompanyID=theDict[theJSON['store_name']]
    theLocation=theJSON['location']
    theDelivery=theJSON['delivery_option']
    theRowForStore= (theCompanyID,theLocation,theDelivery)
    insert_store(conn, theRowForStore)
    print(theRowForStore)
    for item in theJSON['inventory_list']:
        theGood=productDict[item]
        theName=item
        theWeight=theJSON['inventory_list'][item]['weight']
        theFreshness=theJSON['inventory_list'][item]['freshness']
        thePrice=theJSON['inventory_list'][item]['Price']
        theDiscount=theJSON['inventory_list'][item]['Discount rate']
        theStock=theJSON['inventory_list'][item]['num_in_stock']
        theRow = (theCompanyID,theGood,theLocation,theFreshness,theDiscount,theStock,thePrice,theDelivery)
        theRowForInventory= (theGood,theCompanyID)
        theRowForItem = (theGood,theName,theWeight,theFreshness,thePrice,theDiscount,theStock)
        insert_inventory(conn,theRowForInventory)
        insert_item(conn,theRowForItem)
        
            
addValFromJsonToSQL(theJSON,connection)
connection.close()
