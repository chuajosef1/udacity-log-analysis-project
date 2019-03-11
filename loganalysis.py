#!/usr/bin/env python3
import psycopg2
import sys

# Variables for accessing newsdata.sql
DATABASE = "news"

QUERY_1 = ("select title, count(path) as num from log, articles\n"
           "where replace(path,'/article/','') = slug\n"
           " group by title order by num desc limit 3")

QUERY_2 = ("select authors.name , count(path) as num \n"
           "from authors,articles,log"
           " where articles.author = authors.id and \n"
           "replace(path,'/article/','') = slug \n"
           "group by authors.name order by num desc")

QUERY_3 = ("select count,visitors, to_char(errortime,'MON-DD-YYYY')\n"
           "from totalerror where ((count*100)/visitors)> 1.0")

# Function to access the database.


def acc_db():
    try:
        db = psycopg2.connect(database=DATABASE)
    except psycopg2.Error as e:
        print("Unable to connect to the database, exiting program.")
        print(e.pgerror)
        print(e.diag.message_detail)
        sys.exit(1)
    else:
        cursor = db.cursor()
        return cursor


# Fuction to access the .txt file.
def acc_txt():
    try:
        # Clearing contents of previous run program,
        # since we are appending to the file,
        # we don't want multiple outputs showing
        open("log_analysis.txt", "w").close()
        # Opening file to allow program to write to it.
        txt = open("log_analysis.txt", "a")
    except:
        print("Unable to open/create file, exiting program")
        sys.exit(1)
    else:
        return txt
# Function that finds the top 3 articles viewed.
# 1. What are the most popular three articles of all time?


def get_art(db, txt):
    db.execute(QUERY_1)
    post = db.fetchall()
    txt.write("1.What are the most popular three articles of all time?\n\n")
    for x in post:
        s = str(x[0])
        s2 = str(x[1])
        txt.write('\t\t' + '"' + s + '"' + ' - ' + s2 + ' views'+'\n')
    txt.write('\n')
    return post


# Functions that calculates who is the most popular author.
# 2. Who are the most popular article authors of all time?
def get_auth(db, txt):
    db.execute(QUERY_2)
    post = db.fetchall()
    txt.write("2.Who are the most popular article authors of all time?\n\n")
    for x in post:
        s = str(x[0])
        s2 = str(x[1])
        txt.write('\t\t'+s + ' - ' + s2 + ' views' + '\n')
    txt.write('\n')
    return post


# Function that finds which days had >1% errors.
# 3. On which days did more than 1% of requests lead to errors?
def get_log(db, txt):
    db.execute(QUERY_3)
    post = db.fetchall()
    txt.write('3.On which days did more than 1% of requests lead to errors?\n\n')
    for x in post:
        s = format((x[0]*100.0)/x[1], '.2f')
        s = str(s)
        txt.write('\t\t'+x[2] + ' - ' + s + ' % error')
    txt.write('\n')
    return post


def main():
    db = acc_db()
    txt = acc_txt()
    get_art(db, txt)
    get_auth(db, txt)
    get_log(db, txt)
    db.close()
    txt.close()


if __name__ == "__main__":
    main()
