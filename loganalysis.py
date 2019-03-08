import psycopg2

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


# Function that finds the top 3 articles viewed.
# 1. What are the most popular three articles of all time?
def get_art():
    db = psycopg2.connect(database=DATABASE)
    cursor = db.cursor()
    cursor.execute(QUERY_1)
    post = cursor.fetchall()
    a = open("log_analysis.txt", "w")
    a.write("1.What are the most popular three articles of all time?\n\n")
    for x in post:
        s = str(x[0])
        s2 = str(x[1])
        a.write('\t\t' + '"' + s + '"' + ' - ' + s2 + ' views'+'\n')
    a.write('\n')
    db.close()
    a.close()
    return post


# Functions that calculates who is the most popular author.
# 2. Who are the most popular article authors of all time?
def get_auth():
    db = psycopg2.connect(database=DATABASE)
    cursor = db.cursor()
    cursor.execute(QUERY_2)
    post = cursor.fetchall()
    a = open("log_analysis.txt", "a")
    a.write("2.Who are the most popular article authors of all time?\n\n")
    for x in post:
        s = str(x[0])
        s2 = str(x[1])
        a.write('\t\t'+s + ' - ' + s2 + ' views' + '\n')
    a.write('\n')
    db.close()
    a.close()
    return post


# Function that finds which days had >1% errors.
# 3. On which days did more than 1% of requests lead to errors?
def get_log():
    db = psycopg2.connect(database=DATABASE)
    cursor = db.cursor()
    cursor.execute(QUERY_3)
    post = cursor.fetchall()
    a = open("log_analysis.txt", "a")
    a.write('3.On which days did more than 1% of requests lead to errors?\n\n')
    for x in post:
        s = format((x[0]*100.0)/x[1], '.2f')
        s = str(s)
        a.write('\t\t'+x[2] + ' - ' + s + ' % error')
    a.write('\n')
    db.close()
    a.close()
    return post


def main():
    get_art()
    get_auth()
    get_log()


if __name__ == "__main__":
    main()
